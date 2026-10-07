package natsmqtt5

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/taumatix/natsmqtt5/packet"
	"github.com/taumatix/natsmqtt5/topic"
)

// The offline queue (Options.OfflineQueue).
//
// Core NATS delivers to whoever is subscribed when a message is published and
// keeps nothing, so a session whose client is away misses everything published
// meanwhile. With the queue on, the broker publishes a copy of each QoS 1 and
// QoS 2 message under <prefix>.$queue.<subject>, into a stream of its own. A
// stream on <prefix>.> would overlap the retained stream's <prefix>.$retained.>,
// which JetStream refuses, and the "$" keeps the copies from matching any MQTT
// wildcard subscription [MQTT-4.7.2-1].
//
// When a session resumes, its delivery loop replays from the stream what the
// session's filters match, up to the stream's end, before it starts on live
// deliveries. It starts at the lowest sequence the previous connection had not
// delivered (conn.awayFloor), which can be minutes back for a client that was
// behind, or when there is none from a little before that connection ended.
// The replay has no deadline: it goes at the client's pace. Two sets of
// message ids keep that exactly once: what the previous connection delivered
// (so a rewind does not repeat it), and what the replay delivered (so a live
// copy that arrives late is skipped).

// offlineRewind is how far before the previous connection ended a replay
// starts when it has no better one. A message published while the connection was
// going down may have reached neither the client nor anything that resends it;
// starting earlier and skipping what was delivered catches it. A connection
// that ended with messages undelivered gives the lowest of their sequences
// instead (conn.awayFloor), however long ago they were published.
const offlineRewind = 2 * time.Second

type offlineQueue struct {
	stream jetstream.Stream
	prefix string
	// infoMu serialises stream.Info, which writes a cached copy into the
	// shared stream handle and so races between two connections replaying at
	// once (found by -race on CI, once the queue became the default).
	infoMu sync.Mutex
}

func newOfflineQueue(ctx context.Context, js jetstream.JetStream, opts *resolved) (*offlineQueue, error) {
	cfg := jetstream.StreamConfig{
		Name:        opts.StreamPrefix + "_queue",
		Description: "MQTT v5 QoS 1 and 2 messages kept for disconnected sessions by natsmqtt5",
		Subjects:    []string{queuedSubject(opts.SubjectPrefix, ">")},
		Storage:     opts.OfflineQueueStorage,
		Replicas:    opts.OfflineQueueReplicas,
		MaxAge:      opts.OfflineQueueMaxAge,
		Discard:     jetstream.DiscardOld,
	}
	stream, err := js.CreateOrUpdateStream(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &offlineQueue{stream: stream, prefix: opts.SubjectPrefix}, nil
}

func queuedSubject(prefix, subject string) string {
	return topic.Prefix(prefix, "$queue."+subject)
}

// keep stores the queue's copy of msg, a message about to be published on
// subject (without the prefix), and returns once JetStream has it.
//
// It runs before the live publish, and waits, and that order is what makes a
// resume lose nothing. A replay reads up to the stream's end as it is when the
// replay starts, which is after the session has its connection again. A copy
// stored after that point belongs to a live publish made after it too, which
// the reattached session receives live. Stored the other way round, a message
// published just before a resume had its live copy dropped while the session
// was away and its queued copy stored too late for the replay, and was lost
// (found on CI, on #36's merge commit).
func (q *offlineQueue) keep(ctx context.Context, js jetstream.JetStream, subject string, msg *nats.Msg) error {
	// The mark travels on the live copy too, which shares the header: that is
	// how a shared subscription knows its backlog has this message.
	msg.Header.Set(hdrQueued, "1")
	copied := &nats.Msg{Subject: queuedSubject(q.prefix, subject), Header: msg.Header, Data: msg.Data}
	ack, err := js.PublishMsg(ctx, copied)
	if err != nil {
		return err
	}
	// Set after the copy is stored, so only the live copy carries it.
	msg.Header.Set(hdrQueueSeq, strconv.FormatUint(ack.Sequence, 10))
	return nil
}

// queueSeq is the stream sequence of a live message's queued copy, or 0 when
// it has none.
func queueSeq(msg *nats.Msg) uint64 {
	if msg.Header == nil {
		return 0
	}
	seq, _ := strconv.ParseUint(msg.Header.Get(hdrQueueSeq), 10, 64)
	return seq
}

// offlineKeepTimeout bounds waiting for JetStream to store a queue copy.
const offlineKeepTimeout = 5 * time.Second

// replay calls fn with every queued message stored since `since`, in order,
// up to the end of the stream as it is when replay starts. The subject handed
// to fn is the live one, without the "$queue." part.
func (q *offlineQueue) replay(ctx context.Context, since time.Time, fn func(*nats.Msg)) error {
	_, err := q.replayFrom(ctx, jetstream.OrderedConsumerConfig{
		DeliverPolicy: jetstream.DeliverByStartTimePolicy,
		OptStartTime:  &since,
	}, fn)
	return err
}

// replaySeq is replay from a stream sequence rather than a time. It returns
// the last sequence it read up to, which is the stream's end when it started.
func (q *offlineQueue) replaySeq(ctx context.Context, from uint64, fn func(*nats.Msg)) (uint64, error) {
	return q.replayFrom(ctx, jetstream.OrderedConsumerConfig{
		DeliverPolicy: jetstream.DeliverByStartSequencePolicy,
		OptStartSeq:   from,
	}, fn)
}

func (q *offlineQueue) replayFrom(ctx context.Context, start jetstream.OrderedConsumerConfig, fn func(*nats.Msg)) (uint64, error) {
	q.infoMu.Lock()
	info, err := q.stream.Info(ctx)
	q.infoMu.Unlock()
	if err != nil {
		return 0, err
	}
	last := info.State.LastSeq
	if last == 0 {
		return 0, nil
	}
	cons, err := q.stream.OrderedConsumer(ctx, start)
	if err != nil {
		return last, err
	}
	queuedPrefix := queuedSubject(q.prefix, "")
	for {
		if err := ctx.Err(); err != nil {
			return last, err
		}
		batch, err := cons.Fetch(256, jetstream.FetchMaxWait(500*time.Millisecond))
		if err != nil {
			return last, err
		}
		n := 0
		for m := range batch.Messages() {
			n++
			meta, err := m.Metadata()
			if err != nil {
				continue
			}
			if meta.Sequence.Stream > last {
				return last, nil
			}
			subject := m.Subject()
			if len(subject) > len(queuedPrefix) && subject[:len(queuedPrefix)] == queuedPrefix {
				subject = topic.Prefix(q.prefix, subject[len(queuedPrefix):])
			}
			// A copy read back carries its own sequence, as its live copy does.
			header := m.Headers()
			if header == nil {
				header = nats.Header{}
			}
			header.Set(hdrQueueSeq, strconv.FormatUint(meta.Sequence.Stream, 10))
			fn(&nats.Msg{Subject: subject, Header: header, Data: m.Data()})
			if meta.Sequence.Stream >= last {
				return last, nil
			}
		}
		if err := batch.Error(); err != nil && !errors.Is(err, nats.ErrTimeout) && !errors.Is(err, jetstream.ErrNoMessages) {
			return last, err
		}
		if n == 0 {
			// Nothing at or after the start: the stream's end was before it.
			return last, nil
		}
	}
}

func newMessageID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func messageID(msg *nats.Msg) string {
	if msg.Header == nil {
		return ""
	}
	return msg.Header.Get(hdrMsgID)
}

// deliverQueued delivers a message read from the queue stream to each of the
// session's non-shared subscriptions it matches, and records it as replayed so
// its live copy, arriving late, is skipped. It runs on the delivery goroutine.
func (c *conn) deliverQueued(id string, msg *nats.Msg) error {
	qos, _, _, _ := fromNATS(msg)
	if qos == packet.QoS0 {
		return nil
	}
	subject, ok := topic.TrimPrefix(c.broker.opts.SubjectPrefix, msg.Subject)
	if !ok {
		return nil
	}
	name := topic.SubjectToName(subject)
	matched := false
	for _, sub := range c.sess.subscriptions() {
		if sub.share != "" || !topic.Match(sub.filter, name) {
			// A shared subscription's messages come from its own backlog
			// (shared.go), to whichever connected member pulls them.
			continue
		}
		d := c.deliveryFor(sub, name, msg)
		if d == nil || d.qos == packet.QoS0 {
			continue
		}
		matched = true
		if err := c.deliver(d); err != nil {
			return err
		}
	}
	if matched {
		c.replayed[id] = true
	}
	return nil
}

// replayOffline delivers what the queue holds for this session since its last
// connection ended. It runs on the delivery goroutine, after retransmit and
// before the live loop, which is what keeps queued messages ahead of live ones
// [MQTT-4.6.0-5].
func (c *conn) replayOffline() error {
	q := c.broker.queue
	if q == nil {
		return nil
	}
	c.sess.awaitPredecessor(c)
	a, ok := c.sess.takeAway()
	if !ok {
		return nil
	}
	c.resume = &a
	since := a.at.Add(-offlineRewind)
	if a.restored {
		// Restored from the session store: the ids delivered before the
		// release are not stored, so a rewind could deliver a message twice,
		// which QoS 2 forbids. A message lost in the moments the connection
		// was going down is the cost, as for the in-flight messages a restored
		// session does not carry either.
		since = a.at
	}
	delivered := c.sess.deliveredIDs()

	// No deadline: the replay goes at the client's pace, as a catch-up does,
	// and ends with the connection.
	ctx, cancel := c.streamContext()
	defer cancel()
	var deliverErr error
	handle := func(msg *nats.Msg) {
		if deliverErr != nil || c.isClosed() {
			return
		}
		id := messageID(msg)
		if id != "" && !delivered[id] && !c.wasReplayed(id) {
			deliverErr = c.deliverQueued(id, msg)
		}
		if deliverErr == nil && !c.isClosed() {
			c.streamNext = queueSeq(msg) + 1
		}
	}
	var err error
	if a.fromSeq != 0 && !a.restored {
		c.streamNext = a.fromSeq
		_, err = q.replaySeq(ctx, a.fromSeq, handle)
	} else {
		err = q.replay(ctx, since, handle)
	}
	if deliverErr != nil {
		return deliverErr
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		c.logger.Warn("replaying the offline queue failed; what it held is lost for this session",
			"client_id", c.sess.clientID, "error", err)
	}
	if err == nil || !errors.Is(err, context.Canceled) {
		c.resume, c.streamNext = nil, 0
	}
	if n := len(c.replayed); n > 0 {
		c.logger.Info("delivered messages queued while the client was away",
			"client_id", c.sess.clientID, "count", n)
	}
	return nil
}
