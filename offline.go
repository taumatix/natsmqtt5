package natsmqtt5

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
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
// session's filters match, from a little before the previous connection ended
// up to the stream's end, before it starts on live deliveries. Two sets of
// message ids keep that exactly once: what the previous connection delivered
// (so the rewind does not repeat it), and what the replay delivered (so a live
// copy that arrives late is skipped).

// offlineRewind is how far before the previous connection ended a replay
// starts. A message published while the connection was going down may have
// reached neither the client nor anything that resends it; starting earlier
// and skipping what was delivered catches it.
const offlineRewind = 2 * time.Second

// offlineReplayTimeout bounds one replay, so a slow stream cannot hold the
// connection's delivery loop for ever.
const offlineReplayTimeout = 30 * time.Second

type offlineQueue struct {
	stream jetstream.Stream
	prefix string
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
	_, err := js.PublishMsg(ctx, copied)
	return err
}

// offlineKeepTimeout bounds waiting for JetStream to store a queue copy.
const offlineKeepTimeout = 5 * time.Second

// replay calls fn with every queued message stored since `since`, in order,
// up to the end of the stream as it is when replay starts. The subject handed
// to fn is the live one, without the "$queue." part.
func (q *offlineQueue) replay(ctx context.Context, since time.Time, fn func(*nats.Msg)) error {
	info, err := q.stream.Info(ctx)
	if err != nil {
		return err
	}
	last := info.State.LastSeq
	if last == 0 {
		return nil
	}
	cons, err := q.stream.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{
		DeliverPolicy: jetstream.DeliverByStartTimePolicy,
		OptStartTime:  &since,
	})
	if err != nil {
		return err
	}
	queuedPrefix := queuedSubject(q.prefix, "")
	for {
		batch, err := cons.Fetch(256, jetstream.FetchMaxWait(500*time.Millisecond))
		if err != nil {
			return err
		}
		n := 0
		for m := range batch.Messages() {
			n++
			meta, err := m.Metadata()
			if err != nil {
				continue
			}
			if meta.Sequence.Stream > last {
				return nil
			}
			subject := m.Subject()
			if len(subject) > len(queuedPrefix) && subject[:len(queuedPrefix)] == queuedPrefix {
				subject = topic.Prefix(q.prefix, subject[len(queuedPrefix):])
			}
			fn(&nats.Msg{Subject: subject, Header: m.Headers(), Data: m.Data()})
			if meta.Sequence.Stream >= last {
				return nil
			}
		}
		if err := batch.Error(); err != nil && !errors.Is(err, nats.ErrTimeout) && !errors.Is(err, jetstream.ErrNoMessages) {
			return err
		}
		if n == 0 {
			// Nothing at or after `since`: the stream's end was before it.
			return nil
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

// replayOffline delivers what the queue holds for this session since its last
// connection ended. It runs on the delivery goroutine, after retransmit and
// before the live loop, which is what keeps queued messages ahead of live ones
// [MQTT-4.6.0-5].
func (c *conn) replayOffline() error {
	q := c.broker.queue
	if q == nil {
		return nil
	}
	awayAt, restored, ok := c.sess.takeAway()
	if !ok {
		return nil
	}
	since := awayAt.Add(-offlineRewind)
	if restored {
		// Restored from the session store: the ids delivered before the
		// release are not stored, so a rewind could deliver a message twice,
		// which QoS 2 forbids. A message lost in the moments the connection
		// was going down is the cost, as for the in-flight messages a restored
		// session does not carry either.
		since = awayAt
	}
	delivered := c.sess.deliveredIDs()

	ctx, cancel := context.WithTimeout(context.Background(), offlineReplayTimeout)
	defer cancel()
	var deliverErr error
	err := q.replay(ctx, since, func(msg *nats.Msg) {
		if deliverErr != nil {
			return
		}
		id := messageID(msg)
		if id == "" || delivered[id] || c.replayed[id] {
			return
		}
		qos, _, _, _ := fromNATS(msg)
		if qos == packet.QoS0 {
			return
		}
		subject, ok := topic.TrimPrefix(c.broker.opts.SubjectPrefix, msg.Subject)
		if !ok {
			return
		}
		name := topic.SubjectToName(subject)
		matched := false
		for _, sub := range c.sess.subscriptions() {
			if sub.share != "" || !topic.Match(sub.filter, name) {
				// A shared subscription's messages go to the group's members
				// that are connected; MQTT-5.0 §4.8.2 gives a disconnected
				// member no claim on them.
				continue
			}
			d := c.deliveryFor(sub, name, msg)
			if d == nil || d.qos == packet.QoS0 {
				continue
			}
			matched = true
			if err := c.deliver(d); err != nil {
				deliverErr = err
				return
			}
		}
		if matched {
			c.replayed[id] = true
		}
	})
	if deliverErr != nil {
		return deliverErr
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		c.logger.Warn("replaying the offline queue failed; what it held is lost for this session",
			"client_id", c.sess.clientID, "error", err)
	}
	if n := len(c.replayed); n > 0 {
		c.logger.Info("delivered messages queued while the client was away",
			"client_id", c.sess.clientID, "count", n)
	}
	return nil
}
