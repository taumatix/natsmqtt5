package natsmqtt5

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/taumatix/natsmqtt5/packet"
	"github.com/taumatix/natsmqtt5/topic"
)

// retainStoreTimeout bounds a single write to the retained-message stream.
const retainStoreTimeout = 5 * time.Second

// retained is one stored retained message.
type retained struct {
	topicName string
	payload   []byte
	qos       packet.QoS
	props     *packet.Properties
	// stored is when the message was stored, which is when its Message Expiry
	// Interval starts to run; seq is its stream sequence, 0 if unknown.
	stored time.Time
	seq    uint64
	// id is the Mqtt5-Msg-Id of the PUBLISH it came from, which its offline-queue
	// copy carries too, so a session sent it as a retained message is not also
	// replayed that copy. Empty for one stored before ids were kept.
	id string
}

// expired reports whether the message's Message Expiry Interval has passed
// [MQTT-3.3.2-5].
func (r *retained) expired(now time.Time) bool {
	if r.props == nil || r.props.MessageExpiryInterval == nil {
		return false
	}
	_, expired := remainingExpiry(*r.props.MessageExpiryInterval, r.stored, now)
	return expired
}

// properties returns a copy, so a subscriber-specific Subscription Identifier
// cannot leak into the stored message and reach the next subscriber.
func (r *retained) properties() *packet.Properties {
	if r.props == nil {
		return &packet.Properties{}
	}
	cp := *r.props
	cp.SubscriptionIdentifiers = nil
	return &cp
}

// retainedStore keeps the retained message for each topic.
//
// The durable copy lives in a JetStream stream configured with
// MaxMsgsPerSubject 1, so publishing to a subject both replaces the previous
// retained message and compacts the old one away — which is exactly the
// semantics of [MQTT-3.3.1-5]. Every broker against the same NATS server
// consumes that stream, so retained messages are shared: a client that
// subscribes through one broker sees a message retained through another.
//
// A local map mirrors the stream so that matching a Topic Filter against the
// retained set is an in-memory operation, as it is in any other broker.
type retainedStore struct {
	js     jetstream.JetStream
	stream jetstream.Stream
	prefix string
	logger *slog.Logger

	mu sync.RWMutex
	// byTopic is keyed by MQTT Topic Name.
	byTopic map[string]*retained

	cancel context.CancelFunc
	// ready closes once the initial replay has caught up, so New does not
	// return a broker that would report an empty retained set.
	ready   chan struct{}
	closeMu sync.Mutex
	closed  bool
}

func newRetainedStore(ctx context.Context, js jetstream.JetStream, opts *resolved, logger *slog.Logger) (*retainedStore, error) {
	name := opts.StreamPrefix + "_retained"
	subject := retainedSubject(opts.SubjectPrefix, ">")

	storage := opts.RetainedStorage
	cfg := jetstream.StreamConfig{
		Name:        name,
		Description: "MQTT v5 retained messages held by natsmqtt5",
		Subjects:    []string{subject},
		Storage:     storage,
		Replicas:    opts.RetainedReplicas,
		// One message per subject is the whole retention model: a new retained
		// message replaces the old one [MQTT-3.3.1-5].
		MaxMsgsPerSubject: 1,
		Discard:           jetstream.DiscardOld,
		AllowDirect:       true,
	}
	stream, err := js.CreateOrUpdateStream(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("creating stream %s on subject %s: %w", name, subject, err)
	}

	s := &retainedStore{
		js:      js,
		stream:  stream,
		prefix:  opts.SubjectPrefix,
		logger:  logger,
		byTopic: make(map[string]*retained),
		ready:   make(chan struct{}),
	}

	runCtx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	if err := s.startConsumer(runCtx); err != nil {
		cancel()
		return nil, err
	}

	// Wait for the replay to catch up before the broker starts serving, so a
	// client that subscribes immediately gets the retained messages that were
	// already there.
	select {
	case <-s.ready:
	case <-ctx.Done():
		cancel()
		return nil, fmt.Errorf("waiting for the retained-message replay: %w", ctx.Err())
	}
	return s, nil
}

// retainedSubject namespaces the retained stream away from live traffic, so
// that the stream does not capture every published message.
func retainedSubject(prefix, subject string) string {
	return topic.Prefix(prefix, "$retained."+subject)
}

func (s *retainedStore) startConsumer(ctx context.Context) error {
	cons, err := s.stream.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		return fmt.Errorf("creating the retained-message consumer: %w", err)
	}
	iter, err := cons.Messages()
	if err != nil {
		return fmt.Errorf("consuming retained messages: %w", err)
	}

	// An empty stream never delivers a message, so the caught-up signal has to
	// come from the stream state rather than from the first delivery.
	info, err := s.stream.Info(ctx)
	if err != nil {
		iter.Stop()
		return fmt.Errorf("reading retained stream info: %w", err)
	}
	if info.State.Msgs == 0 {
		close(s.ready)
	}

	go func() {
		defer iter.Stop()
		caughtUp := info.State.Msgs == 0
		for {
			msg, err := iter.Next()
			if err != nil {
				if ctx.Err() == nil && !errors.Is(err, jetstream.ErrMsgIteratorClosed) {
					s.logger.Warn("the retained-message consumer stopped", "error", err)
				}
				if !caughtUp {
					close(s.ready)
				}
				return
			}
			s.apply(msg)
			if !caughtUp {
				if md, err := msg.Metadata(); err == nil && md.NumPending == 0 {
					caughtUp = true
					close(s.ready)
				}
			}
		}
	}()
	return nil
}

// apply folds one stream message into the in-memory view. A zero-length
// payload is the tombstone MQTT prescribes: it removes the retained message
// and is itself not stored [MQTT-3.3.1-6, MQTT-3.3.1-7].
func (s *retainedStore) apply(msg jetstream.Msg) {
	subject, ok := topic.TrimPrefix(s.prefix, msg.Subject())
	if !ok {
		return
	}
	subject, ok = trimRetainedMarker(subject)
	if !ok {
		return
	}
	name := topic.SubjectToName(subject)

	s.mu.Lock()
	defer s.mu.Unlock()
	if len(msg.Data()) == 0 {
		delete(s.byTopic, name)
		return
	}
	qos, _, _, props := fromNATS(&nats.Msg{Header: msg.Headers(), Data: msg.Data()})
	r := &retained{
		topicName: name,
		payload:   msg.Data(),
		qos:       qos,
		props:     props,
		stored:    time.Now(),
		id:        messageID(&nats.Msg{Header: msg.Headers()}),
	}
	// The stream's timestamp, so a message stored before this broker started
	// has been waiting since then and not since the broker read it.
	if md, err := msg.Metadata(); err == nil {
		r.stored, r.seq = md.Timestamp, md.Sequence.Stream
	}
	s.byTopic[name] = r
}

func trimRetainedMarker(subject string) (string, bool) {
	const marker = "$retained."
	if len(subject) <= len(marker) || subject[:len(marker)] != marker {
		return "", false
	}
	return subject[len(marker):], true
}

// store writes a retained message, or removes it when the payload is empty.
//
// The write is synchronous: PublishMsg waits for the JetStream acknowledgement
// so that a PUBACK to the client is not sent before the retained message is
// durable.
func (s *retainedStore) store(ctx context.Context, subject, originClientID, id string, p *packet.Publish) error {
	msg := toNATS(retainedSubject(s.prefix, subject), originClientID, p)
	if id != "" {
		msg.Header.Set(hdrMsgID, id)
	}
	if len(p.Payload) == 0 {
		// "If the Payload contains zero bytes ... any retained message with
		// the same topic name MUST be removed" [MQTT-3.3.1-6]. The empty
		// message becomes the tombstone; MaxMsgsPerSubject 1 drops the old
		// message it replaces.
		msg.Data = nil
	}
	ack, err := s.js.PublishMsg(ctx, msg)
	if err != nil {
		return err
	}

	// Update the local view immediately rather than waiting for the consumer
	// to loop it back, so a subscribe on this broker right after a retained
	// publish cannot miss it.
	name := topic.SubjectToName(subject)
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(p.Payload) == 0 {
		delete(s.byTopic, name)
		return nil
	}
	// The properties are read back from the message just written, not taken from
	// the PUBLISH: this copy must equal the one the stream consumer builds from
	// the same message, which carries only what toNATS keeps. A Topic Alias is a
	// mapping on the publisher's connection and means nothing on another, so it
	// is not part of a retained message [MQTT-3.1.2-26], [MQTT-3.1.2-27],
	// [MQTT-3.3.2-11].
	_, _, _, props := fromNATS(msg)
	s.byTopic[name] = &retained{
		topicName: name,
		payload:   p.Payload,
		qos:       p.QoS,
		props:     props,
		stored:    time.Now(),
		seq:       ack.Sequence,
		id:        messageID(msg),
	}
	return nil
}

// match returns the retained messages whose Topic Name matches the filter and
// have not expired. An expired one is deleted, here and from the stream, as
// the specification requires of a message that has passed its interval
// [MQTT-3.3.2-5]; doing it when a subscription looks is the sweep, and it
// costs nothing while no one asks.
func (s *retainedStore) match(filter string) []*retained {
	now := time.Now()
	var out, expired []*retained
	s.mu.RLock()
	for name, r := range s.byTopic {
		if !topic.Match(filter, name) {
			continue
		}
		if r.expired(now) {
			expired = append(expired, r)
			continue
		}
		out = append(out, r)
	}
	s.mu.RUnlock()
	if len(expired) > 0 {
		s.discard(expired)
	}
	return out
}

// sweep discards every retained message whose interval has passed, including
// those no subscription has asked for since [MQTT-3.3.2-5]. It runs on the
// broker's session sweep, so a message outlives its interval by up to
// Options.SessionSweepInterval.
func (s *retainedStore) sweep() {
	now := time.Now()
	var expired []*retained
	s.mu.RLock()
	for _, r := range s.byTopic {
		if r.expired(now) {
			expired = append(expired, r)
		}
	}
	s.mu.RUnlock()
	if len(expired) > 0 {
		s.discard(expired)
	}
}

// discard removes expired messages from the local view and from the stream.
// A message replaced since it was seen stays: only the one that expired goes.
func (s *retainedStore) discard(expired []*retained) {
	s.mu.Lock()
	for _, r := range expired {
		if s.byTopic[r.topicName] == r {
			delete(s.byTopic, r.topicName)
		}
	}
	s.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), retainStoreTimeout)
	defer cancel()
	for _, r := range expired {
		if r.seq == 0 {
			continue
		}
		// Another broker may have deleted it first, which is as good.
		if err := s.stream.DeleteMsg(ctx, r.seq); err != nil &&
			!errors.Is(err, jetstream.ErrMsgNotFound) {
			s.logger.Debug("deleting an expired retained message failed", "topic", r.topicName, "error", err)
		}
	}
}

func (s *retainedStore) close() {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	if s.cancel != nil {
		s.cancel()
	}
}
