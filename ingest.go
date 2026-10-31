package natsmqtt5

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/taumatix/natsmqtt5/packet"
	"github.com/taumatix/natsmqtt5/topic"
)

// Publishing through a broker from plain NATS (Options.NATSIngest).
//
// A broker can only keep what passes through it: the queue copy is stored
// before the live copy goes out (offlineQueue.keep), and the live copy carries
// the id and the queue sequence every later step relies on (the replay, the
// catch-up, the shared backlog). A message a NATS client publishes on a live
// subject reaches subscribers before any broker sees it, so a copy stored
// afterwards would be a second delivery to every session that had the first,
// with nothing to tell them apart. The ingest subject puts the broker first:
// nothing subscribes to it but the brokers, in one queue group, and the one
// that takes a message handles it as a PUBLISH from an MQTT client.

// ingestQueueGroup is the NATS queue group the brokers share on the ingest
// subject, so that each message is published once.
const ingestQueueGroup = "natsmqtt5.ingest"

// hdrIngestError is set on the reply to an ingest request the broker refused.
const hdrIngestError = "Mqtt5-Ingest-Error"

func ingestSubject(prefix, subject string) string {
	return topic.Prefix(prefix, "$ingest."+subject)
}

// listenForIngest subscribes to the ingest subject, with Options.NATSIngest.
func (b *Broker) listenForIngest() error {
	if !b.opts.NATSIngest {
		return nil
	}
	sub, err := b.nc.QueueSubscribe(ingestSubject(b.opts.SubjectPrefix, ">"), ingestQueueGroup, b.handleIngest)
	if err != nil {
		return fmt.Errorf("natsmqtt5: listening on the ingest subject: %w", err)
	}
	b.ingestSub = sub
	return nil
}

func (b *Broker) stopIngest() {
	if b.ingestSub != nil {
		_ = b.ingestSub.Unsubscribe()
	}
}

// handleIngest runs on the subscription's dispatcher goroutine, one message
// at a time, so that a publisher's messages that reach this broker go out in
// the order they came [MQTT-4.6.0-5].
func (b *Broker) handleIngest(m *nats.Msg) {
	live, err := b.ingest(m)
	if m.Reply == "" {
		if err != nil {
			b.logger.Debug("refused a message on the ingest subject", "subject", m.Subject, "error", err)
		}
		return
	}
	reply := nats.NewMsg(m.Reply)
	if err != nil {
		reply.Header.Set(hdrIngestError, err.Error())
		reply.Data = []byte(err.Error())
	} else {
		reply.Header.Set(hdrMsgID, messageID(live))
		if seq := queueSeq(live); seq != 0 {
			reply.Header.Set(hdrQueueSeq, strconv.FormatUint(seq, 10))
		}
	}
	if err := m.RespondMsg(reply); err != nil {
		b.logger.Debug("answering an ingest request failed", "subject", m.Subject, "error", err)
	}
}

// ingest handles one message from the ingest subject as handlePublish handles
// a PUBLISH, and returns the live copy it published.
func (b *Broker) ingest(m *nats.Msg) (*nats.Msg, error) {
	marker := ingestSubject(b.opts.SubjectPrefix, "")
	if !strings.HasPrefix(m.Subject, marker) || len(m.Subject) == len(marker) {
		return nil, errors.New("no topic after the ingest prefix")
	}
	name := b.codec.subjectToName(m.Subject[len(marker):])
	if err := b.codec.validateName(name); err != nil {
		return nil, err
	}
	if dollarName(name) {
		// Refused whatever RestrictDollarTopics says: $queue and $retained are the
		// broker's own subjects, and NATS permissions on the ingest subject must not
		// become a way to write them.
		return nil, errors.New("topics starting with $ are refused on the ingest subject")
	}
	qos, retain, origin, props := fromNATS(m)
	if qos > b.opts.maxQoS {
		return nil, fmt.Errorf("QoS %d exceeds the broker's maximum of %d", qos, b.opts.maxQoS)
	}
	if retain && !b.retainAvailable() {
		return nil, errors.New("this broker has retained messages disabled")
	}
	if props.ResponseTopic != "" && hasWildcard(props.ResponseTopic) {
		return nil, fmt.Errorf("the Response Topic %q contains a wildcard", props.ResponseTopic)
	}
	subject, err := b.codec.nameToSubject(name)
	if err != nil {
		return nil, err
	}
	p := &packet.Publish{Topic: name, QoS: qos, Retain: retain, Payload: m.Data, Properties: props}

	ctx, cancel := context.WithTimeout(context.Background(), offlineKeepTimeout)
	defer cancel()
	id := newMessageID()
	if retain {
		if err := b.retain.store(ctx, subject, origin, id, p); err != nil {
			b.logger.Warn("storing a retained message from the ingest subject failed", "topic", name, "error", err)
			return nil, errors.New("could not store the retained message")
		}
	}
	full := topic.Prefix(b.opts.SubjectPrefix, subject)
	msg := toNATS(full, origin, p)
	msg.Header.Set(hdrMsgID, id)
	if b.opts.PublisherClockExpiry && props.MessageExpiryInterval != nil {
		// As handlePublish does: the expiry counts from this broker's receipt.
		msg.Header.Set(hdrPublishedAt, strconv.FormatInt(time.Now().UnixNano(), 10))
	}
	// Headers that are not the broker's travel on, for plain NATS subscribers. Nats-*
	// ones do not: on the queue copy JetStream would act on them (a Nats-Msg-Id
	// would make a later message a duplicate of this one).
	for k, v := range m.Header {
		lower := strings.ToLower(k)
		if !strings.HasPrefix(lower, "mqtt5-") && !strings.HasPrefix(lower, "nats-") {
			msg.Header[k] = v
		}
	}
	if q := b.queue.Load(); q != nil && qos > packet.QoS0 {
		if err := q.keep(ctx, b.js, subject, msg); err != nil {
			b.logger.Warn("queueing a message from the ingest subject failed", "subject", full, "error", err)
			return nil, errors.New("the message could not be queued")
		}
	}
	if err := b.nc.PublishMsg(msg); err != nil {
		return nil, fmt.Errorf("publishing the live copy failed: %w", err)
	}
	if b.opts.DurablePublish && qos > packet.QoS0 {
		if err := b.nc.FlushWithContext(ctx); err != nil {
			return nil, errors.New("the NATS server did not confirm the message")
		}
	}
	return msg, nil
}
