package natsmqtt5

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/nats-io/nats.go"

	"github.com/taumatix/natsmqtt5/v2/topic"
)

// Queueing a plain NATS publish on a live subject (Options.QueuePlainPublishes).
//
// ingest.go explains why a copy stored after a plain publish cannot be told
// apart from it: the live copy has reached subscribers with no id. Since v2.0.0
// the brokers make themselves the first to act on such a message instead of
// asking the publisher to use another subject. A plain publish that asks for a
// queue (its Mqtt5-Qos header is 1 or 2) and has no Mqtt5-Msg-Id is ignored as a
// live copy by every MQTT subscription (heldForQueue), and one broker, chosen by
// a NATS queue group on the live subjects, handles it as an ingest message and
// publishes the live copy again with its id and queue sequence. The message a
// subscriber is delivered is that second copy, the same one an MQTT PUBLISH
// would have produced.

// plainQueueGroup is the NATS queue group the brokers share on the live
// subjects, so that each plain publish is taken once.
const plainQueueGroup = "natsmqtt5.plain"

// wantsQueueCopy reports whether msg is a plain publish that asks for a queue:
// no id (a broker has not seen it), a QoS of 1 or 2 in its headers, and a
// subject that is not one of the broker's own ($queue, $retained, $ingest, ...).
func (b *Broker) wantsQueueCopy(msg *nats.Msg) bool {
	if msg.Header == nil || msg.Header.Get(hdrMsgID) != "" {
		return false
	}
	if n, err := strconv.Atoi(msg.Header.Get(hdrQoS)); err != nil || n < 1 || n > 2 {
		return false
	}
	suffix, ok := topic.TrimPrefix(b.opts.SubjectPrefix, msg.Subject)
	return ok && !strings.HasPrefix(suffix, "$")
}

// heldForQueue reports whether a subscription must leave msg to the broker that
// publishes it again. Only a broker with an offline queue does: without one
// there is no copy to wait for, and the message is delivered as before.
func (b *Broker) heldForQueue(msg *nats.Msg) bool {
	return b.opts.QueuePlainPublishes && b.queue.Load() != nil && b.wantsQueueCopy(msg)
}

// listenForPlain subscribes to the live subjects in the plain queue group, with
// Options.QueuePlainPublishes.
func (b *Broker) listenForPlain() error {
	if !b.opts.QueuePlainPublishes {
		return nil
	}
	sub, err := b.nc.QueueSubscribe(topic.Prefix(b.opts.SubjectPrefix, ">"), plainQueueGroup, b.handlePlain)
	if err != nil {
		return fmt.Errorf("natsmqtt5: listening for plain publishes: %w", err)
	}
	b.plainSub = sub
	return nil
}

func (b *Broker) stopPlain() {
	if b.plainSub != nil {
		_ = b.plainSub.Unsubscribe()
	}
}

// handlePlain runs on the subscription's dispatcher goroutine, one message at a
// time, so that a publisher's messages that reach this broker go out in the
// order they came [MQTT-4.6.0-5].
func (b *Broker) handlePlain(m *nats.Msg) {
	if !b.wantsQueueCopy(m) {
		return
	}
	suffix, _ := topic.TrimPrefix(b.opts.SubjectPrefix, m.Subject)
	if _, err := b.publishThrough(m, suffix); err != nil {
		// The subscriptions left the message to this broker, so a refusal drops it
		// for them too; the publisher is not answered, as it did not ask for a reply.
		b.logger.Warn("dropped a plain NATS publish that asked for a queue copy", "subject", m.Subject, "error", err)
	}
}
