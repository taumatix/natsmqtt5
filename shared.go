package natsmqtt5

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/taumatix/natsmqtt5/packet"
	"github.com/taumatix/natsmqtt5/topic"
)

// The shared-subscription backlog.
//
// A shared subscription hands each message to one of its member sessions,
// chosen "on a message by message basis" by whatever criteria the server likes
// (MQTT-5.0 §4.8.2), and once chosen the message is part of that session's
// state [MQTT-4.5.0-1]. A NATS queue group alone chooses among members whether
// or not their client is connected, and a member with no connection had
// nowhere to put its share: a probe of v0.9.0 lost 27 of 40 messages that way.
//
// With the offline queue on, every QoS 1 and 2 message already has a copy in
// the queue stream, so each shared subscription gets a durable pull consumer
// on that stream, filtered to its Topic Filter. Only a member with a
// connection pulls from it, one message at a time and only when its client has
// room under Receive Maximum, so the work goes to members that can take it, on
// any broker. With no member connected the messages wait in the consumer for
// the first one back, which is §4.8.2's "undelivered messages associated with"
// the subscription. The live copy of a queued message is skipped by every
// member, so each message reaches the group once.
//
// QoS 0 messages, and messages published straight onto NATS, have no queue copy
// and still go through the NATS queue group.

// hdrQueued marks a message whose copy the offline queue holds. A shared
// subscription with a backlog skips the live copy of such a message, because
// the backlog delivers it.
const hdrQueued = "Mqtt5-Queued"

// sharedPullWait bounds one pull request. An idle member re-issues one this
// often, which is also how quickly it notices that its connection has gone or
// its subscription was removed.
const sharedPullWait = 2 * time.Second

// sharedAckWait is how long a member may hold a message it pulled before
// JetStream hands it to another member. The hand-off to the session is
// immediate, since the member pulls only when it has send quota, so this only
// matters when a broker dies holding a message.
const sharedAckWait = 30 * time.Second

// sharedConsumerName names the durable consumer behind a shared subscription.
// Brokers sharing a StreamPrefix compute the same name, which is what makes the
// subscription one group across them. The share name and filter can hold
// characters a consumer name cannot, so they are hashed.
func sharedConsumerName(streamPrefix, share, subject string) string {
	sum := sha256.Sum256([]byte(share + "\x00" + subject))
	return streamPrefix + "_share_" + hex.EncodeToString(sum[:16])
}

// sharedConsumer creates, or finds, the durable consumer for a shared
// subscription. Creating it is idempotent across brokers.
func (q *offlineQueue) sharedConsumer(ctx context.Context, opts *resolved, sub *subscription) (jetstream.Consumer, error) {
	subject, ok := topic.TrimPrefix(q.prefix, sub.subject)
	if !ok {
		return nil, errors.New("shared subscription subject outside the subject prefix")
	}
	filters := []string{queuedSubject(q.prefix, subject)}
	if parent, ok := topic.ParentSubject(subject); ok {
		filters = append(filters, queuedSubject(q.prefix, parent))
	}
	return q.stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:        sharedConsumerName(opts.StreamPrefix, sub.share, subject),
		Description:    "natsmqtt5 shared subscription $share/" + sub.share + "/" + subject,
		FilterSubjects: filters,
		DeliverPolicy:  jetstream.DeliverNewPolicy,
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        sharedAckWait,
		// A shared subscription ends when no session is subscribed to it
		// (MQTT-5.0 §4.8.2). No member pulling for longer than any session can
		// be away means every member's session has ended.
		InactiveThreshold: opts.maxSessionExpiry + time.Minute,
	})
}

// liveSubject turns the subject of a queued copy back into the subject the
// message was published on.
func (q *offlineQueue) liveSubject(subject string) string {
	queuedPrefix := queuedSubject(q.prefix, "")
	if len(subject) > len(queuedPrefix) && subject[:len(queuedPrefix)] == queuedPrefix {
		return topic.Prefix(q.prefix, subject[len(queuedPrefix):])
	}
	return subject
}

// sharedItem is a message a member pulled from a backlog, on its way to the
// delivery goroutine. done receives whether the session took it.
type sharedItem struct {
	sub  *subscription
	msg  *nats.Msg
	done chan bool
}

// startPulling makes this connection a puller for every shared subscription
// with a backlog that its session holds. It is idempotent per subscription,
// so the delivery goroutine and a SUBSCRIBE may both call it.
func (c *conn) startPulling() {
	for _, sub := range c.sess.subscriptions() {
		c.startPull(sub)
	}
}

func (c *conn) startPull(sub *subscription) {
	if sub.backlog == nil {
		return
	}
	c.pullMu.Lock()
	defer c.pullMu.Unlock()
	if c.pulling[sub] {
		return
	}
	c.pulling[sub] = true
	go c.pull(sub)
}

// pull takes the subscription's messages from its backlog for as long as this
// connection is the session's and the subscription is in it.
func (c *conn) pull(sub *subscription) {
	defer func() {
		c.pullMu.Lock()
		delete(c.pulling, sub)
		c.pullMu.Unlock()
	}()
	for sub.live.Load() {
		// Room under the client's Receive Maximum first (MQTT-5.0 §4.9), so a
		// member never holds a message it cannot send while another could.
		select {
		case c.quota <- struct{}{}:
		case <-c.done:
			return
		}
		m, err := c.fetchOne(sub)
		if m == nil {
			c.releaseQuota()
			if err != nil {
				select {
				case <-time.After(sharedPullWait):
				case <-c.done:
					return
				}
			}
			continue
		}

		header := m.Headers()
		if meta, err := m.Metadata(); err == nil {
			header = setArrived(header, meta.Timestamp)
		}
		it := &sharedItem{
			sub:  sub,
			msg:  &nats.Msg{Subject: c.broker.queue.liveSubject(m.Subject()), Header: header, Data: m.Data()},
			done: make(chan bool, 1),
		}
		var taken bool
		select {
		case c.shared <- it:
			// The delivery goroutine answers every item it takes, even on a
			// connection that is closing: once a PUBLISH is in flight the
			// session owns the message, and handing it to another member too
			// would deliver it twice.
			taken = <-it.done
		case <-c.done:
			c.releaseQuota()
		}
		if taken {
			_ = m.Ack()
		} else {
			// Not this member's after all: another can have it now.
			_ = m.Nak()
		}
	}
}

// fetchOne pulls at most one message, waiting up to sharedPullWait. It
// returns a nil message and nil error when there was none to take.
func (c *conn) fetchOne(sub *subscription) (jetstream.Msg, error) {
	batch, err := sub.backlog.Fetch(1, jetstream.FetchMaxWait(sharedPullWait))
	if err != nil {
		c.logger.Debug("pulling from a shared subscription's backlog failed", "filter", sub.filter, "error", err)
		return nil, err
	}
	var got jetstream.Msg
	for m := range batch.Messages() {
		got = m
	}
	if err := batch.Error(); got == nil && err != nil &&
		!errors.Is(err, nats.ErrTimeout) && !errors.Is(err, jetstream.ErrNoMessages) {
		c.logger.Debug("pulling from a shared subscription's backlog failed", "filter", sub.filter, "error", err)
		return nil, err
	}
	return got, nil
}

// deliverShared runs on the delivery goroutine. The puller holds a quota slot
// for it, which this either spends on the PUBLISH or gives back. It reports
// whether the session took the message.
func (c *conn) deliverShared(it *sharedItem) (bool, error) {
	if !it.sub.live.Load() {
		c.releaseQuota()
		return false, nil
	}
	subject, ok := topic.TrimPrefix(c.broker.opts.SubjectPrefix, it.msg.Subject)
	name := topic.SubjectToName(subject)
	if !ok || !topic.Match(it.sub.filter, name) {
		// The consumer's filter is coarser than MQTT's, as a NATS subscription
		// is (MQTT-5.0 §4.7.2): no member of this group wants it.
		c.releaseQuota()
		return true, nil
	}
	d := c.deliveryFor(it.sub, name, it.msg)
	if d == nil {
		c.releaseQuota()
		return false, nil
	}
	d.quotaHeld = true
	if d.qos == packet.QoS0 {
		// Granted QoS 0: nothing will be in flight to hold the slot.
		c.releaseQuota()
		d.quotaHeld = false
	}
	return true, c.deliver(d)
}
