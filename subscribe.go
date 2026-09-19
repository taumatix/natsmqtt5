package natsmqtt5

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/taumatix/natsmqtt5/packet"
	"github.com/taumatix/natsmqtt5/topic"
)

// handleSubscribe creates or replaces the session's subscriptions and answers
// with one Reason Code per filter [MQTT-3.8.4-6].
func (c *conn) handleSubscribe(ctx context.Context, p *packet.Subscribe) error {
	subID := 0
	if p.Properties != nil && len(p.Properties.SubscriptionIdentifiers) > 0 {
		// SUBSCRIBE carries at most one Subscription Identifier; the decoder
		// has already rejected a repeat (MQTT-5.0 §3.8.2.1.2).
		subID = p.Properties.SubscriptionIdentifiers[0]
	}

	codes := make([]packet.ReasonCode, len(p.Subscriptions))
	granted := make([]*subscription, len(p.Subscriptions))
	replaced := make([]bool, len(p.Subscriptions))
	for i, want := range p.Subscriptions {
		sub, existed, code := c.subscribeOne(ctx, want, subID)
		codes[i] = code
		granted[i] = sub
		replaced[i] = existed
	}

	// A SUBACK tells the client its subscription is live, so the interest has
	// to have reached the NATS server before the SUBACK reaches the client.
	// nats.go buffers the SUB protocol and writes it from another goroutine, so
	// without this round trip a publisher on a second broker can beat the SUB
	// to the server and the message is lost with nothing to show for it.
	if anyGranted(granted) {
		if err := c.flushNATS(ctx); err != nil {
			c.logger.Warn("could not confirm the subscriptions with NATS", "error", err)
			for i, sub := range granted {
				if sub == nil {
					continue
				}
				c.sess.removeSubscription(sub.filter)
				unsubscribeAll(sub)
				granted[i] = nil
				codes[i] = packet.ImplementationSpecificError
			}
		}
	}

	// The subscription set has changed, so the durable copy has to change with
	// it or a restart would resume a set the client never asked for.
	if anyGranted(granted) {
		c.broker.persistSession(c)
	}

	if err := c.write(&packet.Suback{PacketID: p.PacketID, ReasonCodes: codes}); err != nil {
		return err
	}

	// Retained messages go out after the SUBACK so the client has been told
	// what QoS it was granted before anything arrives at that QoS.
	for i, sub := range granted {
		if sub == nil {
			continue
		}
		c.sendRetained(sub, p.Subscriptions[i].RetainHandling, replaced[i])
	}
	return nil
}

// subscribeOne installs a single subscription. It returns the subscription,
// whether it replaced one the session already held — which Retain Handling 1
// needs [MQTT-3.3.1-10] — and the Reason Code: the granted QoS on success, or
// a failure code from MQTT-5.0 Table 3-8.
func (c *conn) subscribeOne(ctx context.Context, want packet.Subscription, subID int) (*subscription, bool, packet.ReasonCode) {
	if err := topic.ValidateFilter(want.Filter); err != nil {
		c.logger.Debug("rejecting a subscription", "filter", want.Filter, "error", err)
		return nil, false, packet.TopicFilterInvalid
	}
	share, _, err := topic.SplitShared(want.Filter)
	if err != nil {
		return nil, false, packet.TopicFilterInvalid
	}
	// "It is a Protocol Error to set the No Local bit to 1 on a Shared
	// Subscription" [MQTT-3.8.3-4].
	if share != "" && want.NoLocal {
		return nil, false, packet.ProtocolError
	}

	if a := c.broker.opts.Authorizer; a != nil {
		req := &AuthzRequest{
			Action: ActionSubscribe, ClientID: c.sess.clientID, Identity: c.sess.identity,
			Username: c.sess.username, Topic: want.Filter, QoS: want.QoS,
		}
		if err := a.Authorize(ctx, req); err != nil {
			c.logger.Debug("subscription denied", "filter", want.Filter, "error", err)
			return nil, false, packet.NotAuthorized
		}
	}

	subject, err := topic.FilterToSubject(want.Filter)
	if err != nil {
		return nil, false, packet.TopicFilterInvalid
	}
	full := topic.Prefix(c.broker.opts.SubjectPrefix, subject)

	granted := want.QoS
	if granted > c.broker.opts.maxQoS {
		// The server may grant a lower QoS than requested [MQTT-3.8.4-7].
		granted = c.broker.opts.maxQoS
	}
	// A QoS 2 shared subscription needs per-member delivery affinity, which
	// JetStream cannot express: its unit of ownership is a consumer, not a
	// queue-group member. Downgrading to QoS 1 is explicitly the server's
	// choice to make [MQTT-3.8.4-7], and is recorded in the SUBACK.
	if share != "" && granted == packet.QoS2 {
		granted = packet.QoS1
	}

	// "If a Server receives a SUBSCRIBE packet containing a Topic Filter that
	// is identical to a Non-shared Subscription's Topic Filter for the current
	// Session, then it MUST replace that existing Subscription"
	// [MQTT-3.8.4-3]. Replacing means tearing the NATS subscriptions down and
	// building them again, since the options may have changed.
	old, existed := c.sess.removeSubscription(want.Filter)
	if existed {
		unsubscribeAll(old)
	}

	sub := &subscription{
		filter:     want.Filter,
		subject:    full,
		share:      share,
		opts:       want,
		grantedQoS: granted,
		id:         subID,
	}
	if err := c.bindNATS(sub); err != nil {
		c.logger.Warn("could not create the NATS subscription",
			"filter", want.Filter, "subject", full, "error", err)
		return nil, existed, packet.ImplementationSpecificError
	}
	c.sess.putSubscription(sub)

	return sub, existed, packet.ReasonCode(granted)
}

func anyGranted(granted []*subscription) bool {
	for _, sub := range granted {
		if sub != nil {
			return true
		}
	}
	return false
}

// natsFlushTimeout bounds the round trip that confirms a subscription reached
// the NATS server. It is generous: exceeding it means the NATS connection is
// in trouble, not that the server is briefly slow.
const natsFlushTimeout = 5 * time.Second

// flushNATS waits for everything the broker has handed to nats.go to reach the
// server.
func (c *conn) flushNATS(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, natsFlushTimeout)
	defer cancel()
	return c.broker.nc.FlushWithContext(ctx)
}

// bindNATS creates the NATS subscriptions behind an MQTT filter. A filter
// ending in '#' needs a second one on the parent subject, because MQTT's '#'
// matches the parent level and NATS's '>' does not (MQTT-5.0 §4.7.1.2).
func (c *conn) bindNATS(sub *subscription) error {
	// The handler resolves the connection at delivery time rather than
	// capturing the one that created the subscription. A session that is
	// resumed on a new connection keeps its subscriptions (MQTT-5.0 §4.1), and
	// capturing would leave them delivering into the closed socket.
	sess := c.sess
	handler := func(msg *nats.Msg) {
		if cur := sess.currentConn(); cur != nil {
			cur.onNATSMessage(sub, msg)
		}
	}

	subjects := []string{sub.subject}
	if parent, ok := topic.ParentSubject(sub.subject); ok {
		subjects = append(subjects, parent)
	}

	for _, subject := range subjects {
		var (
			ns  *nats.Subscription
			err error
		)
		if sub.share != "" {
			// The ShareName becomes the NATS queue group, which is exactly the
			// work-queue semantics a shared subscription asks for
			// (MQTT-5.0 §4.8.2). Namespacing it by filter keeps two shared
			// subscriptions with the same ShareName but different filters
			// independent, as the spec requires.
			ns, err = c.broker.nc.QueueSubscribe(subject, queueGroup(sub), handler)
		} else {
			ns, err = c.broker.nc.Subscribe(subject, handler)
		}
		if err != nil {
			unsubscribeAll(sub)
			return err
		}
		sub.natsSubs = append(sub.natsSubs, ns)
	}
	return nil
}

// queueGroup names the NATS queue group for a shared subscription. Two shared
// subscriptions with the same ShareName but different filters "are distinct
// shared subscriptions" (MQTT-5.0 §4.8.2), so the filter is part of the name.
func queueGroup(sub *subscription) string {
	return "mqtt5." + sub.share + "." + sub.subject
}

func (c *conn) handleUnsubscribe(p *packet.Unsubscribe) error {
	codes := make([]packet.ReasonCode, len(p.Filters))
	removed := false
	for i, filter := range p.Filters {
		sub, ok := c.sess.removeSubscription(filter)
		if !ok {
			// [MQTT-3.11.3-1] with 0x11.
			codes[i] = packet.NoSubscriptionExisted
			continue
		}
		unsubscribeAll(sub)
		codes[i] = packet.Success
		removed = true
	}
	if removed {
		c.broker.persistSession(c)
	}
	return c.write(&packet.Unsuback{PacketID: p.PacketID, ReasonCodes: codes})
}

// onNATSMessage runs on a nats.go dispatcher goroutine. It must not block, so
// it does the cheap filtering here and hands the result to the delivery loop.
func (c *conn) onNATSMessage(sub *subscription, msg *nats.Msg) {
	subject, ok := topic.TrimPrefix(c.broker.opts.SubjectPrefix, msg.Subject)
	if !ok {
		return
	}
	name := topic.SubjectToName(subject)

	// NATS wildcards are coarser than MQTT's in one direction: the extra
	// parent subscription for '#' and the '$' rule both need a check against
	// the MQTT filter itself (MQTT-5.0 §4.7.2).
	if !topic.Match(sub.filter, name) {
		return
	}

	qos, retain, origin, props := fromNATS(msg)

	// "If the value is 1, Application Messages MUST NOT be forwarded to a
	// connection with a ClientID equal to the ClientID of the publishing
	// connection" [MQTT-3.8.3-3].
	if sub.opts.NoLocal && origin == c.sess.clientID {
		return
	}
	// Retain As Published: keep the flag only if the subscription asked for it
	// [MQTT-3.3.1-12, MQTT-3.3.1-13].
	if !sub.opts.RetainAsPublished {
		retain = false
	}
	if sub.id != 0 {
		props.SubscriptionIdentifiers = []int{sub.id}
	}

	c.enqueue(&delivery{
		topic: name,
		// "The QoS of Application Messages sent in response to a Subscription
		// MUST be the minimum of the QoS of the originally published message
		// and the Maximum QoS granted" [MQTT-3.8.4-8].
		qos:     minQoS(qos, sub.grantedQoS),
		retain:  retain,
		payload: msg.Data,
		props:   props,
	})
}

func minQoS(a, b packet.QoS) packet.QoS {
	if a < b {
		return a
	}
	return b
}

// enqueue hands a message to the delivery loop, dropping it if the client is
// too far behind. Blocking here would stall the NATS dispatcher for every
// other subscriber on this connection.
func (c *conn) enqueue(d *delivery) {
	select {
	case c.deliveries <- d:
	case <-c.done:
	default:
		c.logger.Warn("dropping a message: the client is not keeping up",
			"topic", d.topic, "queue_depth", deliveryQueueDepth)
	}
}

// deliverLoop turns queued messages into PUBLISH packets. It runs on its own
// goroutine so that the receive-quota wait cannot block control packets such
// as PINGRESP, which would make a slow client look dead.
//
// It opens by resending whatever the last connection left unacknowledged, so
// that a resumed session's arrears go out before anything published since.
func (c *conn) deliverLoop() {
	if err := c.retransmit(); err != nil {
		c.logger.Debug("could not resend the unacknowledged messages", "error", err)
		c.close()
		return
	}

	for {
		select {
		case <-c.done:
			return
		case d := <-c.deliveries:
			if err := c.deliver(d); err != nil {
				c.logger.Debug("delivery failed", "topic", d.topic, "error", err)
				c.close()
				return
			}
		}
	}
}

func (c *conn) deliver(d *delivery) error {
	pub := &packet.Publish{
		Topic:      d.topic,
		QoS:        d.qos,
		Retain:     d.retain,
		Payload:    d.payload,
		Properties: d.props,
	}
	if d.qos == packet.QoS0 {
		return c.write(pub)
	}

	// Receive Maximum limits how many QoS 1 and QoS 2 publications may be in
	// flight towards the client at once (MQTT-5.0 §4.9).
	select {
	case c.quota <- struct{}{}:
	case <-c.done:
		return nil
	}

	id, ok := c.sess.nextID()
	if !ok {
		c.releaseQuota()
		return fmt.Errorf("no free Packet Identifier for %q", d.topic)
	}
	pub.PacketID = id
	c.sess.trackInflight(&outbound{packetID: id, qos: d.qos, publish: pub, quotaHeld: true})
	sent, err := c.writePublish(pub)
	if err != nil {
		return err
	}
	if !sent {
		// Discarded for exceeding the client's Maximum Packet Size, so no
		// acknowledgement is coming. The server must "behave as if it had
		// completed sending that Application Message" [MQTT-3.1.2-25]: the
		// exchange ends here, rather than holding a Packet Identifier and a
		// send-quota slot until the session does.
		c.sess.completeInflight(id)
		c.releaseQuota()
	}
	return nil
}

// sendRetained delivers the retained messages matching a new subscription, as
// the Retain Handling option directs (MQTT-5.0 §3.3.1.3).
func (c *conn) sendRetained(sub *subscription, handling packet.RetainHandling, replacedExisting bool) {
	if c.broker.retain == nil || handling == packet.RetainSendNever {
		return
	}
	// "No Retained Messages are sent to the Session when it first subscribes"
	// to a shared subscription (MQTT-5.0 §4.8.2).
	if sub.share != "" {
		return
	}
	// "If Retain Handling is set to 1 then if the subscription did not already
	// exist, the Server MUST send all retained messages matching the Topic
	// Filter of the subscription to the Client, and if the subscription did
	// exist the Server MUST NOT send the retained messages" [MQTT-3.3.1-10].
	if handling == packet.RetainSendOnNew && replacedExisting {
		return
	}

	for _, r := range c.broker.retain.match(sub.filter) {
		props := r.properties()
		if sub.id != 0 {
			props.SubscriptionIdentifiers = []int{sub.id}
		}
		c.enqueue(&delivery{
			topic:   r.topicName,
			qos:     minQoS(r.qos, sub.grantedQoS),
			payload: r.payload,
			props:   props,
			// "These messages are sent with the RETAIN flag set to 1"
			// (MQTT-5.0 §3.3.1.3).
			retain: true,
		})
	}
}
