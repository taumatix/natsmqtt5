package natsmqtt5

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/taumatix/natsmqtt5/packet"
	"github.com/taumatix/natsmqtt5/topic"
)

// handlePublish processes a PUBLISH from the client: resolve any Topic Alias,
// check the topic and the QoS, authorize, store or clear the retained message,
// hand the message to NATS, and acknowledge as the QoS requires
// (MQTT-5.0 §3.3.4, Table 3-3).
func (c *conn) handlePublish(ctx context.Context, p *packet.Publish) error {
	// "A PUBLISH packet sent from a Client to a Server MUST NOT contain a
	// Subscription Identifier" [MQTT-3.3.4-6]. The decoder accepts the property
	// because the server sends it; from a client it is a Protocol Error, answered
	// with DISCONNECT 0x82 and the connection closed [MQTT-4.13.1-1] before
	// anything is acknowledged or forwarded.
	if p.Properties != nil && len(p.Properties.SubscriptionIdentifiers) > 0 {
		c.sendDisconnect(packet.ProtocolError, "a PUBLISH from a client must not carry a Subscription Identifier")
		return errors.New("PUBLISH carrying a Subscription Identifier")
	}

	if p.QoS > c.broker.opts.maxQoS {
		// [MQTT-3.2.2-11] makes this a protocol error the server reports with
		// 0x9B.
		c.sendDisconnect(packet.QoSNotSupported,
			fmt.Sprintf("QoS %d exceeds the broker's maximum of %d", p.QoS, c.broker.opts.maxQoS))
		return fmt.Errorf("PUBLISH at QoS %d above the broker maximum", p.QoS)
	}
	if p.Retain && !c.broker.retainAvailable() {
		// [MQTT-3.2.2-14]
		c.sendDisconnect(packet.RetainNotSupported, "this broker has retained messages disabled")
		return errors.New("retained PUBLISH but retained messages are disabled")
	}

	// "The Response Topic MUST NOT contain wildcard characters" [MQTT-3.3.2-14].
	// The packet parsed, so it is not Malformed; it holds data the protocol does
	// not allow, which is a Protocol Error (MQTT-5.0 §1.2): DISCONNECT 0x82 and
	// the connection closed [MQTT-4.13.1-1]. 0x90 would describe this PUBLISH's own
	// Topic Name, and a QoS 0 PUBLISH has no acknowledgement to carry it.
	if p.Properties != nil && hasWildcard(p.Properties.ResponseTopic) {
		c.sendDisconnect(packet.ProtocolError,
			fmt.Sprintf("the Response Topic %q contains a wildcard", p.Properties.ResponseTopic))
		return errors.New("PUBLISH with a wildcard in its Response Topic")
	}

	topicName, err := c.resolveAlias(p)
	if err != nil {
		return err
	}
	if err := topic.ValidateName(topicName); err != nil {
		return c.rejectPublish(p, packet.TopicNameInvalid, err.Error())
	}
	if c.broker.opts.RestrictDollarTopics && dollarName(topicName) {
		// MQTT-5.0 §4.7.2 (dollar.go).
		return c.rejectPublish(p, packet.TopicNameInvalid, "clients may not publish to topics starting with $")
	}

	// A QoS 2 PUBLISH whose Packet Identifier is already outstanding is a
	// redelivery: acknowledge it, but do not deliver the message twice
	// (MQTT-5.0 §4.3.3).
	if p.QoS == packet.QoS2 && c.sess.markQoS2Received(p.PacketID) {
		return c.write(&packet.Pubrec{Ack: packet.Ack{PacketID: p.PacketID}})
	}

	if a := c.broker.opts.Authorizer; a != nil {
		identity, username := c.sess.principal()
		req := &AuthzRequest{
			Action: ActionPublish, ClientID: c.sess.clientID, Identity: identity,
			Username: username, Topic: topicName, QoS: p.QoS, Retain: p.Retain,
		}
		if err := a.Authorize(ctx, req); err != nil {
			c.logger.Debug("publish denied", "topic", topicName, "error", err)
			return c.rejectPublish(p, packet.NotAuthorized, "")
		}
	}

	subject, err := topic.NameToSubject(topicName)
	if err != nil {
		return c.rejectPublish(p, packet.TopicNameInvalid, err.Error())
	}
	full := topic.Prefix(c.broker.opts.SubjectPrefix, subject)

	// The retained store is updated before the message is fanned out, so a
	// subscriber that arrives immediately after cannot see the message on the
	// wire but miss it in the retained set.
	if p.Retain {
		if err := c.broker.retain.store(ctx, subject, c.sess.clientID, p); err != nil {
			c.logger.Warn("storing a retained message failed", "topic", topicName, "error", err)
			return c.rejectPublish(p, packet.UnspecifiedError, "could not store the retained message")
		}
	}

	msg := toNATS(full, c.sess.clientID, p)
	if q := c.broker.queue; q != nil && p.QoS > packet.QoS0 {
		// Queued first, and confirmed; see offlineQueue.keep for why the
		// order matters. Refusing on failure lets the client try again rather
		// than have the message reach only the sessions that are connected.
		kctx, cancel := context.WithTimeout(ctx, offlineKeepTimeout)
		err := q.keep(kctx, c.broker.js, subject, msg)
		cancel()
		if err != nil {
			c.logger.Warn("queueing a message for disconnected sessions failed", "subject", full, "error", err)
			return c.rejectPublish(p, packet.ImplementationSpecificError, "the message could not be queued")
		}
	}
	if err := c.broker.nc.PublishMsg(msg); err != nil {
		c.logger.Warn("publishing to NATS failed", "subject", full, "error", err)
		return c.rejectPublish(p, packet.ImplementationSpecificError, "the NATS server rejected the message")
	}
	if c.broker.opts.DurablePublish && p.QoS > packet.QoS0 {
		// The queue copy is stored; now wait until the NATS server has
		// processed the live publish too (Options.DurablePublish). A refusal
		// lets the client send it again.
		fctx, cancel := context.WithTimeout(ctx, offlineKeepTimeout)
		err := c.broker.nc.FlushWithContext(fctx)
		cancel()
		if err != nil {
			c.logger.Warn("NATS did not confirm a durable publish", "subject", full, "error", err)
			return c.rejectPublish(p, packet.ImplementationSpecificError, "the NATS server did not confirm the message")
		}
	}

	switch p.QoS {
	case packet.QoS0:
		return nil
	case packet.QoS1:
		return c.write(&packet.Puback{Ack: packet.Ack{PacketID: p.PacketID}})
	default:
		// The identifier is in the stored record before the PUBREC that tells the
		// client its PUBLISH need not be sent again; a successor of a broker killed
		// after it must not forward a resend [MQTT-4.3.3-10].
		c.sess.qos2Forwarded(p.PacketID)
		c.checkpointNow()
		return c.write(&packet.Pubrec{Ack: packet.Ack{PacketID: p.PacketID}})
	}
}

// hasWildcard reports whether a Response Topic holds a wildcard character
// [MQTT-3.3.2-14].
func hasWildcard(responseTopic string) bool {
	return strings.ContainsAny(responseTopic, "+#")
}

// resolveAlias applies the Topic Alias rules of MQTT-5.0 §3.3.2.3.4: a PUBLISH
// with a non-empty Topic Name and an alias establishes the mapping, and one
// with an empty Topic Name uses it.
func (c *conn) resolveAlias(p *packet.Publish) (string, error) {
	props := p.Properties
	if props == nil || props.TopicAlias == nil {
		return p.Topic, nil
	}
	alias := *props.TopicAlias

	// "A Server MUST accept all Topic Alias values greater than 0 and less
	// than or equal to the Topic Alias Maximum value that it returned in the
	// CONNACK" [MQTT-3.3.2-12]; anything above it is 0x94.
	if alias > c.topicAliasMax {
		c.sendDisconnect(packet.TopicAliasInvalid,
			fmt.Sprintf("Topic Alias %d exceeds the advertised maximum of %d", alias, c.topicAliasMax))
		return "", fmt.Errorf("topic alias %d above maximum", alias)
	}

	c.aliasMu.Lock()
	defer c.aliasMu.Unlock()
	if p.Topic != "" {
		c.aliases[alias] = p.Topic
		return p.Topic, nil
	}
	name, ok := c.aliases[alias]
	if !ok {
		c.sendDisconnect(packet.TopicAliasInvalid,
			fmt.Sprintf("Topic Alias %d has not been mapped on this connection", alias))
		return "", fmt.Errorf("unmapped topic alias %d", alias)
	}
	return name, nil
}

// rejectPublish answers a refused PUBLISH. At QoS 0 there is no acknowledgement
// to carry the reason, so the message is simply dropped (MQTT-5.0 Table 3-3).
func (c *conn) rejectPublish(p *packet.Publish, code packet.ReasonCode, reason string) error {
	if !c.requestProblemInfo {
		// With Request Problem Information 0 the server must not put a Reason
		// String on a PUBACK or PUBREC [MQTT-3.1.2-29].
		reason = ""
	}
	var props *packet.Properties
	if reason != "" {
		props = &packet.Properties{ReasonString: reason}
	}

	switch p.QoS {
	case packet.QoS0:
		c.logger.Debug("dropping a QoS 0 publish", "topic", p.Topic, "reason", code.String())
		return nil
	case packet.QoS1:
		return c.write(&packet.Puback{Ack: packet.Ack{PacketID: p.PacketID, ReasonCode: code, Properties: props}})
	default:
		c.sess.releaseQoS2(p.PacketID)
		return c.write(&packet.Pubrec{Ack: packet.Ack{PacketID: p.PacketID, ReasonCode: code, Properties: props}})
	}
}

// handlePuback completes a QoS 1 delivery to the client and returns its slot
// in the receive quota (MQTT-5.0 §4.9).
func (c *conn) handlePuback(p *packet.Puback) error {
	o, ok := c.sess.completeInflight(c, p.Ack.PacketID)
	if !ok {
		if c.forgetWithdrawn(p.Ack.PacketID, packet.PUBACK) || c.forgetResent(p.Ack.PacketID, packet.PUBACK) {
			return nil
		}
		c.sendDisconnect(packet.ProtocolError,
			fmt.Sprintf("PUBACK for unknown Packet Identifier %d", p.Ack.PacketID))
		return fmt.Errorf("PUBACK for unknown packet id %d", p.Ack.PacketID)
	}
	if o.qos != packet.QoS1 {
		c.sendDisconnect(packet.ProtocolError,
			fmt.Sprintf("PUBACK for Packet Identifier %d, which is a QoS 2 exchange", p.Ack.PacketID))
		return errors.New("PUBACK for a QoS 2 message")
	}
	c.activity.Add(1)
	c.releaseQuotaFor(o)
	return nil
}

// handlePubrec is stage 1 of a broker-to-client QoS 2 delivery. An error code
// ends the exchange there [MQTT-4.8.2-6].
func (c *conn) handlePubrec(p *packet.Pubrec) error {
	o, ok := c.sess.inflightEntry(p.Ack.PacketID)
	if !ok {
		return c.write(&packet.Pubrel{Ack: packet.Ack{
			PacketID:   p.Ack.PacketID,
			ReasonCode: packet.PacketIdentifierNotFound,
		}})
	}
	if o.qos != packet.QoS2 {
		// The mirror of the check handlePuback makes, down to ending the
		// exchange before the disconnect: letting it through would move a QoS 1
		// exchange on to expecting a PUBCOMP that its client has no reason to
		// send, and a resumed session would then resend a PUBREL for it on
		// every resumption [MQTT-4.4.0-1].
		c.sess.completeInflight(c, p.Ack.PacketID)
		c.sendDisconnect(packet.ProtocolError,
			fmt.Sprintf("PUBREC for Packet Identifier %d, which is a QoS 1 exchange", p.Ack.PacketID))
		return errors.New("PUBREC for a QoS 1 message")
	}
	if p.Ack.ReasonCode.IsError() {
		// "If PUBACK or PUBREC is received containing a Reason Code of 0x80 or
		// greater the corresponding PUBLISH packet is treated as acknowledged,
		// and MUST NOT be retransmitted" [MQTT-4.4.0-2].
		done, _ := c.sess.completeInflight(c, p.Ack.PacketID)
		c.activity.Add(1)
		c.releaseQuotaFor(done)
		return nil
	}
	c.sess.awaitPubcomp(p.Ack.PacketID)
	c.activity.Add(1)
	// The record says PUBREL before the PUBREL goes out: the client may answer it
	// and the broker be killed before the next tick, and a successor that still
	// resent the PUBLISH would break [MQTT-4.3.3-6].
	c.checkpointNow()
	return c.write(&packet.Pubrel{Ack: packet.Ack{PacketID: p.Ack.PacketID}})
}

// handlePubrel is stage 2 of a client-to-broker QoS 2 delivery: the client
// releases the Packet Identifier and the broker confirms with PUBCOMP.
func (c *conn) handlePubrel(p *packet.Pubrel) error {
	code := packet.Success
	if !c.sess.releaseQoS2(p.Ack.PacketID) {
		// "If the Packet Identifier is not found, the receiver uses 0x92"
		// (MQTT-5.0 §3.6.2.1).
		code = packet.PacketIdentifierNotFound
	}
	// Released in the stored record before the PUBCOMP, after which the
	// identifier is a new message again [MQTT-4.3.3-12]: a successor that still
	// held it would take that PUBLISH for a repeat and drop it.
	c.checkpointNow()
	return c.write(&packet.Pubcomp{Ack: packet.Ack{PacketID: p.Ack.PacketID, ReasonCode: code}})
}

// handlePubcomp is stage 3 of a broker-to-client QoS 2 delivery.
func (c *conn) handlePubcomp(p *packet.Pubcomp) error {
	o, ok := c.sess.completeInflight(c, p.Ack.PacketID)
	if !ok {
		// A withdrawn QoS 2 exchange reaches here by way of handlePubrec, which
		// answers an identifier it no longer holds with 0x92 and leaves the
		// client owing the PUBCOMP that closes it (MQTT-5.0 §3.6.2.1).
		if c.forgetWithdrawn(p.Ack.PacketID, packet.PUBCOMP) || c.forgetResent(p.Ack.PacketID, packet.PUBCOMP) {
			return nil
		}
		c.sendDisconnect(packet.ProtocolError,
			fmt.Sprintf("PUBCOMP for unknown Packet Identifier %d", p.Ack.PacketID))
		return fmt.Errorf("PUBCOMP for unknown packet id %d", p.Ack.PacketID)
	}
	c.activity.Add(1)
	c.releaseQuotaFor(o)
	return nil
}

// forgetWithdrawn settles an acknowledgement for an identifier the session
// only remembers as owed, returning the send-quota slot it was holding if that
// slot is this connection's.
func (c *conn) forgetWithdrawn(id uint16, t packet.Type) bool {
	w, ok := c.sess.forgetWithdrawn(id, t)
	if ok && w.quotaHolder == c {
		c.releaseQuota()
	}
	return ok
}

// forgetResent settles the acknowledgement of a copy the broker resent after the
// original had been acknowledged, returning the send-quota slot the copy held
// if the original's acknowledgement left it to the copy.
func (c *conn) forgetResent(id uint16, t packet.Type) bool {
	ok, quota := c.sess.forgetResent(c, id, t)
	if ok && quota {
		c.releaseQuota()
	}
	return ok
}

// releaseQuotaFor returns the send-quota slot a completed exchange was holding,
// if it was holding one on this connection. An exchange begun on an earlier
// connection is not: the quota was re-initialised when this one started
// (MQTT-5.0 §4.9), and a resend takes a fresh slot or, in the case of a PUBREL,
// none at all.
func (c *conn) releaseQuotaFor(o outbound) {
	if o.quotaHeld {
		c.releaseQuota()
	}
}

func (c *conn) releaseQuota() {
	select {
	case <-c.quota:
	default:
	}
}

// publishWill publishes a session's Will Message (MQTT-5.0 §3.1.2.5). It runs
// on the broker rather than the connection, because the Will may fire after
// the connection object is gone.
func (b *Broker) publishWill(s *session, will *packet.Will) {
	b.publishWillFor(s.clientID, will)
}

// publishAdoptedWill publishes the Will of a connection whose broker is gone.
func (b *Broker) publishAdoptedWill(clientID string, will *packet.Will) {
	b.publishWillFor(clientID, will)
}

func (b *Broker) publishWillFor(clientID string, will *packet.Will) {
	name := will.Topic
	subject, err := topic.NameToSubject(name)
	if err != nil {
		b.logger.Warn("dropping a will message with an unusable topic",
			"client_id", clientID, "topic", name, "error", err)
		return
	}

	p := &packet.Publish{
		Topic:      name,
		QoS:        will.QoS,
		Retain:     will.Retain,
		Payload:    will.Payload,
		Properties: willProperties(will),
	}
	if will.Retain && b.retain != nil {
		ctx, cancel := context.WithTimeout(context.Background(), retainStoreTimeout)
		if err := b.retain.store(ctx, subject, clientID, p); err != nil {
			b.logger.Warn("storing a retained will message failed", "topic", name, "error", err)
		}
		cancel()
	}

	full := topic.Prefix(b.opts.SubjectPrefix, subject)
	msg := toNATS(full, clientID, p)
	if b.queue != nil && p.QoS > packet.QoS0 {
		kctx, cancel := context.WithTimeout(context.Background(), offlineKeepTimeout)
		if err := b.queue.keep(kctx, b.js, subject, msg); err != nil {
			// A Will has no one to refuse to; it still goes out live.
			b.logger.Warn("queueing a will message failed", "topic", name, "error", err)
		}
		cancel()
	}
	if err := b.nc.PublishMsg(msg); err != nil {
		b.logger.Warn("publishing a will message failed", "topic", name, "error", err)
		return
	}
	b.logger.Info("published will message", "client_id", clientID, "topic", name)
}

// willProperties selects the Will Properties that describe the message itself.
// Will Delay Interval is not among them: it controls when the message is
// published, not what subscribers receive (MQTT-5.0 §3.1.3.2.2).
func willProperties(will *packet.Will) *packet.Properties {
	if will.Properties == nil {
		return nil
	}
	src := will.Properties
	return &packet.Properties{
		PayloadFormat:         src.PayloadFormat,
		MessageExpiryInterval: src.MessageExpiryInterval,
		ContentType:           src.ContentType,
		ResponseTopic:         src.ResponseTopic,
		CorrelationData:       src.CorrelationData,
		User:                  src.User,
	}
}
