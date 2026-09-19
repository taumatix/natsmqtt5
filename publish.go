package natsmqtt5

import (
	"context"
	"errors"
	"fmt"

	"github.com/taumatix/natsmqtt5/packet"
	"github.com/taumatix/natsmqtt5/topic"
)

// handlePublish processes a PUBLISH from the client: resolve any Topic Alias,
// check the topic and the QoS, authorize, store or clear the retained message,
// hand the message to NATS, and acknowledge as the QoS requires
// (MQTT-5.0 §3.3.4, Table 3-3).
func (c *conn) handlePublish(ctx context.Context, p *packet.Publish) error {
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

	topicName, err := c.resolveAlias(p)
	if err != nil {
		return err
	}
	if err := topic.ValidateName(topicName); err != nil {
		return c.rejectPublish(p, packet.TopicNameInvalid, err.Error())
	}

	// A QoS 2 PUBLISH whose Packet Identifier is already outstanding is a
	// redelivery: acknowledge it, but do not deliver the message twice
	// (MQTT-5.0 §4.3.3).
	if p.QoS == packet.QoS2 && c.sess.markQoS2Received(p.PacketID) {
		return c.write(&packet.Pubrec{Ack: packet.Ack{PacketID: p.PacketID}})
	}

	if a := c.broker.opts.Authorizer; a != nil {
		req := &AuthzRequest{
			Action: ActionPublish, ClientID: c.sess.clientID, Identity: c.sess.identity,
			Username: c.sess.username, Topic: topicName, QoS: p.QoS, Retain: p.Retain,
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
	if err := c.broker.nc.PublishMsg(msg); err != nil {
		c.logger.Warn("publishing to NATS failed", "subject", full, "error", err)
		return c.rejectPublish(p, packet.ImplementationSpecificError, "the NATS server rejected the message")
	}

	switch p.QoS {
	case packet.QoS0:
		return nil
	case packet.QoS1:
		return c.write(&packet.Puback{Ack: packet.Ack{PacketID: p.PacketID}})
	default:
		return c.write(&packet.Pubrec{Ack: packet.Ack{PacketID: p.PacketID}})
	}
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
	if alias > c.broker.opts.topicAliasMax {
		c.sendDisconnect(packet.TopicAliasInvalid,
			fmt.Sprintf("Topic Alias %d exceeds the advertised maximum of %d", alias, c.broker.opts.topicAliasMax))
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
	o, ok := c.sess.completeInflight(p.Ack.PacketID)
	if !ok {
		c.sendDisconnect(packet.ProtocolError,
			fmt.Sprintf("PUBACK for unknown Packet Identifier %d", p.Ack.PacketID))
		return fmt.Errorf("PUBACK for unknown packet id %d", p.Ack.PacketID)
	}
	if o.qos != packet.QoS1 {
		c.sendDisconnect(packet.ProtocolError,
			fmt.Sprintf("PUBACK for Packet Identifier %d, which is a QoS 2 exchange", p.Ack.PacketID))
		return errors.New("PUBACK for a QoS 2 message")
	}
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
		c.sess.completeInflight(p.Ack.PacketID)
		c.sendDisconnect(packet.ProtocolError,
			fmt.Sprintf("PUBREC for Packet Identifier %d, which is a QoS 1 exchange", p.Ack.PacketID))
		return errors.New("PUBREC for a QoS 1 message")
	}
	if p.Ack.ReasonCode.IsError() {
		// "If PUBACK or PUBREC is received containing a Reason Code of 0x80 or
		// greater the corresponding PUBLISH packet is treated as acknowledged,
		// and MUST NOT be retransmitted" [MQTT-4.4.0-2].
		done, _ := c.sess.completeInflight(p.Ack.PacketID)
		c.releaseQuotaFor(done)
		return nil
	}
	c.sess.awaitPubcomp(p.Ack.PacketID)
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
	return c.write(&packet.Pubcomp{Ack: packet.Ack{PacketID: p.Ack.PacketID, ReasonCode: code}})
}

// handlePubcomp is stage 3 of a broker-to-client QoS 2 delivery.
func (c *conn) handlePubcomp(p *packet.Pubcomp) error {
	o, ok := c.sess.completeInflight(p.Ack.PacketID)
	if !ok {
		c.sendDisconnect(packet.ProtocolError,
			fmt.Sprintf("PUBCOMP for unknown Packet Identifier %d", p.Ack.PacketID))
		return fmt.Errorf("PUBCOMP for unknown packet id %d", p.Ack.PacketID)
	}
	c.releaseQuotaFor(o)
	return nil
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
	name := will.Topic
	subject, err := topic.NameToSubject(name)
	if err != nil {
		b.logger.Warn("dropping a will message with an unusable topic",
			"client_id", s.clientID, "topic", name, "error", err)
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
		if err := b.retain.store(ctx, subject, s.clientID, p); err != nil {
			b.logger.Warn("storing a retained will message failed", "topic", name, "error", err)
		}
		cancel()
	}

	full := topic.Prefix(b.opts.SubjectPrefix, subject)
	if err := b.nc.PublishMsg(toNATS(full, s.clientID, p)); err != nil {
		b.logger.Warn("publishing a will message failed", "topic", name, "error", err)
		return
	}
	b.logger.Info("published will message", "client_id", s.clientID, "topic", name)
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
