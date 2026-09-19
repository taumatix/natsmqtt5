package natsmqtt5

import "github.com/taumatix/natsmqtt5/packet"

// retransmit resends what the previous network connection left unacknowledged:
//
//	"When a Client reconnects with Clean Start set to 0 and a session is
//	present, both the Client and Server MUST resend any unacknowledged PUBLISH
//	packets (where QoS > 0) and PUBREL packets using their original Packet
//	Identifiers. This is the only circumstance where a Client or Server is
//	REQUIRED to resend messages. Clients and Servers MUST NOT resend messages at
//	any other time" [MQTT-4.4.0-1].
//
// That last sentence is why there is no retry timer anywhere in this broker: a
// resend is an event of resumption, not of time passing. The whole of the
// resending this broker does is the loop below, and it runs once per connection.
//
// A connection with nothing to resend runs it too and finds the in-flight set
// empty, which is the case for every new session and for a Clean Start — the
// session object those get is a fresh one. So there is no "was this resumed?"
// condition to keep in step with the handshake.
//
// It runs on the delivery goroutine before that loop reads its first message,
// which is what keeps a resend ahead of anything published since: a Server
// "MUST send PUBLISH packets to consumers (for the same Topic and QoS) in the
// order that they were received from any given Client" [MQTT-4.6.0-5], and by
// default "MUST treat every Topic as an Ordered Topic when it is forwarding
// messages on Non-shared Subscriptions" [MQTT-4.6.0-6]. Messages arriving
// meanwhile queue in c.deliveries as they would behind any other slow send.
func (c *conn) retransmit() error {
	for _, snapshot := range c.sess.unacknowledged() {
		if err := c.resend(snapshot); err != nil {
			return err
		}
	}
	return nil
}

// resend puts one in-flight entry back on the wire.
//
// The snapshot says what to expect; the live entry decides. Both can differ by
// the time the entry is reached, because the quota wait below is unbounded and
// because a connection displaced mid-exchange keeps decoding the packets its
// socket had already buffered — so an acknowledgement for this identifier may
// land, on either connection, while an earlier entry is waiting for room.
// Acting on the snapshot would then resend a completed exchange, and the
// client's acknowledgement of it arrives for a Packet Identifier the session no
// longer holds, which this broker answers by disconnecting the client.
func (c *conn) resend(snapshot outbound) error {
	// A PUBREL costs no send quota, so an entry that was past its PUBREC when
	// the set was read asks for none. The reverse cannot happen: an exchange
	// never goes back to expecting a PUBREC, it is only completed and removed.
	held := false
	if !snapshot.awaitingPubcomp {
		select {
		case c.quota <- struct{}{}:
			held = true
		case <-c.done:
			return nil
		}
	}

	o, live := c.sess.inflightEntry(snapshot.packetID)
	spent, err := c.writeResend(o, live)
	if held && !spent {
		// Nothing went out that an acknowledgement will pay for.
		c.releaseQuota()
	}
	return err
}

// writeResend writes the packet the entry still owes and reports whether it
// consumed a send-quota slot.
func (c *conn) writeResend(o outbound, live bool) (spentQuota bool, err error) {
	switch {
	case !live:
		// Acknowledged while this resumption was working through the set.
		return false, nil

	case o.awaitingPubcomp:
		// The client took ownership of the message when it sent the PUBREC
		// [MQTT-4.3.3-8], so what is outstanding is the PUBCOMP and it is the
		// PUBREL that goes out again.
		//
		// It spends no send quota: the quota counts PUBLISH packets at QoS > 0
		// (MQTT-5.0 §4.9) and a PUBREL is not one. The PUBCOMP answering it
		// then replenishes a quota this connection never spent, which the
		// specification names as the expected case — "the attempt to increment
		// above the initial send quota might be caused by the re-transmission
		// of a PUBREL packet after a new Network Connection is established" —
		// and handles by capping the quota at its initial value rather than by
		// tracking which message holds which unit. That cap is what
		// releaseQuota's non-blocking receive gives when the quota is
		// otherwise idle; when it is not, this broker over-replenishes exactly
		// as §4.9's own counter would.
		return false, c.write(&packet.Pubrel{Ack: packet.Ack{PacketID: o.packetID}})
	}

	// Claim the slot for this entry before the packet goes out, so that the
	// acknowledgement returns this slot and not one belonging to something
	// else. A false return means the exchange completed in the moment between
	// the read above and here.
	if !c.sess.takeQuotaSlot(o.packetID) {
		return false, nil
	}

	// DUP is set by the fact of the retransmission and is never copied from
	// anywhere [MQTT-3.3.1-3]. The stored packet is left untouched: it belongs
	// to the session, which outlives this connection.
	dup := *o.publish
	dup.Dup = true
	sent, err := c.writePublish(&dup)
	if err != nil {
		return false, err
	}
	if !sent {
		// Too large for this client to accept, so it will never be
		// acknowledged. The server must "behave as if it had completed sending
		// that Application Message" [MQTT-3.1.2-25]; keeping it in flight
		// instead would resend an undeliverable packet on every future
		// resumption and hold its quota slot for the life of the session.
		c.sess.completeInflight(o.packetID)
		return false, nil
	}
	return true, nil
}
