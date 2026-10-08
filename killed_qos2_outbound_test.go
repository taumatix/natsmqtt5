package natsmqtt5_test

import (
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5/packet"
)

// A QoS 2 message the broker sent to a client, and the broker killed with SIGKILL
// at each step of the exchange (cmd/natsmqtt5 as a subprocess), put to the broker
// that takes the session over. The checkpoint interval is an hour, so whatever
// the record holds at the kill was written because of the exchange itself and not
// by the tick.
//
// Statement wording, read from the OASIS MQTT 5.0 specification (os edition),
// section 4.3.3 and 4.4:
//
//	[MQTT-4.3.3-6] "MUST NOT re-send the PUBLISH once it has sent the
//	  corresponding PUBREL packet."
//	[MQTT-4.3.3-5] "MUST treat the PUBREL packet as unacknowledged until it has
//	  received the corresponding PUBCOMP packet from the receiver."
//	[MQTT-4.4.0-1] "When a Client reconnects with Clean Start set to 0 and a
//	  session is present, both the Client and Server MUST resend any
//	  unacknowledged PUBLISH packets (where QoS > 0) and PUBREL packets using
//	  their original Packet Identifiers. ... Clients and Servers MUST NOT resend
//	  messages at any other time."

// outboundQoS2 is a client subscribed at QoS 2 to a broker that is killed, and a
// publisher on the surviving one that sends it one message.
func outboundQoS2(t *testing.T, clientID string) (child *killableBroker, survivor string, victim *rawClient, sent *packet.Publish) {
	t.Helper()
	natsURL := startNATS(t)
	child = startBrokerBinary(t, natsURL, "-persistent-sessions", "-session-checkpoint-interval", "1h")
	survivor = startBroker(t, natsURL, persistentWithQueue)

	victim = dialRaw(t, child.addr)
	require.False(t, victim.connect(rawConnect(clientID, 300)).SessionPresent)
	victim.subscribe("ko/#", packet.QoS2)
	pub, _ := connectClient(t, survivor, connectOpts(clientID+"-pub"))
	pub.publish(&paho.Publish{Topic: "ko/a", QoS: 2, Payload: []byte("once")})
	sent = victim.expectPublish()
	require.Equal(t, packet.QoS2, sent.QoS)
	require.False(t, sent.Dup)
	return child, survivor, victim, sent
}

// afterTheKill reconnects the session on the survivor and returns what the
// broker sends in the next moments.
func afterTheKill(t *testing.T, survivor, clientID string) (*rawClient, []packet.Packet) {
	t.Helper()
	back := dialRaw(t, survivor)
	require.True(t, back.connect(rawConnect(clientID, 300)).SessionPresent, "[MQTT-3.1.2-5]")
	return back, back.p3Drain(700 * time.Millisecond)
}

// [MQTT-4.4.0-1]: a QoS 2 PUBLISH the client had been sent and had not answered
// when the broker was killed is sent again once, by the broker that takes the
// session over, with its original Packet Identifier and DUP set, and not also as
// a new message.
func TestAKilledBrokersQoS2PublishSentJustBeforeTheKillIsResentNotRepeated(t *testing.T) {
	child, survivor, _, sent := outboundQoS2(t, "ko-sent")
	child.kill9(t)

	back, got := afterTheKill(t, survivor, "ko-sent")
	pubs := p3Publishes(got)
	require.Len(t, pubs, 1, "[MQTT-4.4.0-1] the unacknowledged PUBLISH once, not again as a new message")
	assert.Equal(t, sent.PacketID, pubs[0].PacketID, "the original Packet Identifier")
	assert.True(t, pubs[0].Dup, "a resend has DUP set [MQTT-3.3.1-1]")
	assert.Equal(t, "once", string(pubs[0].Payload))

	back.send(&packet.Pubrec{Ack: packet.Ack{PacketID: sent.PacketID}})
	assert.Equal(t, sent.PacketID, back.expectPubrel().PacketID)
}

// [MQTT-4.3.3-6], [MQTT-4.4.0-1]: a QoS 2 message whose PUBREL the broker had sent
// when it was killed is not sent again as a PUBLISH, by the broker that takes
// the session over; the PUBREL is the unacknowledged packet and is resent.
func TestAKilledBrokersQoS2MessageAfterPubrelIsNotPublishedAgain(t *testing.T) {
	child, survivor, victim, sent := outboundQoS2(t, "ko-rel")
	victim.send(&packet.Pubrec{Ack: packet.Ack{PacketID: sent.PacketID}})
	require.Equal(t, sent.PacketID, victim.expectPubrel().PacketID)
	child.kill9(t)

	back, got := afterTheKill(t, survivor, "ko-rel")
	assert.Empty(t, p3Publishes(got), "[MQTT-4.3.3-6] no PUBLISH once the PUBREL has been sent")
	require.Len(t, got, 1)
	rel, ok := got[0].(*packet.Pubrel)
	require.True(t, ok, "[MQTT-4.4.0-1] the unacknowledged PUBREL is resent, got %s", got[0].Type())
	assert.Equal(t, sent.PacketID, rel.PacketID)
	back.send(&packet.Pubcomp{Ack: packet.Ack{PacketID: rel.PacketID}})
	back.expectNothing()
}

// [MQTT-4.3.3-6], [MQTT-4.4.0-1]: a QoS 2 exchange the client completed with its
// PUBCOMP just before the kill is not delivered to it again as a new message. The
// broker had not seen the end of it on its record, so the PUBREL goes out again,
// which the client answers, and the message is not published a second time.
func TestAKilledBrokersQoS2MessageCompletedJustBeforeTheKillIsNotDeliveredAgain(t *testing.T) {
	child, survivor, victim, sent := outboundQoS2(t, "ko-done")
	victim.send(&packet.Pubrec{Ack: packet.Ack{PacketID: sent.PacketID}})
	require.Equal(t, sent.PacketID, victim.expectPubrel().PacketID)
	victim.send(&packet.Pubcomp{Ack: packet.Ack{PacketID: sent.PacketID}})
	// The PUBCOMP is read by the broker before the kill: a PUBLISH on the same
	// connection that is answered shows it has been handled.
	victim.send(&packet.Pingreq{})
	require.IsType(t, &packet.Pingresp{}, victim.read())
	child.kill9(t)

	back, got := afterTheKill(t, survivor, "ko-done")
	assert.Empty(t, p3Publishes(got), "the completed exchange is not delivered again")
	require.Len(t, got, 1, "the record said PUBREL, so that is what is resent [MQTT-4.4.0-1]")
	for _, p := range got {
		rel, ok := p.(*packet.Pubrel)
		require.True(t, ok, "only the PUBREL may be resent, got %s", p.Type())
		assert.Equal(t, sent.PacketID, rel.PacketID)
		back.send(&packet.Pubcomp{Ack: packet.Ack{PacketID: rel.PacketID}})
	}
	back.expectNothing()
}
