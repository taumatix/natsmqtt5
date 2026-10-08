package natsmqtt5_test

import (
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5/packet"
)

// The in-flight state of a session whose broker is killed with SIGKILL
// (cmd/natsmqtt5 as a subprocess; see killed_broker_test.go), put to the broker
// that takes the session over.
//
// Statement wording, read from the OASIS MQTT 5.0 specification (os edition):
//
//	[MQTT-4.4.0-1]  "When a Client reconnects with Clean Start set to 0 and a
//	  session is present, both the Client and Server MUST resend any
//	  unacknowledged PUBLISH packets (where QoS > 0) and PUBREL packets using
//	  their original Packet Identifiers. ... Clients and Servers MUST NOT resend
//	  messages at any other time."
//	[MQTT-4.3.3-10] "The receiver MUST respond with a PUBREC containing the Packet
//	  Identifier from the incoming PUBLISH packet, having accepted ownership of
//	  the Application Message. ... until it has received the corresponding PUBREL
//	  packet, the receiver MUST acknowledge any subsequent PUBLISH packet with the
//	  same Packet Identifier by sending a PUBREC. It MUST NOT cause duplicate
//	  messages to be delivered to any onward recipients in this case."
//	[MQTT-4.3.3-12] "After it has sent a PUBCOMP, the receiver MUST treat any
//	  subsequent PUBLISH packet that contains that Packet Identifier as being a
//	  new Application Message."

// [MQTT-4.4.0-1]: a QoS 1 message the client had been sent and had not
// acknowledged when the broker was killed is sent again by the broker that takes
// the session over, with its original Packet Identifier and DUP set, and is not
// sent a third time once acknowledged.
func TestAKilledBrokersUnacknowledgedQoS1MessageIsResentWithItsOriginalID(t *testing.T) {
	natsURL := startNATS(t)
	child := startBrokerBinary(t, natsURL, "-persistent-sessions", "-session-checkpoint-interval", killedBrokerCheckpoint)
	survivor := startBroker(t, natsURL, persistentWithQueue)

	victim := dialRaw(t, child.addr)
	victim.connect(rawConnect("killed-qos1", 300))
	victim.subscribe("k1/#", packet.QoS1)
	pub, _ := connectClient(t, survivor, connectOpts("k1-pub"))
	pub.publish(&paho.Publish{Topic: "k1/a", QoS: 1, Payload: []byte("a")})
	sent := victim.expectPublish()
	require.False(t, sent.Dup)

	time.Sleep(700 * time.Millisecond) // the checkpoint with it in flight
	child.kill9(t)

	back := dialRaw(t, survivor)
	require.True(t, back.connect(rawConnect("killed-qos1", 300)).SessionPresent)
	again := back.expectPublish()
	assert.Equal(t, sent.PacketID, again.PacketID, "[MQTT-4.4.0-1] the original Packet Identifier")
	assert.True(t, again.Dup, "a resend has DUP set [MQTT-3.3.1-1]")
	assert.Equal(t, "k1/a", again.Topic)
	back.send(&packet.Puback{Ack: packet.Ack{PacketID: again.PacketID}})
	back.expectNothing()
}

// [MQTT-4.4.0-1]: a QoS 2 message whose PUBREC the client had sent, and whose
// PUBCOMP it had not, is continued by the broker that takes the session over
// with a PUBREL carrying the original Packet Identifier, not with the PUBLISH
// again.
func TestAKilledBrokersQoS2MessageAwaitingPubcompIsContinuedWithPubrel(t *testing.T) {
	natsURL := startNATS(t)
	child := startBrokerBinary(t, natsURL, "-persistent-sessions", "-session-checkpoint-interval", killedBrokerCheckpoint)
	survivor := startBroker(t, natsURL, persistentWithQueue)

	victim := dialRaw(t, child.addr)
	victim.connect(rawConnect("killed-qos2", 300))
	victim.subscribe("k2/#", packet.QoS2)
	pub, _ := connectClient(t, survivor, connectOpts("k2-pub"))
	pub.publish(&paho.Publish{Topic: "k2/a", QoS: 2, Payload: []byte("a")})
	sent := victim.expectPublish()
	victim.send(&packet.Pubrec{Ack: packet.Ack{PacketID: sent.PacketID}})
	rel := victim.expectPubrel()
	require.Equal(t, sent.PacketID, rel.PacketID)

	time.Sleep(700 * time.Millisecond)
	child.kill9(t)

	back := dialRaw(t, survivor)
	require.True(t, back.connect(rawConnect("killed-qos2", 300)).SessionPresent)
	again := back.expectPubrel()
	assert.Equal(t, sent.PacketID, again.PacketID, "[MQTT-4.4.0-1] PUBREL with the original Packet Identifier")
	back.send(&packet.Pubcomp{Ack: packet.Ack{PacketID: again.PacketID}})
	back.expectNothing()
}

// inboundQoS2 is a QoS 2 publisher on a killable broker and a subscriber on the
// surviving one. The checkpoint interval is an hour, so that whatever the record
// holds when the broker is killed was written because of the exchange itself.
func inboundQoS2(t *testing.T, clientID, filter string) (child *killableBroker, survivorAddr string, sub *rawClient) {
	t.Helper()
	natsURL := startNATS(t)
	child = startBrokerBinary(t, natsURL, "-persistent-sessions", "-session-checkpoint-interval", "1h")
	survivor := startBroker(t, natsURL, persistentWithQueue)
	sub = dialRaw(t, survivor)
	sub.connect(rawConnect(clientID+"-sub", 0))
	sub.subscribe(filter, packet.QoS2)
	return child, survivor, sub
}

// [MQTT-4.3.3-10]: a client whose broker was killed after it had answered its
// QoS 2 PUBLISH with a PUBREC sends the PUBLISH again, on the broker that takes
// the session over; that broker answers it with a PUBREC and does not forward
// the message a second time.
func TestAKilledBrokersReceivedQoS2MessageIsNotForwardedAgainOnAResend(t *testing.T) {
	child, survivor, sub := inboundQoS2(t, "killed-in", "ki/#")

	victim := dialRaw(t, child.addr)
	victim.connect(rawConnect("killed-in", 300))
	victim.send(&packet.Publish{QoS: packet.QoS2, Topic: "ki/a", PacketID: 7, Payload: []byte("once")})
	require.IsType(t, &packet.Pubrec{}, victim.read())
	receiveQoS2(t, sub, "ki/a")

	child.kill9(t)

	back := dialRaw(t, survivor)
	require.True(t, back.connect(rawConnect("killed-in", 300)).SessionPresent)
	back.send(&packet.Publish{Dup: true, QoS: packet.QoS2, Topic: "ki/a", PacketID: 7, Payload: []byte("once")})
	rec, ok := back.read().(*packet.Pubrec)
	require.True(t, ok, "the repeated PUBLISH is acknowledged with a PUBREC")
	assert.Equal(t, uint16(7), rec.PacketID)
	sub.expectNothing()

	back.send(&packet.Pubrel{Ack: packet.Ack{PacketID: 7}})
	comp, ok := back.read().(*packet.Pubcomp)
	require.True(t, ok)
	assert.Equal(t, packet.Success, comp.ReasonCode, "the identifier was held, so the PUBREL is known")
}

// [MQTT-4.3.3-12]: a Packet Identifier whose exchange was completed with PUBCOMP
// before the broker was killed is a new message again on the broker that takes
// over, and is forwarded.
func TestAKilledBrokersCompletedQoS2IdentifierIsANewMessageAgain(t *testing.T) {
	child, survivor, sub := inboundQoS2(t, "killed-done", "kd/#")

	victim := dialRaw(t, child.addr)
	victim.connect(rawConnect("killed-done", 300))
	victim.send(&packet.Publish{QoS: packet.QoS2, Topic: "kd/a", PacketID: 7, Payload: []byte("first")})
	require.IsType(t, &packet.Pubrec{}, victim.read())
	victim.send(&packet.Pubrel{Ack: packet.Ack{PacketID: 7}})
	require.IsType(t, &packet.Pubcomp{}, victim.read())
	receiveQoS2(t, sub, "kd/a")

	child.kill9(t)

	back := dialRaw(t, survivor)
	require.True(t, back.connect(rawConnect("killed-done", 300)).SessionPresent)
	back.send(&packet.Publish{QoS: packet.QoS2, Topic: "kd/b", PacketID: 7, Payload: []byte("second")})
	require.IsType(t, &packet.Pubrec{}, back.read())
	receiveQoS2(t, sub, "kd/b")
}
