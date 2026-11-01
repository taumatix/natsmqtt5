package natsmqtt5_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5/v2"
	"github.com/taumatix/natsmqtt5/v2/packet"
)

// QoS 2 on a shared subscription across a killed broker, at the two points the
// roadmap's "QoS 2 on shared subscriptions" entry named as duplicate windows:
// a message sent before any periodic checkpoint wrote it, and a PUBREC read
// before the record naming the PUBREL was written. The member's broker is
// killed (natsmqtt5.Kill, which closes its NATS connection and sockets under
// it), and the client comes back on another broker within the group's AckWait.
//
// Statement wording, read from the OASIS MQTT 5.0 specification (os edition):
//
//	[MQTT-4.8.2-4] "If the Server is in the process of sending a QoS 2 message
//	  to its chosen subscribing Client and the connection to the Client breaks
//	  before delivery is complete, the Server MUST complete the delivery of the
//	  message to that Client when it reconnects"
//	[MQTT-4.8.2-5] "If the Client's Session terminates before the Client
//	  reconnects, the Server MUST NOT send the Application Message to any other
//	  subscribed Client"
//	[MQTT-4.3.3-2] the sender "MUST send a PUBLISH packet containing this Packet
//	  Identifier with QoS 2 and DUP flag set to 0" - once per Application
//	  Message: the group's other member must not be sent it as a new one.
//	[MQTT-4.4.0-1] "When a Client reconnects with Clean Start set to 0 and a
//	  session is present, both the Client and Server MUST resend any
//	  unacknowledged PUBLISH packets (where QoS > 0) and PUBREL packets using
//	  their original Packet Identifiers."
//
// What these do not cover, a client that stays away past the AckWait after its
// broker was killed, is on ROADMAP.md.

// killedSharedQoS2 is a member granted QoS 2 on a broker that will be killed,
// a survivor broker, and the group's other member on the survivor.
type killedSharedQoS2 struct {
	victim   *natsmqtt5.Broker
	survivor string
	member   *rawClient
	other    *rawClient
}

func newKilledSharedQoS2(t *testing.T, id string) *killedSharedQoS2 {
	t.Helper()
	natsmqtt5.SetSharedAckWait(t, handBackAckWait)
	natsURL := startNATS(t)
	k := &killedSharedQoS2{}
	var victimAddr string
	k.victim, victimAddr, _ = startBrokerHandle(t, natsURL, persistentWithQueue, sharedQoS2)
	k.survivor = startBroker(t, natsURL, persistentWithQueue, sharedQoS2)
	t.Cleanup(func() { natsmqtt5.Reap(k.victim) })

	k.member = dialRaw(t, victimAddr)
	k.member.connect(noKeepAlive(rawConnect(id, 300)))
	require.Equal(t, []packet.ReasonCode{packet.GrantedQoS2}, k.member.subscribeCodes(group, packet.QoS2))
	return k
}

// joinOther makes the group's other member, once the message is the member's.
func (k *killedSharedQoS2) joinOther(t *testing.T) {
	t.Helper()
	k.other = dialRaw(t, k.survivor)
	k.other.connect(noKeepAlive(rawConnect(t.Name()+"-other", 300)))
	k.other.subscribe(group, packet.QoS1)
}

// resume connects the member back on the survivor and returns what it is sent
// first.
func (k *killedSharedQoS2) resume(t *testing.T, id string) (*rawClient, *packet.Publish) {
	t.Helper()
	back := dialRaw(t, k.survivor)
	require.True(t, back.connect(noKeepAlive(rawConnect(id, 300))).SessionPresent)
	return back, back.expectPublish()
}

// The broker is killed the moment the client has the PUBLISH, with no periodic
// checkpoint in between: the record naming the message was written before the
// PUBLISH went out, so the survivor resends it to that client and the group's
// other member is never given it.
func TestAKilledBrokersSharedQoS2MessageJustSentIsCompletedByTheSameClient_MQTT_4_8_2_4_MQTT_4_8_2_5(t *testing.T) {
	k := newKilledSharedQoS2(t, "sq2-sent")
	sharedFleet{a: k.survivor}.publishQoS2(t, "job-sent")
	sent := k.member.expectPublish()
	require.Equal(t, packet.QoS2, sent.QoS)
	natsmqtt5.Kill(k.victim)
	k.joinOther(t)

	back, again := k.resume(t, "sq2-sent")
	assert.True(t, again.Dup, "a resend")
	assert.Equal(t, sent.PacketID, again.PacketID, "[MQTT-4.4.0-1] with the original Packet Identifier")
	assert.Equal(t, "job-sent", string(again.Payload))

	k.other.expectNoPublishFor(4 * handBackAckWait)
	back.completeQoS2(again)
	k.other.expectNoPublishFor(2 * handBackAckWait)
	back.expectNoPublishFor(time.Second)
}

// The broker reads the client's PUBREC and is killed before any record says the
// PUBREL is owed. The record still names the PUBLISH, so the survivor resends it
// with DUP; the client answers PUBREC again, as [MQTT-4.3.3-10] has it do for a
// PUBLISH it already accepted, and the exchange completes. The group's other
// member is never given the message.
func TestAKilledBrokersSharedQoS2MessageAnsweredBeforeItsRecordIsCompletedByTheSameClient_MQTT_4_8_2_4_MQTT_4_8_2_5(t *testing.T) {
	k := newKilledSharedQoS2(t, "sq2-rec")
	entered := make(chan uint16, 1)
	release := make(chan struct{})
	natsmqtt5.SetPubrecGate(k.victim, func(id uint16) {
		entered <- id
		<-release
	})
	t.Cleanup(func() { close(release) })
	sharedFleet{a: k.survivor}.publishQoS2(t, "job-rec")
	sent := k.member.expectPublish()
	require.Equal(t, packet.QoS2, sent.QoS)
	k.member.send(&packet.Pubrec{Ack: packet.Ack{PacketID: sent.PacketID}})
	select {
	case id := <-entered:
		require.Equal(t, sent.PacketID, id)
	case <-time.After(5 * time.Second):
		t.Fatal("the PUBREC never reached the broker")
	}
	natsmqtt5.Kill(k.victim)
	k.joinOther(t)

	back, again := k.resume(t, "sq2-rec")
	assert.True(t, again.Dup, "the PUBLISH again, as the record named it")
	assert.Equal(t, sent.PacketID, again.PacketID, "[MQTT-4.4.0-1] with the original Packet Identifier")

	k.other.expectNoPublishFor(4 * handBackAckWait)
	back.completeQoS2(again)
	k.other.expectNoPublishFor(2 * handBackAckWait)
	back.expectNoPublishFor(time.Second)
}

// The window the two tests above leave: the client stays away for much longer than the group's
// AckWait after its broker was killed. Nobody is saying the message is still being worked on, so
// JetStream would give it to the group's other member, and the returning client would be sent it
// again [MQTT-4.8.2-4], [MQTT-4.8.2-5]. The surviving broker holds the messages that the dead
// broker's record names (Broker.holdOrphans), without restoring the session, so the other member is
// given nothing however long the client is away, and the client still finds the message waiting
// for it.
func TestAKilledBrokersSharedQoS2MessageIsHeldForAClientAwayPastTheAckWait_MQTT_4_8_2_4_MQTT_4_8_2_5(t *testing.T) {
	k := newKilledSharedQoS2(t, "sq2-away")
	sharedFleet{a: k.survivor}.publishQoS2(t, "job-away")
	sent := k.member.expectPublish()
	require.Equal(t, packet.QoS2, sent.QoS)
	natsmqtt5.Kill(k.victim)
	k.joinOther(t)

	k.other.expectNoPublishFor(10 * handBackAckWait)

	back, again := k.resume(t, "sq2-away")
	assert.True(t, again.Dup, "a resend")
	assert.Equal(t, sent.PacketID, again.PacketID, "[MQTT-4.4.0-1] with the original Packet Identifier")
	assert.Equal(t, "job-away", string(again.Payload))
	back.completeQoS2(again)
	k.other.expectNoPublishFor(2 * handBackAckWait)
	back.expectNoPublishFor(time.Second)
}
