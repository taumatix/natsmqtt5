package natsmqtt5_test

import (
	"testing"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5/packet"
)

// A burst of QoS 2 messages sent to one client shares the session records that
// precede its PUBLISH and PUBREL packets (deliverBurst, queuePubrel); the broker
// killed after the packets reached the client must still be taken over as if each
// message had been recorded alone. Statement wording, from OASIS MQTT 5.0 4.3.3
// and 4.4, as in killed_qos2_outbound_test.go:
//
//	[MQTT-4.3.3-6] "MUST NOT re-send the PUBLISH once it has sent the
//	  corresponding PUBREL packet."
//	[MQTT-4.4.0-1] "... both the Client and Server MUST resend any
//	  unacknowledged PUBLISH packets (where QoS > 0) and PUBREL packets using
//	  their original Packet Identifiers."

const killedBurstSize = 8

// outboundQoS2Burst sends killedBurstSize QoS 2 messages to a client of a broker
// that is to be killed, and returns the PUBLISH packets it read.
func outboundQoS2Burst(t *testing.T, clientID string) (child *killableBroker, survivor string, victim *rawClient, sent []*packet.Publish) {
	t.Helper()
	natsURL := startNATS(t)
	child = startBrokerBinary(t, natsURL, "-persistent-sessions", "-session-checkpoint-interval", "1h")
	survivor = startBroker(t, natsURL, persistentWithQueue)

	victim = dialRaw(t, child.addr)
	require.False(t, victim.connect(rawConnect(clientID, 300)).SessionPresent)
	victim.subscribe("ko/#", packet.QoS2)
	pub, _ := connectClient(t, survivor, connectOpts(clientID+"-pub"))
	for i := 0; i < killedBurstSize; i++ {
		pub.publish(&paho.Publish{Topic: "ko/a", QoS: 2, Payload: []byte{byte('a' + i)}})
	}
	for i := 0; i < killedBurstSize; i++ {
		sent = append(sent, victim.expectPublish())
	}
	return child, survivor, victim, sent
}

// [MQTT-4.4.0-1]: every PUBLISH of a burst the client had been sent and had not
// answered is sent again once, with its own Packet Identifier, and none as new.
func TestAKilledBrokersBurstOfQoS2PublishesIsResentNotRepeated(t *testing.T) {
	child, survivor, _, sent := outboundQoS2Burst(t, "ko-burst-sent")
	child.kill9(t)

	_, got := afterTheKill(t, survivor, "ko-burst-sent")
	pubs := p3Publishes(got)
	require.Len(t, pubs, killedBurstSize, "[MQTT-4.4.0-1] each unacknowledged PUBLISH once, none as a new message")
	want := map[uint16]string{}
	for _, p := range sent {
		want[p.PacketID] = string(p.Payload)
	}
	for _, p := range pubs {
		payload, ok := want[p.PacketID]
		require.True(t, ok, "the original Packet Identifier %d", p.PacketID)
		assert.Equal(t, payload, string(p.Payload))
		assert.True(t, p.Dup, "a resend has DUP set [MQTT-3.3.1-1]")
		delete(want, p.PacketID)
	}
}

// [MQTT-4.3.3-6], [MQTT-4.4.0-1]: a burst whose PUBRELs the broker had sent is
// not published again; each PUBREL is the unacknowledged packet and is resent.
func TestAKilledBrokersBurstOfQoS2MessagesAfterPubrelIsNotPublishedAgain(t *testing.T) {
	child, survivor, victim, sent := outboundQoS2Burst(t, "ko-burst-rel")
	for _, p := range sent {
		victim.send(&packet.Pubrec{Ack: packet.Ack{PacketID: p.PacketID}})
	}
	released := map[uint16]bool{}
	for range sent {
		released[victim.expectPubrel().PacketID] = true
	}
	child.kill9(t)

	_, got := afterTheKill(t, survivor, "ko-burst-rel")
	assert.Empty(t, p3Publishes(got), "[MQTT-4.3.3-6] no PUBLISH once the PUBREL has been sent")
	require.Len(t, got, killedBurstSize, "[MQTT-4.4.0-1] one PUBREL for each message")
	for _, p := range got {
		rel, ok := p.(*packet.Pubrel)
		require.True(t, ok, "only the PUBRELs may be resent, got %s", p.Type())
		assert.True(t, released[rel.PacketID], "the original Packet Identifier %d", rel.PacketID)
		delete(released, rel.PacketID)
	}
}
