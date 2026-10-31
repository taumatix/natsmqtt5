package natsmqtt5_test

import (
	"os"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// QoS 2 on a shared subscription, first piece: a QoS 2 message a shared member
// holds is the session's in-flight state, so it survives the member's broker
// being killed and a takeover of the session, and the group is not given it
// while the member still has it. SUBACK grants QoS 1 unless Options.SharedQoS2
// is set.
//
// Statement wording, read from the OASIS MQTT 5.0 specification (os edition):
//
//	[MQTT-3.8.4-7] "The Server MAY grant a lower Maximum QoS than the
//	  subscriber requested" (the Reason Code of the SUBACK says which).
//	[MQTT-4.4.0-1] "When a Client reconnects with Clean Start set to 0 and a
//	  session is present, both the Client and Server MUST resend any
//	  unacknowledged PUBLISH packets (where QoS > 0) and PUBREL packets using
//	  their original Packet Identifiers."
//	[MQTT-4.3.3-2] "In the QoS 2 delivery protocol, the Sender MUST send a
//	  PUBLISH packet containing a QoS 2 ... and MUST NOT re-send it with a
//	  different Packet Identifier"; the message is delivered to one member once.
func sharedQoS2(o *natsmqtt5.Options) { o.SharedQoS2 = true }

func (f sharedFleet) publishQoS2(t *testing.T, payload string) {
	t.Helper()
	pub, _ := connectClient(t, f.a, connectOpts("producer-q2"))
	pub.publish(&paho.Publish{Topic: "jobs/x", QoS: 2, Payload: []byte(payload)})
}

func (c *rawClient) completeQoS2(p *packet.Publish) {
	c.t.Helper()
	c.send(&packet.Pubrec{Ack: packet.Ack{PacketID: p.PacketID}})
	require.Equal(c.t, p.PacketID, c.expectPubrel().PacketID)
	c.send(&packet.Pubcomp{Ack: packet.Ack{PacketID: p.PacketID}})
}

// [MQTT-3.8.4-7]: a QoS 2 request on a shared filter is granted QoS 1 unless
// the broker was told otherwise, and the SUBACK says which.
func TestASharedSubscriptionIsGrantedQoS1UnlessSharedQoS2IsOn_MQTT_3_8_4_7(t *testing.T) {
	for name, tc := range map[string]struct {
		opts []func(*natsmqtt5.Options)
		want packet.ReasonCode
	}{
		"off": {nil, packet.GrantedQoS1},
		"on":  {[]func(*natsmqtt5.Options){sharedQoS2}, packet.GrantedQoS2},
	} {
		t.Run(name, func(t *testing.T) {
			addr := startBroker(t, startNATS(t), append([]func(*natsmqtt5.Options){persistentWithQueue}, tc.opts...)...)
			c := dialRaw(t, addr)
			c.connect(rawConnect("asks-for-2", 300))
			assert.Equal(t, []packet.ReasonCode{tc.want}, c.subscribeCodes(group, packet.QoS2))
		})
	}
}

// A QoS 2 message sent to a shared member and not yet answered is resent, with
// its original Packet Identifier, to the session when it is taken over on
// another broker [MQTT-4.4.0-1], and the group's other member is not given it
// in the meantime, however many times JetStream's AckWait passes.
func TestATakenOverSessionKeepsItsHeldSharedQoS2Message_MQTT_4_4_0_1(t *testing.T) {
	f := newSharedFleet(t, persistentWithQueue, sharedQoS2)

	held := dialRaw(t, f.a)
	held.connect(rawConnect("holder", 300))
	require.Equal(t, []packet.ReasonCode{packet.GrantedQoS2}, held.subscribeCodes(group, packet.QoS2))
	f.publishQoS2(t, "job-q2")
	sent := held.expectPublish()
	require.Equal(t, packet.QoS2, sent.QoS)
	require.False(t, sent.Dup)

	other := dialRaw(t, f.b)
	other.connect(rawConnect("other", 300))
	other.subscribe(group, packet.QoS1)

	taker := dialRaw(t, f.b)
	require.True(t, taker.connect(rawConnect("holder", 300)).SessionPresent)
	again := taker.expectPublish()
	assert.True(t, again.Dup, "resent with DUP set")
	assert.Equal(t, sent.PacketID, again.PacketID, "[MQTT-4.4.0-1] the original Packet Identifier")
	assert.Equal(t, "job-q2", string(again.Payload))

	other.expectNoPublishFor(4 * handBackAckWait)
	taker.completeQoS2(again)
	other.expectNoPublishFor(2 * handBackAckWait)
	taker.expectNoPublishFor(time.Second)
}

// The member's broker is killed (SIGKILL) while its QoS 2 message is unanswered,
// and the session is restored on another broker before the group's AckWait (30
// seconds in the broker binary) passes. The restored session holds the message
// again, so the group's other member is never given it, and the exchange
// completes on the original Packet Identifier [MQTT-4.4.0-1]. Gated like the
// QoS 1 case in shared_killed_test.go because it waits out the real AckWait.
func TestAKilledBrokersHeldSharedQoS2MessageIsHeldAgainByTheRestoredSession_MQTT_4_4_0_1(t *testing.T) {
	t.Parallel()
	if os.Getenv("NATSMQTT5_SLOW_TESTS") == "" {
		t.Skip("waits out the 30 s AckWait of the broker binary; set NATSMQTT5_SLOW_TESTS=1")
	}
	natsURL := startNATS(t)
	child := startBrokerBinary(t, natsURL, "-persistent-sessions", "-shared-qos2",
		"-session-checkpoint-interval", killedBrokerCheckpoint)
	survivor := startBroker(t, natsURL, persistentWithQueue, sharedQoS2)

	victim := dialRaw(t, child.addr)
	victim.connect(noKeepAlive(rawConnect("killed-q2", 300)))
	require.Equal(t, []packet.ReasonCode{packet.GrantedQoS2}, victim.subscribeCodes(group, packet.QoS2))
	sharedFleet{a: survivor, b: survivor}.publishQoS2(t, "job-k2")
	sent := victim.expectPublish()
	require.Equal(t, packet.QoS2, sent.QoS)
	other := dialRaw(t, survivor)
	other.connect(noKeepAlive(rawConnect("other-q2", 300)))
	other.subscribe(group, packet.QoS1)

	awaitCheckpointedInflight(t, natsURL, "killed-q2", sent.PacketID, false)
	child.kill9(t)

	back := dialRaw(t, survivor)
	require.True(t, back.connect(noKeepAlive(rawConnect("killed-q2", 300))).SessionPresent)
	again := back.expectPublish()
	assert.True(t, again.Dup, "the restored session resends it with DUP set")
	assert.Equal(t, sent.PacketID, again.PacketID, "[MQTT-4.4.0-1] the original Packet Identifier")
	assert.Equal(t, "job-k2", string(again.Payload))

	other.expectNoPublishFor(killedAckWait + 5*time.Second)
	back.completeQoS2(again)
	other.expectNoPublishFor(2 * time.Second)
	back.expectNoPublishFor(2 * time.Second)
}
