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
//
// [MQTT-3.8.4-7]'s wording above is a paraphrase, not a quotation: the
// specification text was not reachable when this was written, so the ids are
// what to check against the specification.
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

// Second piece: the ownership point. Past its PUBREC the message is the client's
// and the group is told so, whatever happens to the session or the broker next.
//
//	[MQTT-4.3.3-8] (paraphrase, as elsewhere in this repo's comments): once the
//	  client's PUBREC is in, the client owns the message and the sender owes a
//	  PUBREL, not the PUBLISH; [MQTT-4.4.0-1] makes the PUBREL repeatable.
//	§4.8.2 "If the Client's Session terminates before the Client reconnects, the
//	  Server SHOULD send the Application Message to another Client that is
//	  subscribed to the same Shared Subscription" - for a message the Client has
//	  not yet taken; one it has taken is not sent again.

// qos2Holder connects a member that was granted QoS 2, is sent one job and has
// answered it with PUBREC and been sent the PUBREL, which it leaves unanswered.
func qos2Holder(t *testing.T, f sharedFleet, id string, expiry uint32) (*rawClient, *packet.Publish) {
	t.Helper()
	m := dialRaw(t, f.a)
	m.connect(rawConnect(id, expiry))
	require.Equal(t, []packet.ReasonCode{packet.GrantedQoS2}, m.subscribeCodes(group, packet.QoS2))
	f.publishQoS2(t, "job-own")
	p := m.expectPublish()
	require.Equal(t, packet.QoS2, p.QoS)
	m.send(&packet.Pubrec{Ack: packet.Ack{PacketID: p.PacketID}})
	require.Equal(t, p.PacketID, m.expectPubrel().PacketID)
	return m, p
}

// Past PUBREC, a message is not given to another member when the member
// unsubscribes [MQTT-4.3.3-8].
func TestAQoS2MessagePastPubrecIsNotHandedBackWhenTheMemberUnsubscribes_MQTT_4_3_3_8(t *testing.T) {
	f := newSharedFleet(t, persistentWithQueue, sharedQoS2)
	m1, _ := qos2Holder(t, f, "m1", 300)
	m2 := member(t, f.b, "m2", 300)
	m1.unsubscribe(group)
	m2.expectNoPublishFor(4 * handBackAckWait)
}

// Past PUBREC, a message is not given to another member when the member's
// session ends with a Clean Start, nor after it has gone on unanswered for longer
// than AckWait [MQTT-4.3.3-8].
func TestAQoS2MessagePastPubrecIsNotHandedBackWhenTheSessionEnds_MQTT_4_3_3_8(t *testing.T) {
	f := newSharedFleet(t, persistentWithQueue, sharedQoS2)
	qos2Holder(t, f, "m1", 300)
	m2 := member(t, f.b, "m2", 300)
	m2.expectNoPublishFor(3 * handBackAckWait)

	fresh := dialRaw(t, f.a)
	cp := rawConnect("m1", 300)
	cp.CleanStart = true
	require.False(t, fresh.connect(cp).SessionPresent)
	m2.expectNoPublishFor(4 * handBackAckWait)
}

// Past PUBREC, a message is not given to another member when the session
// expires with the PUBCOMP still owed [MQTT-4.3.3-8].
func TestAQoS2MessagePastPubrecIsNotHandedBackWhenTheSessionExpires_MQTT_4_3_3_8(t *testing.T) {
	f := newSharedFleet(t, persistentWithQueue, sharedQoS2)
	m1, _ := qos2Holder(t, f, "m1", handBackSessionLife)
	m2 := member(t, f.b, "m2", 300)
	m1.drop()
	m2.expectNoPublishFor(handBackSessionLife*time.Second + 4*handBackAckWait)
}

// Past PUBREC and disconnected, the client that returns is owed the PUBREL and
// only that; the exchange completes and the group never sees the message
// [MQTT-4.4.0-1].
func TestAReturningMemberIsResentThePubrelOfAMessagePastPubrec_MQTT_4_4_0_1(t *testing.T) {
	f := newSharedFleet(t, persistentWithQueue, sharedQoS2)
	m1, p := qos2Holder(t, f, "m1", 300)
	m2 := member(t, f.b, "m2", 300)
	m1.drop()
	m2.expectNoPublishFor(3 * handBackAckWait)

	back := dialRaw(t, f.b)
	require.True(t, back.connect(rawConnect("m1", 300)).SessionPresent)
	assert.Equal(t, p.PacketID, back.expectPubrel().PacketID, "[MQTT-4.4.0-1] the PUBREL, original identifier")
	back.send(&packet.Pubcomp{Ack: packet.Ack{PacketID: p.PacketID}})
	m2.expectNoPublishFor(2 * handBackAckWait)
	back.expectNoPublishFor(time.Second)
}

// An unsubscribe does not take back a message already sent: it is the session's
// in-flight state until the exchange ends, as for QoS 1, and the client may still
// answer it. The exchange completes and the group never sees the message.
func TestAQoS2MessageSentBeforeAnUnsubscribeIsStillCompletedByTheMember_MQTT_4_4_0_1(t *testing.T) {
	f := newSharedFleet(t, persistentWithQueue, sharedQoS2)
	m1 := dialRaw(t, f.a)
	m1.connect(rawConnect("m1", 300))
	require.Equal(t, []packet.ReasonCode{packet.GrantedQoS2}, m1.subscribeCodes(group, packet.QoS2))
	f.publishQoS2(t, "job-early")
	p := m1.expectPublish()
	m2 := member(t, f.b, "m2", 300)
	m1.unsubscribe(group)
	m2.expectNoPublishFor(3 * handBackAckWait)
	m1.completeQoS2(p)
	m2.expectNoPublishFor(2 * handBackAckWait)
}

// Before PUBREC the message is still the group's: the end of the session gives
// it to another member (§4.8.2), as at QoS 1.
func TestAQoS2MessageBeforePubrecIsHandedBackWhenTheSessionEnds_MQTT_4_8_2(t *testing.T) {
	f := newSharedFleet(t, persistentWithQueue, sharedQoS2)
	m1 := dialRaw(t, f.a)
	m1.connect(rawConnect("m1", 0))
	require.Equal(t, []packet.ReasonCode{packet.GrantedQoS2}, m1.subscribeCodes(group, packet.QoS2))
	f.publishQoS2(t, "job-early")
	m1.expectPublish()
	m2 := member(t, f.b, "m2", 300)
	m1.send(&packet.Disconnect{})
	got := m2.expectPublish()
	assert.Equal(t, "job-early", string(got.Payload))
}

// The member's broker is killed (SIGKILL) just past the PUBREC, with the
// PUBCOMP unsent, and its AckWait (30 seconds in the broker binary) passes: the
// group is not given the message, and the session restored elsewhere is owed
// the PUBREL [MQTT-4.4.0-1]. Gated: waits out the real AckWait.
func TestAKilledBrokersQoS2MessagePastPubrecIsNotGivenToTheGroup_MQTT_4_3_3_8(t *testing.T) {
	t.Parallel()
	if os.Getenv("NATSMQTT5_SLOW_TESTS") == "" {
		t.Skip("waits out the 30 s AckWait of the broker binary; set NATSMQTT5_SLOW_TESTS=1")
	}
	natsURL := startNATS(t)
	child := startBrokerBinary(t, natsURL, "-persistent-sessions", "-shared-qos2",
		"-session-checkpoint-interval", killedBrokerCheckpoint)
	survivor := startBroker(t, natsURL, persistentWithQueue, sharedQoS2)

	victim := dialRaw(t, child.addr)
	victim.connect(noKeepAlive(rawConnect("killed-own", 300)))
	require.Equal(t, []packet.ReasonCode{packet.GrantedQoS2}, victim.subscribeCodes(group, packet.QoS2))
	sharedFleet{a: survivor, b: survivor}.publishQoS2(t, "job-own-k")
	sent := victim.expectPublish()
	other := dialRaw(t, survivor)
	other.connect(noKeepAlive(rawConnect("other-own", 300)))
	other.subscribe(group, packet.QoS1)
	victim.send(&packet.Pubrec{Ack: packet.Ack{PacketID: sent.PacketID}})
	require.Equal(t, sent.PacketID, victim.expectPubrel().PacketID)

	awaitCheckpointedInflight(t, natsURL, "killed-own", sent.PacketID, true)
	child.kill9(t)
	other.expectNoPublishFor(killedAckWait + 5*time.Second)

	back := dialRaw(t, survivor)
	require.True(t, back.connect(noKeepAlive(rawConnect("killed-own", 300))).SessionPresent)
	assert.Equal(t, sent.PacketID, back.expectPubrel().PacketID)
	back.send(&packet.Pubcomp{Ack: packet.Ack{PacketID: sent.PacketID}})
	other.expectNoPublishFor(2 * time.Second)
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
