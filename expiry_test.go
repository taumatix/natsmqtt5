package natsmqtt5_test

import (
	"context"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// Message Expiry Interval, MQTT-5.0 §3.3.2.3.3, read from the specification
// (not from memory):
//
//	[MQTT-3.3.2-5] "If the Message Expiry Interval has passed and the Server has
//	not managed to start onward delivery to a matching subscriber, then it MUST
//	delete the copy of the message for that subscriber."
//	[MQTT-3.3.2-6] "The PUBLISH packet sent to a Client by the Server MUST
//	contain a Message Expiry Interval set to the received value minus the time
//	that the message has been waiting in the Server."
//	[MQTT-4.3.3-7] "In the QoS 2 delivery protocol, the sender MUST NOT apply
//	Application Message expiry if a PUBLISH packet has been sent."
//
// The broker counts the time waited in whole seconds, rounded down, so every
// literal below is the interval minus the whole seconds that passed: a message
// that waited 1.3 s of a 2 s interval goes out with 1. It has expired once it
// has waited its whole interval, and never with the interval at 0 unless the
// publisher sent 0 and it has waited under a second. Each test says in a
// comment what it waited.

const waited = 1300 * time.Millisecond

func ptrU32(v uint32) *uint32 { return &v }

// publishExpiring publishes with a Message Expiry Interval, or none when
// expiry is nil, and returns once the broker has acknowledged it (QoS 1).
func publishExpiring(c *testClient, topic, payload string, expiry *uint32, retain bool) {
	c.t.Helper()
	p := &paho.Publish{Topic: topic, QoS: 1, Payload: []byte(payload), Retain: retain}
	if expiry != nil {
		p.Properties = &paho.PublishProperties{MessageExpiry: expiry}
	}
	c.publish(p)
}

// expiryOf is the Message Expiry Interval of a PUBLISH the broker sent, or -1
// when the packet carries none.
func expiryOf(p *packet.Publish) int64 {
	if p.Properties == nil || p.Properties.MessageExpiryInterval == nil {
		return -1
	}
	return int64(*p.Properties.MessageExpiryInterval)
}

// expectDelivered reads one PUBLISH, acknowledges it, and checks its payload
// and the interval it carries (-1 for none).
func (c *rawClient) expectDelivered(payload string, wantExpiry int64) *packet.Publish {
	c.t.Helper()
	p := c.expectPublish()
	require.Equal(c.t, payload, string(p.Payload))
	assert.Equal(c.t, wantExpiry, expiryOf(p), "Message Expiry Interval of %q", payload)
	if p.QoS > packet.QoS0 {
		c.send(&packet.Puback{Ack: packet.Ack{PacketID: p.PacketID}})
	}
	return p
}

// A subscriber that is online gets the message at once with the interval it
// was sent with, 0 and absent included: neither is "already expired".
func TestExpiryLiveDeliveryKeepsTheIntervalAndTreatsZeroAndAbsentAsLive(t *testing.T) {
	addr := startBroker(t, startNATS(t))
	sub := dialRaw(t, addr)
	sub.connect(rawConnect("exp-live-sub", 0))
	sub.subscribe("exp/live", packet.QoS1)
	pub, _ := connectClient(t, addr, connectOpts("exp-live-pub"))

	publishExpiring(pub, "exp/live", "ten", ptrU32(10), false)
	publishExpiring(pub, "exp/live", "zero", ptrU32(0), false)
	publishExpiring(pub, "exp/live", "none", nil, false)

	sub.expectDelivered("ten", 10)
	sub.expectDelivered("zero", 0)
	sub.expectDelivered("none", -1)
}

// [MQTT-3.3.2-5] [MQTT-3.3.2-6] on the live path. Receive Maximum 1 and an
// unacknowledged first message make the broker hold the next ones, which is
// how a message waits in the server with its subscriber online.
func TestExpiryLivePathDecrementsAndDeletesWhileWaitingForSendQuota(t *testing.T) {
	addr := startBroker(t, startNATS(t))
	sub := dialRaw(t, addr)
	cp := rawConnect("exp-quota-sub", 0)
	cp.Properties.ReceiveMaximum = packet.Uint16(1)
	sub.connect(cp)
	sub.subscribe("exp/quota/#", packet.QoS1)
	pub, _ := connectClient(t, addr, connectOpts("exp-quota-pub"))

	publishExpiring(pub, "exp/quota/a", "head", nil, false)
	head := sub.expectPublish()
	require.Equal(t, "head", string(head.Payload))
	// Published while "head" holds the only slot; each waits behind it.
	publishExpiring(pub, "exp/quota/b", "two-of-three", ptrU32(3), false)
	publishExpiring(pub, "exp/quota/c", "one-of-one", ptrU32(1), false)
	publishExpiring(pub, "exp/quota/d", "no-expiry", nil, false)

	time.Sleep(waited) // 1 s passed: 3 -> 2, and the 1 s message has expired
	sub.send(&packet.Puback{Ack: packet.Ack{PacketID: head.PacketID}})

	sub.expectDelivered("two-of-three", 2)
	sub.expectDelivered("no-expiry", -1)
	sub.expectNothing()
}

// [MQTT-3.3.2-5] [MQTT-3.3.2-6] on the offline-queue replay, and a session that
// resumes after dropping expired messages is not left with a hole: the next
// message is delivered and acknowledged normally.
func TestExpiryOfflineQueueReplayDecrementsAndDeletes(t *testing.T) {
	addr := startBroker(t, startNATS(t))
	away := dialRaw(t, addr)
	away.connect(rawConnect("exp-off", 300))
	away.subscribe("exp/off/#", packet.QoS1)
	away.drop()

	pub, _ := connectClient(t, addr, connectOpts("exp-off-pub"))
	publishExpiring(pub, "exp/off/a", "two-of-two", ptrU32(2), false)
	publishExpiring(pub, "exp/off/b", "one-of-one", ptrU32(1), false)
	publishExpiring(pub, "exp/off/c", "sixty", ptrU32(60), false)
	publishExpiring(pub, "exp/off/d", "no-expiry", nil, false)
	time.Sleep(waited) // away 1 s: 2 -> 1, 60 -> 59, the 1 s message is gone

	back := dialRaw(t, addr)
	require.True(t, back.connect(rawConnect("exp-off", 300)).SessionPresent)
	first := back.expectDelivered("two-of-two", 1)
	back.expectDelivered("sixty", 59)
	last := back.expectDelivered("no-expiry", -1)
	back.expectNothing()
	assert.NotEqual(t, first.PacketID, last.PacketID)

	publishExpiring(pub, "exp/off/e", "after", ptrU32(30), false)
	back.expectDelivered("after", 30)
}

// A message that expires while the session is away and the session is then
// restored on another broker (PersistentSessions) is deleted there too.
func TestExpiryReplayOnARestoredSessionHonoursTheQueueTimestamp(t *testing.T) {
	natsURL := startNATS(t)
	addr, stop := startStoppableBroker(t, natsURL, persistentWithQueue)
	away := dialRaw(t, addr)
	away.connect(rawConnect("exp-restore", 300))
	away.subscribe("exp/restore/#", packet.QoS1)
	away.drop()
	pub, _ := connectClient(t, addr, connectOpts("exp-restore-pub"))
	publishExpiring(pub, "exp/restore/a", "sixty-of-sixty", ptrU32(60), false)
	publishExpiring(pub, "exp/restore/b", "one-of-one", ptrU32(1), false)
	time.Sleep(200 * time.Millisecond)
	stop()

	time.Sleep(waited) // the whole outage counts: the stream stamped them
	addr2 := startBroker(t, natsURL, persistentWithQueue)
	back := dialRaw(t, addr2)
	require.True(t, back.connect(rawConnect("exp-restore", 300)).SessionPresent)
	// Stamped about 1.5 s ago, so a 60 s interval has about 58 s left. The
	// interval is long so that a stalled runner cannot expire it, and the
	// assertion is a range for the same reason: it says the outage was counted
	// (below 60) and not how long the runner took. The 1 s message above has
	// waited more than its interval however slow the runner was.
	p := back.expectPublish()
	require.Equal(t, "sixty-of-sixty", string(p.Payload))
	back.send(&packet.Puback{Ack: packet.Ack{PacketID: p.PacketID}})
	assert.GreaterOrEqual(t, expiryOf(p), int64(30))
	assert.LessOrEqual(t, expiryOf(p), int64(59), "the time the stream stamped counts, outage included")
	back.expectNothing()
}

// [MQTT-3.3.2-5] [MQTT-3.3.2-6] on the catch-up path: a client that cannot
// keep up is read back from the queue stream, and the stream's timestamp is
// what counts there.
func TestExpiryCatchUpDecrementsAndDeletes(t *testing.T) {
	var logs syncBuffer
	addr := startBroker(t, startNATS(t), loggingBroker(t, &logs))
	slow := slowSubscriber(t, addr, "exp-catch", "exp/catch/#")

	pub, _ := connectClient(t, addr, connectOpts("exp-catch-pub"))
	const total = 3000
	for i, m := range sequence(0, total) {
		switch i {
		case 2900:
			publishExpiring(pub, "exp/catch/x", "short-lived", ptrU32(1), false)
		case 2950:
			publishExpiring(pub, "exp/catch/x", "long-lived", ptrU32(600), false)
		default:
			publishExpiring(pub, "exp/catch/x", m, nil, false)
		}
	}
	time.Sleep(waited)

	require.Contains(t, logs.String(), "catching up from the offline queue",
		"the test must drive the catch-up path")
	delivered := 0
	for delivered < total-1 {
		p := slow.expectPublish()
		slow.send(&packet.Puback{Ack: packet.Ack{PacketID: p.PacketID}})
		delivered++
		require.NotEqual(t, "short-lived", string(p.Payload), "expired, must be deleted [MQTT-3.3.2-5]")
		if string(p.Payload) == "long-lived" {
			// Waited 1.3 s plus the rest of the publishing: whole seconds.
			assert.GreaterOrEqual(t, expiryOf(p), int64(590))
			assert.LessOrEqual(t, expiryOf(p), int64(599))
		} else {
			assert.Equal(t, int64(-1), expiryOf(p))
		}
	}
	slow.expectNothing()
}

// [MQTT-3.3.2-5] [MQTT-3.3.2-6] on the shared-subscription backlog. The
// expired message is acknowledged to JetStream and not redelivered, and the
// member goes on taking work.
func TestExpirySharedBacklogDecrementsAndDeletes(t *testing.T) {
	var logs syncBuffer
	addr := startBroker(t, startNATS(t), loggingBroker(t, &logs))
	w := dialRaw(t, addr)
	w.connect(rawConnect("exp-shared", 300))
	w.subscribe("$share/g/exp/shared/#", packet.QoS1)
	w.drop()
	waitDetached(t, &logs, "exp-shared")

	pub, _ := connectClient(t, addr, connectOpts("exp-shared-pub"))
	publishExpiring(pub, "exp/shared/a", "short-lived", ptrU32(1), false)
	publishExpiring(pub, "exp/shared/b", "ten", ptrU32(10), false)
	publishExpiring(pub, "exp/shared/c", "no-expiry", nil, false)
	time.Sleep(waited) // 1 s: the 1 s message is gone, 10 -> 9

	back := dialRaw(t, addr)
	require.True(t, back.connect(rawConnect("exp-shared", 300)).SessionPresent)
	back.expectDelivered("ten", 9)
	back.expectDelivered("no-expiry", -1)
	back.expectNothing()

	publishExpiring(pub, "exp/shared/d", "next", ptrU32(30), false)
	back.expectDelivered("next", 30)
}

// [MQTT-3.3.2-5] [MQTT-3.3.2-6] for retained messages: the time counts from
// when the message was stored, and a subscriber arriving later gets what is
// left. An expired one is deleted: not sent, and gone from the stream.
func TestExpiryRetainedMessagesCountFromStorage(t *testing.T) {
	natsURL := startNATS(t)
	addr := startBroker(t, natsURL)
	pub, _ := connectClient(t, addr, connectOpts("exp-ret-pub"))
	publishExpiring(pub, "exp/ret/two", "two-of-two", ptrU32(2), true)
	publishExpiring(pub, "exp/ret/one", "one-of-one", ptrU32(1), true)
	publishExpiring(pub, "exp/ret/none", "no-expiry", nil, true)
	publishExpiring(pub, "exp/ret/zero", "zero", ptrU32(0), true)
	time.Sleep(waited)

	sub := dialRaw(t, addr)
	sub.connect(rawConnect("exp-ret-sub", 0))
	sub.subscribe("exp/ret/#", packet.QoS1)

	got := map[string]int64{}
	for i := 0; i < 2; i++ {
		p := sub.expectPublish()
		assert.True(t, p.Retain, "a retained message is sent with RETAIN 1")
		got[string(p.Payload)] = expiryOf(p)
		sub.send(&packet.Puback{Ack: packet.Ack{PacketID: p.PacketID}})
	}
	// "zero" had 0 and has waited over a second: expired, as is "one-of-one".
	assert.Equal(t, map[string]int64{"two-of-two": 1, "no-expiry": -1}, got)
	sub.expectNothing()

	js := jetStreamOf(t, natsURL)
	require.Eventually(t, func() bool {
		s, err := js.Stream(context.Background(), natsmqtt5.DefaultStreamPrefix+"_retained")
		if err != nil {
			return false
		}
		info, err := s.Info(context.Background())
		return err == nil && info.State.Msgs == 2
	}, 5*time.Second, 20*time.Millisecond, "expired retained messages must leave the stream")
}

// A retained message with an interval of 0 or none is not expired by a new
// subscriber arriving moments later.
func TestExpiryRetainedZeroIsDeliveredToAPromptSubscriber(t *testing.T) {
	addr := startBroker(t, startNATS(t))
	pub, _ := connectClient(t, addr, connectOpts("exp-ret0-pub"))
	publishExpiring(pub, "exp/ret0/zero", "zero", ptrU32(0), true)

	sub := dialRaw(t, addr)
	sub.connect(rawConnect("exp-ret0-sub", 0))
	sub.subscribe("exp/ret0/#", packet.QoS1)
	sub.expectDelivered("zero", 0)
}

// Retained messages wait in the stream while no broker is running, and the
// stream's own timestamp is the start of the wait.
func TestExpiryRetainedMessagesCountAcrossABrokerRestart(t *testing.T) {
	natsURL := startNATS(t)
	addr, stop := startStoppableBroker(t, natsURL)
	pub, _ := connectClient(t, addr, connectOpts("exp-ret-restart-pub"))
	publishExpiring(pub, "exp/restart/a", "three-of-three", ptrU32(3), true)
	publishExpiring(pub, "exp/restart/b", "one-of-one", ptrU32(1), true)
	stop()
	time.Sleep(waited)

	addr2 := startBroker(t, natsURL)
	sub := dialRaw(t, addr2)
	sub.connect(rawConnect("exp-ret-restart-sub", 0))
	sub.subscribe("exp/restart/#", packet.QoS1)
	sub.expectDelivered("three-of-three", 2)
	sub.expectNothing()
}

// [MQTT-3.3.2-6] on a resend after a resumption: the original identifier, DUP,
// and the interval less the time since the message was first taken in. A QoS 1
// message that expired while in flight is deleted instead of resent
// [MQTT-3.3.2-5], and its identifier and quota slot go with it.
func TestExpiryResendCarriesTheRemainingIntervalAndDropsAnExpiredQoS1(t *testing.T) {
	addr := startBroker(t, startNATS(t))
	c := dialRaw(t, addr)
	c.connect(rawConnect("exp-resend", 300))
	c.subscribe("exp/resend/#", packet.QoS1)

	pub, _ := connectClient(t, addr, connectOpts("exp-resend-pub"))
	publishExpiring(pub, "exp/resend/a", "three-of-three", ptrU32(3), false)
	publishExpiring(pub, "exp/resend/b", "one-of-one", ptrU32(1), false)
	a, b := c.expectPublish(), c.expectPublish()
	assert.Equal(t, int64(3), expiryOf(a))
	assert.Equal(t, int64(1), expiryOf(b))
	c.drop() // neither acknowledged

	time.Sleep(waited)
	back := dialRaw(t, addr)
	require.True(t, back.connect(rawConnect("exp-resend", 300)).SessionPresent)
	again := back.expectPublish()
	assert.Equal(t, a.PacketID, again.PacketID, "[MQTT-4.4.0-1] the original Packet Identifier")
	assert.True(t, again.Dup)
	assert.Equal(t, int64(2), expiryOf(again), "3 s minus the 1 s it waited [MQTT-3.3.2-6]")
	back.send(&packet.Puback{Ack: packet.Ack{PacketID: again.PacketID}})
	back.expectNothing() // the 1 s one expired in flight: deleted, not resent

	// Neither the identifier nor the quota slot of the dropped one is stuck.
	publishExpiring(pub, "exp/resend/c", "after", ptrU32(30), false)
	back.expectDelivered("after", 30)
}

// [MQTT-4.3.3-7]: once a QoS 2 PUBLISH has been sent, expiry no longer applies
// to it. The resend goes out even though its interval has passed, with the
// smallest value the field can hold, and the exchange completes.
func TestExpiryDoesNotDropAQoS2PublishThatWasAlreadySent(t *testing.T) {
	addr := startBroker(t, startNATS(t))
	c := dialRaw(t, addr)
	c.connect(rawConnect("exp-q2", 300))
	c.subscribe("exp/q2", packet.QoS2)

	pub, _ := connectClient(t, addr, connectOpts("exp-q2-pub"))
	pub.publish(&paho.Publish{Topic: "exp/q2", QoS: 2, Payload: []byte("sent"),
		Properties: &paho.PublishProperties{MessageExpiry: ptrU32(1)}})
	first := c.expectPublish()
	assert.Equal(t, int64(1), expiryOf(first))
	c.drop()

	time.Sleep(waited)
	back := dialRaw(t, addr)
	require.True(t, back.connect(rawConnect("exp-q2", 300)).SessionPresent)
	again := back.expectPublish()
	assert.Equal(t, first.PacketID, again.PacketID)
	assert.True(t, again.Dup)
	assert.Equal(t, int64(0), expiryOf(again))
	back.send(&packet.Pubrec{Ack: packet.Ack{PacketID: again.PacketID}})
	rel := back.expectPubrel()
	assert.Equal(t, first.PacketID, rel.PacketID)
	back.send(&packet.Pubcomp{Ack: packet.Ack{PacketID: rel.PacketID}})
	back.expectNothing()
}

func jetStreamOf(t *testing.T, natsURL string) jetstream.JetStream {
	t.Helper()
	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	return js
}
