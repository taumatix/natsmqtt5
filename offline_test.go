package natsmqtt5_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"log/slog"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

func withOfflineQueue(o *natsmqtt5.Options) { o.OfflineQueue = true }

// A message published while a session is disconnected used to be dropped for
// it: core NATS delivers to whoever is subscribed at the time and keeps
// nothing. With OfflineQueue it is delivered when the session resumes, in
// publish order, before anything published after the resume.
func TestMessagesPublishedWhileASessionIsAwayAreDeliveredOnResume(t *testing.T) {
	addr := startBroker(t, startNATS(t), withOfflineQueue)

	away := dialRaw(t, addr)
	away.connect(rawConnect("offline-1", 300))
	away.subscribe("q/#", packet.QoS1)
	away.drop()

	pub, _ := connectClient(t, addr, connectOpts("pub-offline-1"))
	for _, n := range []string{"1", "2", "3"} {
		pub.publish(&paho.Publish{Topic: "q/" + n, QoS: 1, Payload: []byte(n)})
	}
	pub.publish(&paho.Publish{Topic: "elsewhere", QoS: 1, Payload: []byte("not subscribed")})

	back := dialRaw(t, addr)
	connack := back.connect(rawConnect("offline-1", 300))
	require.True(t, connack.SessionPresent)

	for _, want := range []string{"q/1", "q/2", "q/3"} {
		got := back.expectPublish()
		assert.Equal(t, want, got.Topic)
		assert.Equal(t, packet.QoS1, got.QoS)
		back.send(&packet.Puback{Ack: packet.Ack{PacketID: got.PacketID}})
	}

	pub.publish(&paho.Publish{Topic: "q/live", QoS: 1, Payload: []byte("live")})
	live := back.expectPublish()
	assert.Equal(t, "q/live", live.Topic, "after the queue, live delivery resumes")
	back.send(&packet.Puback{Ack: packet.Ack{PacketID: live.PacketID}})
	back.expectNothing()
}

// QoS 0 is at most once: a session that was away has no claim on it, as in
// every MQTT broker that queues. Nor does a Clean Start, which begins a new
// session with nothing owed.
func TestTheOfflineQueueHoldsNoQoS0AndNothingForACleanStart(t *testing.T) {
	addr := startBroker(t, startNATS(t), withOfflineQueue)

	for _, id := range []string{"offline-qos0", "offline-clean"} {
		c := dialRaw(t, addr)
		c.connect(rawConnect(id, 300))
		c.subscribe("q/#", packet.QoS1)
		c.drop()
	}

	pub, _ := connectClient(t, addr, connectOpts("pub-offline-2"))
	pub.publish(&paho.Publish{Topic: "q/zero", QoS: 0, Payload: []byte("0")})
	// A QoS 0 publish returns before the broker has handled it. A QoS 1 one
	// after it on the same connection does not, and the broker handles a
	// connection's packets in order, so this one returning means q/zero went
	// out while the session was still away.
	pub.publish(&paho.Publish{Topic: "barrier", QoS: 1, Payload: []byte("b")})

	back := dialRaw(t, addr)
	require.True(t, back.connect(rawConnect("offline-qos0", 300)).SessionPresent)
	back.expectNothing()

	pub.publish(&paho.Publish{Topic: "q/one", QoS: 1, Payload: []byte("1")})
	clean := dialRaw(t, addr)
	cp := rawConnect("offline-clean", 300)
	cp.CleanStart = true
	require.False(t, clean.connect(cp).SessionPresent)
	clean.expectNothing()
}

// The queue is on by default whenever the broker uses JetStream, because
// MQTT-5.0 makes it part of the session state: a session with an expiry keeps
// its pending QoS 1 and 2 messages across a disconnect [MQTT-3.1.2-23], and a
// message is added to the session of every matching subscriber, connected or
// not [MQTT-4.5.0-1]. A broker with no options set must not answer Session
// Present 1 and then deliver nothing.
func TestTheOfflineQueueIsOnByDefault(t *testing.T) {
	addr := startBroker(t, startNATS(t))

	away := dialRaw(t, addr)
	away.connect(rawConnect("offline-default", 300))
	away.subscribe("q/#", packet.QoS1)
	away.drop()

	pub, _ := connectClient(t, addr, connectOpts("pub-offline-default"))
	pub.publish(&paho.Publish{Topic: "q/1", QoS: 1, Payload: []byte("1")})

	back := dialRaw(t, addr)
	require.True(t, back.connect(rawConnect("offline-default", 300)).SessionPresent)
	got := back.expectPublish()
	assert.Equal(t, "q/1", got.Topic, "the message published while away [MQTT-3.1.2-23]")
	back.send(&packet.Puback{Ack: packet.Ack{PacketID: got.PacketID}})
	back.expectNothing()
}

// DisableOfflineQueue is the opt-out, for a deployment that would rather not
// store every QoS 1 and 2 message and accepts the deviation: nothing is kept,
// so a resumed session gets only what was already in flight.
func TestWithoutTheOfflineQueueNothingPublishedWhileAwayArrives(t *testing.T) {
	natsURL := startNATS(t)
	var logs syncBuffer
	addr := startBroker(t, natsURL, func(o *natsmqtt5.Options) {
		o.DisableOfflineQueue = true
		o.Logger = slog.New(slog.NewTextHandler(&logs, nil))
	})

	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	_, err = js.Stream(context.Background(), natsmqtt5.DefaultStreamPrefix+"_queue")
	require.ErrorIs(t, err, jetstream.ErrStreamNotFound, "the opt-out must not create the queue's stream")

	away := dialRaw(t, addr)
	away.connect(rawConnect("offline-off", 300))
	away.subscribe("q/#", packet.QoS1)
	away.drop()
	// Until the broker notices the drop, a message still goes to the old
	// connection, is in flight, and is rightly resent on resume
	// [MQTT-4.4.0-1]. Only once the session is detached is it truly away.
	waitDetached(t, &logs, "offline-off")

	pub, _ := connectClient(t, addr, connectOpts("pub-offline-3"))
	pub.publish(&paho.Publish{Topic: "q/1", QoS: 1, Payload: []byte("1")})
	// The PUBACK means NATS has the message, not that it has reached the
	// session's subscription, which drops it silently. Resumed too soon, the
	// session is attached again when it arrives and takes it live, which is
	// not a fault: the opt-out keeps nothing, it does not promise a loss. There
	// is no event to wait on, so give in-process NATS ample time.
	time.Sleep(200 * time.Millisecond)

	back := dialRaw(t, addr)
	require.True(t, back.connect(rawConnect("offline-off", 300)).SessionPresent)
	back.expectNothing()
}

// Publishing never stops while the client drops and resumes, so messages land
// on every side of every boundary: delivered before the drop, published while
// the connection is going down, while it is away, during the resume, and live
// after it. Every one must arrive, and a repeat is only acceptable as a
// retransmission, flagged DUP [MQTT-4.4.0-1]: a message that came both from the
// queue and live would be a second first delivery.
func TestNoMessageIsLostOrDeliveredTwiceAcrossADropAndResume(t *testing.T) {
	addr := startBroker(t, startNATS(t), withOfflineQueue)

	first := dialRaw(t, addr)
	first.connect(rawConnect("offline-stress", 300))
	first.subscribe("s/#", packet.QoS1)

	pub, _ := connectClient(t, addr, connectOpts("pub-offline-stress"))
	const total = 300
	published := make(chan struct{})
	go func() {
		defer close(published)
		for i := 0; i < total; i++ {
			pub.publish(&paho.Publish{Topic: "s/x", QoS: 1, Payload: []byte(fmt.Sprintf("%04d", i))})
			time.Sleep(time.Millisecond)
		}
	}()

	firstDeliveries := map[string]int{}
	// Read for a while on the first connection, acknowledging, then drop it
	// mid-stream.
	deadline := time.Now().Add(80 * time.Millisecond)
	for time.Now().Before(deadline) {
		require.NoError(t, first.nc.SetReadDeadline(time.Now().Add(100*time.Millisecond)))
		p, err := packet.Read(first.r, 0)
		if err != nil {
			break
		}
		if pp, ok := p.(*packet.Publish); ok {
			firstDeliveries[string(pp.Payload)]++
			first.send(&packet.Puback{Ack: packet.Ack{PacketID: pp.PacketID}})
		}
	}
	first.drop()
	time.Sleep(50 * time.Millisecond)

	back := dialRaw(t, addr)
	require.True(t, back.connect(rawConnect("offline-stress", 300)).SessionPresent)
	<-published

	// A retransmission is a delivery too: a message the broker wrote to the
	// dropped connection and the client never read arrives again with DUP
	// [MQTT-4.4.0-1], and is the only copy this client ever sees. Only a repeat
	// without DUP is a second first delivery.
	firstTime := map[string]int{}
	retransmitted := map[string]int{}
	arrived := func(k string) bool {
		return firstDeliveries[k]+firstTime[k]+retransmitted[k] > 0
	}
	for {
		missing := 0
		for i := 0; i < total; i++ {
			if !arrived(fmt.Sprintf("%04d", i)) {
				missing++
			}
		}
		if missing == 0 {
			break
		}
		require.NoError(t, back.nc.SetReadDeadline(time.Now().Add(5*time.Second)))
		p, err := packet.Read(back.r, 0)
		if err != nil {
			break
		}
		pp, ok := p.(*packet.Publish)
		if !ok {
			continue
		}
		back.send(&packet.Puback{Ack: packet.Ack{PacketID: pp.PacketID}})
		if pp.Dup {
			retransmitted[string(pp.Payload)]++
		} else {
			firstTime[string(pp.Payload)]++
		}
	}

	for i := 0; i < total; i++ {
		k := fmt.Sprintf("%04d", i)
		assert.True(t, arrived(k), "message %s was lost", k)
		first := firstDeliveries[k] + firstTime[k]
		assert.LessOrEqual(t, first, 1, "message %s was delivered %d times without DUP", k, first)
	}
	t.Logf("before drop %d, after resume %d first deliveries and %d retransmissions",
		len(firstDeliveries), len(firstTime), len(retransmitted))
}

// Messages queued for the connection but never written when it died: with a
// Receive Maximum of 1 and the first message unacknowledged, the rest wait in
// the connection's queue, and go down with it. They were published before the
// connection ended, so only a replay that starts before then finds them; the
// in-flight first one is resent by retransmission, not repeated by the replay.
func TestMessagesWaitingOnADyingConnectionAreNotLost(t *testing.T) {
	addr := startBroker(t, startNATS(t), withOfflineQueue)

	c := dialRaw(t, addr)
	cp := rawConnect("offline-dying", 300)
	cp.Properties.ReceiveMaximum = natsmqtt5.Ptr(uint16(1))
	c.connect(cp)
	c.subscribe("w/#", packet.QoS1)

	pub, _ := connectClient(t, addr, connectOpts("pub-offline-dying"))
	for _, n := range []string{"1", "2", "3"} {
		pub.publish(&paho.Publish{Topic: "w/" + n, QoS: 1, Payload: []byte(n)})
	}
	require.Equal(t, "w/1", c.expectPublish().Topic)
	time.Sleep(100 * time.Millisecond) // w/2 and w/3 are waiting behind the quota
	c.drop()
	time.Sleep(100 * time.Millisecond)

	back := dialRaw(t, addr)
	require.True(t, back.connect(rawConnect("offline-dying", 300)).SessionPresent)
	var got []string
	for i := 0; i < 3; i++ {
		p := back.expectPublish()
		got = append(got, p.Topic)
		back.send(&packet.Puback{Ack: packet.Ack{PacketID: p.PacketID}})
	}
	assert.Equal(t, []string{"w/1", "w/2", "w/3"}, got)
	back.expectNothing()
}

func persistentWithQueue(o *natsmqtt5.Options) {
	o.PersistentSessions = true
	o.OfflineQueue = true
}

// A session restored from the session store after a broker restart gets what
// was published while it was away, as one held in memory does.
func TestARestoredSessionGetsWhatWasPublishedWhileItWasAway(t *testing.T) {
	natsURL := startNATS(t)
	addrA, stopA := startStoppableBroker(t, natsURL, persistentWithQueue)
	first, _ := connectClient(t, addrA, durableConnect("offline-restored", 300))
	first.subscribe(paho.SubscribeOptions{Topic: "r/#", QoS: 1})
	require.NoError(t, first.Client.Disconnect(&paho.Disconnect{ReasonCode: 0}))
	stopA()

	addrB := startBroker(t, natsURL, persistentWithQueue)
	pub, _ := connectClient(t, addrB, connectOpts("pub-offline-restored"))
	pub.publish(&paho.Publish{Topic: "r/1", QoS: 1, Payload: []byte("1")})
	pub.publish(&paho.Publish{Topic: "r/2", QoS: 1, Payload: []byte("2")})

	back := dialRaw(t, addrB)
	require.True(t, back.connect(rawConnect("offline-restored", 300)).SessionPresent)
	for _, want := range []string{"r/1", "r/2"} {
		got := back.expectPublish()
		assert.Equal(t, want, got.Topic)
		back.send(&packet.Puback{Ack: packet.Ack{PacketID: got.PacketID}})
	}
	back.expectNothing()
}

// The same for a client that moves to another live broker.
func TestASessionMovingToAnotherBrokerGetsWhatWasPublishedWhileItWasAway(t *testing.T) {
	natsURL := startNATS(t)
	addrA := startBroker(t, natsURL, persistentWithQueue)
	addrB := startBroker(t, natsURL, persistentWithQueue)

	onA := dialRaw(t, addrA)
	onA.connect(rawConnect("offline-moving", 300))
	onA.subscribe("m/#", packet.QoS1)
	onA.send(&packet.Disconnect{})
	time.Sleep(100 * time.Millisecond)

	pub, _ := connectClient(t, addrA, connectOpts("pub-offline-moving"))
	pub.publish(&paho.Publish{Topic: "m/1", QoS: 1, Payload: []byte("1")})

	onB := dialRaw(t, addrB)
	require.True(t, onB.connect(rawConnect("offline-moving", 300)).SessionPresent)
	got := onB.expectPublish()
	assert.Equal(t, "m/1", got.Topic)
	onB.send(&packet.Puback{Ack: packet.Ack{PacketID: got.PacketID}})
	onB.expectNothing()
}

// The disconnect time does not outlive the connection that resumed from it.
// Left in the record while a broker serves the session, a later claim (after a
// broker killed without releasing the session, say) would replay from the old
// disconnect and deliver again what was delivered since. The broker serving the
// session replaces it with its own position within a checkpoint interval
// (TestAKilledBrokersClientIsNotSentWhatItAlreadyGotAgain does it with a real
// kill). Observed in the stored record itself.
func TestAClaimedSessionsRecordForgetsTheOldAbsence(t *testing.T) {
	natsURL := startNATS(t)
	fast := func(o *natsmqtt5.Options) {
		persistentWithQueue(o)
		o.SessionCheckpointInterval = 100 * time.Millisecond
	}
	addrA, stopA := startStoppableBroker(t, natsURL, fast)
	c := dialRaw(t, addrA)
	c.connect(rawConnect("offline-once", 300))
	c.subscribe("o/#", packet.QoS1)
	c.send(&packet.Disconnect{})
	time.Sleep(100 * time.Millisecond)
	stopA()

	stored := func() map[string]any {
		nc, err := nats.Connect(natsURL)
		require.NoError(t, err)
		defer nc.Close()
		js, err := jetstream.New(nc)
		require.NoError(t, err)
		kv, err := js.KeyValue(context.Background(), natsmqtt5.DefaultStreamPrefix+"_sessions")
		require.NoError(t, err)
		entry, err := kv.Get(context.Background(), base64.RawURLEncoding.EncodeToString([]byte("offline-once")))
		require.NoError(t, err)
		var rec map[string]any
		require.NoError(t, json.Unmarshal(entry.Value(), &rec))
		return rec
	}
	released, _ := stored()["AwayAt"].(string)
	require.NotEmpty(t, released, "a released session must record when it was released")

	addrB := startBroker(t, natsURL, fast)
	back := dialRaw(t, addrB)
	require.True(t, back.connect(rawConnect("offline-once", 300)).SessionPresent)
	require.Eventually(t, func() bool {
		at, _ := stored()["AwayAt"].(string)
		return at != "" && at != released
	}, 5*time.Second, 50*time.Millisecond, "the old disconnect time stayed in the record while the session was served")
}
