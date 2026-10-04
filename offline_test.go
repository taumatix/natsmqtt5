package natsmqtt5_test

import (
	"fmt"
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

// Without the option nothing changes: a resumed session gets only what was
// already in flight.
func TestWithoutTheOfflineQueueNothingPublishedWhileAwayArrives(t *testing.T) {
	addr := startBroker(t, startNATS(t))

	away := dialRaw(t, addr)
	away.connect(rawConnect("offline-off", 300))
	away.subscribe("q/#", packet.QoS1)
	away.drop()

	pub, _ := connectClient(t, addr, connectOpts("pub-offline-3"))
	pub.publish(&paho.Publish{Topic: "q/1", QoS: 1, Payload: []byte("1")})

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

	firstTime := map[string]int{}
	dups := 0
	for len(firstTime)+len(firstDeliveries) < total+len(firstDeliveries) {
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
			dups++
			continue
		}
		firstTime[string(pp.Payload)]++
		if len(firstTime) == total {
			break
		}
		missing := 0
		for i := 0; i < total; i++ {
			k := fmt.Sprintf("%04d", i)
			if firstDeliveries[k] == 0 && firstTime[k] == 0 {
				missing++
			}
		}
		if missing == 0 {
			break
		}
	}

	for i := 0; i < total; i++ {
		k := fmt.Sprintf("%04d", i)
		seen := firstDeliveries[k] + firstTime[k]
		assert.GreaterOrEqual(t, seen, 1, "message %s was lost", k)
		assert.LessOrEqual(t, seen, 1, "message %s was delivered %d times without DUP", k, seen)
	}
	t.Logf("before drop %d, after resume %d first deliveries and %d retransmissions", len(firstDeliveries), len(firstTime), dups)
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
