package natsmqtt5_test

import (
	"fmt"
	"log/slog"
	"sort"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// A shared subscription's QoS 1 and 2 messages are handed out from a backlog
// that only connected members take from. MQTT-5.0 §4.8.2 lets the server
// choose a member "on a message by message basis" by whatever criteria it
// likes; once chosen, the message is part of that session's state
// [MQTT-4.5.0-1]. Choosing a member with no connection, and then dropping the
// message because it had none, lost 27 of 40 messages in a probe of v0.9.0.

// collectPublishes reads n PUBLISH packets, acknowledging each, and returns
// their payloads sorted. Shared subscriptions are not ordered topics
// [MQTT-4.6.0-6 covers non-shared ones only], so the order is not asserted.
func (c *rawClient) collectPublishes(n int) []string {
	c.t.Helper()
	got := make([]string, 0, n)
	for len(got) < n {
		p := c.expectPublish()
		got = append(got, string(p.Payload))
		if p.QoS > packet.QoS0 {
			c.send(&packet.Puback{Ack: packet.Ack{PacketID: p.PacketID}})
		}
	}
	sort.Strings(got)
	return got
}

func numbered(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("%03d", i)
	}
	sort.Strings(out)
	return out
}

func loggingBroker(t *testing.T, logs *syncBuffer) func(*natsmqtt5.Options) {
	return func(o *natsmqtt5.Options) { o.Logger = slog.New(slog.NewTextHandler(logs, nil)) }
}

// The v0.9.0 probe, as a test: one member of two goes away, and every message
// published meanwhile reaches the one still connected.
func TestADisconnectedSharedMemberStopsTakingWork(t *testing.T) {
	var logs syncBuffer
	addr := startBroker(t, startNATS(t), loggingBroker(t, &logs))

	stays := dialRaw(t, addr)
	stays.connect(rawConnect("worker-stays", 300))
	stays.subscribe("$share/workers/jobs/#", packet.QoS1)

	away := dialRaw(t, addr)
	away.connect(rawConnect("worker-away", 300))
	away.subscribe("$share/workers/jobs/#", packet.QoS1)
	away.drop()
	waitDetached(t, &logs, "worker-away")

	pub, _ := connectClient(t, addr, connectOpts("producer-1"))
	const jobs = 40
	for _, n := range numbered(jobs) {
		pub.publish(&paho.Publish{Topic: "jobs/x", QoS: 1, Payload: []byte(n)})
	}

	assert.Equal(t, numbered(jobs), stays.collectPublishes(jobs),
		"every job reaches the connected member [MQTT-4.5.0-1]")
	stays.expectNothing()

	back := dialRaw(t, addr)
	require.True(t, back.connect(rawConnect("worker-away", 300)).SessionPresent)
	back.expectNothing()
}

// With no member connected, the messages wait for the subscription, as
// §4.8.2 has them do ("any undelivered messages associated with it"), and the
// first member back gets them.
func TestASharedSubscriptionWithNoMemberConnectedKeepsItsMessages(t *testing.T) {
	var logs syncBuffer
	addr := startBroker(t, startNATS(t), loggingBroker(t, &logs))

	for _, id := range []string{"idle-a", "idle-b"} {
		w := dialRaw(t, addr)
		w.connect(rawConnect(id, 300))
		w.subscribe("$share/idle/jobs/#", packet.QoS1)
		w.drop()
		waitDetached(t, &logs, id)
	}

	pub, _ := connectClient(t, addr, connectOpts("producer-2"))
	const jobs = 10
	for _, n := range numbered(jobs) {
		pub.publish(&paho.Publish{Topic: "jobs/y", QoS: 1, Payload: []byte(n)})
	}

	back := dialRaw(t, addr)
	require.True(t, back.connect(rawConnect("idle-b", 300)).SessionPresent)
	assert.Equal(t, numbered(jobs), back.collectPublishes(jobs))
	back.expectNothing()

	other := dialRaw(t, addr)
	require.True(t, other.connect(rawConnect("idle-a", 300)).SessionPresent)
	other.expectNothing()
}

// Two brokers are one logical broker, and a shared subscription spans them:
// a member gone from one broker leaves the work to a member on the other.
func TestASharedMemberOnAnotherBrokerTakesTheAbsentMembersWork(t *testing.T) {
	natsURL := startNATS(t)
	var logsA syncBuffer
	addrA := startBroker(t, natsURL, loggingBroker(t, &logsA))
	addrB := startBroker(t, natsURL)

	onB := dialRaw(t, addrB)
	onB.connect(rawConnect("worker-on-b", 300))
	onB.subscribe("$share/fleet/jobs/#", packet.QoS1)

	onA := dialRaw(t, addrA)
	onA.connect(rawConnect("worker-on-a", 300))
	onA.subscribe("$share/fleet/jobs/#", packet.QoS1)
	onA.drop()
	waitDetached(t, &logsA, "worker-on-a")

	pub, _ := connectClient(t, addrA, connectOpts("producer-3"))
	const jobs = 20
	for _, n := range numbered(jobs) {
		pub.publish(&paho.Publish{Topic: "jobs/z", QoS: 1, Payload: []byte(n)})
	}
	assert.Equal(t, numbered(jobs), onB.collectPublishes(jobs))
	onB.expectNothing()
}

// A member that unsubscribes takes no more work, and two connected members
// still get each message exactly once between them.
func TestAnUnsubscribedSharedMemberTakesNoMoreWork(t *testing.T) {
	addr := startBroker(t, startNATS(t))

	stays := dialRaw(t, addr)
	stays.connect(rawConnect("worker-1", 300))
	stays.subscribe("$share/u/jobs/#", packet.QoS1)

	leaves := dialRaw(t, addr)
	leaves.connect(rawConnect("worker-2", 300))
	leaves.subscribe("$share/u/jobs/#", packet.QoS1)
	leaves.send(&packet.Unsubscribe{PacketID: 9, Filters: []string{"$share/u/jobs/#"}})
	_, ok := leaves.read().(*packet.Unsuback)
	require.True(t, ok)

	pub, _ := connectClient(t, addr, connectOpts("producer-4"))
	const jobs = 20
	for _, n := range numbered(jobs) {
		pub.publish(&paho.Publish{Topic: "jobs/u", QoS: 1, Payload: []byte(n)})
	}
	assert.Equal(t, numbered(jobs), stays.collectPublishes(jobs))
	stays.expectNothing()
	leaves.expectNothing()
}

// QoS 0 has no claim on a session that is away, so it still goes straight to
// whichever connected member NATS picks.
func TestASharedSubscriptionStillDeliversQoS0(t *testing.T) {
	addr := startBroker(t, startNATS(t))

	w := dialRaw(t, addr)
	w.connect(rawConnect("worker-qos0", 300))
	w.subscribe("$share/z/jobs/#", packet.QoS1)

	pub, _ := connectClient(t, addr, connectOpts("producer-5"))
	pub.publish(&paho.Publish{Topic: "jobs/0", QoS: 0, Payload: []byte("zero")})
	p := w.expectPublish()
	assert.Equal(t, "zero", string(p.Payload))
	assert.Equal(t, packet.QoS0, p.QoS)
}

// A shared member's message is delivered once, including when the member
// comes back in the middle of a stream of work.
func TestSharedWorkIsDeliveredOnceAcrossAReconnect(t *testing.T) {
	var logs syncBuffer
	addr := startBroker(t, startNATS(t), loggingBroker(t, &logs))

	stays := dialRaw(t, addr)
	stays.connect(rawConnect("steady", 300))
	stays.subscribe("$share/r/jobs/#", packet.QoS1)

	flaky := dialRaw(t, addr)
	flaky.connect(rawConnect("flaky", 300))
	flaky.subscribe("$share/r/jobs/#", packet.QoS1)
	flaky.drop()
	waitDetached(t, &logs, "flaky")

	pub, _ := connectClient(t, addr, connectOpts("producer-6"))
	const jobs = 60
	published := make(chan struct{})
	go func() {
		defer close(published)
		for _, n := range numbered(jobs) {
			pub.publish(&paho.Publish{Topic: "jobs/r", QoS: 1, Payload: []byte(n)})
			time.Sleep(2 * time.Millisecond)
		}
	}()

	time.Sleep(30 * time.Millisecond)
	back := dialRaw(t, addr)
	require.True(t, back.connect(rawConnect("flaky", 300)).SessionPresent)
	<-published

	seen := map[string]int{}
	read := func(c *rawClient) bool {
		require.NoError(t, c.nc.SetReadDeadline(time.Now().Add(500*time.Millisecond)))
		pk, err := packet.Read(c.r, 0)
		if err != nil {
			return false
		}
		p, ok := pk.(*packet.Publish)
		require.True(t, ok)
		require.False(t, p.Dup, "nothing was in flight to resend")
		seen[string(p.Payload)]++
		c.send(&packet.Puback{Ack: packet.Ack{PacketID: p.PacketID}})
		return true
	}
	for idle := 0; idle < 2; {
		gotA, gotB := read(stays), read(back)
		if gotA || gotB {
			idle = 0
		} else {
			idle++
		}
	}
	for _, n := range numbered(jobs) {
		assert.Equal(t, 1, seen[n], "job %s", n)
	}
	assert.Len(t, seen, jobs)
}

// The backlog is in JetStream, not in a broker, so it outlives the broker its
// members were on: with persistent sessions, a member that comes back on
// another broker after the first has gone gets what waited for the group.
func TestASharedBacklogOutlivesTheBrokerItsMembersWereOn(t *testing.T) {
	natsURL := startNATS(t)
	addrA, stopA := startStoppableBroker(t, natsURL, persistentWithQueue)

	w := dialRaw(t, addrA)
	w.connect(rawConnect("roamer", 300))
	w.subscribe("$share/roam/jobs/#", packet.QoS1)
	w.send(&packet.Disconnect{})
	stopA()

	addrB := startBroker(t, natsURL, persistentWithQueue)
	pub, _ := connectClient(t, addrB, connectOpts("producer-7"))
	const jobs = 10
	for _, n := range numbered(jobs) {
		pub.publish(&paho.Publish{Topic: "jobs/roam", QoS: 1, Payload: []byte(n)})
	}

	back := dialRaw(t, addrB)
	require.True(t, back.connect(rawConnect("roamer", 300)).SessionPresent)
	assert.Equal(t, numbered(jobs), back.collectPublishes(jobs))
	back.expectNothing()
}

// Without the offline queue there is no backlog, and a shared subscription is
// the NATS queue group it was before: connected members still share the work.
func TestWithoutTheQueueASharedSubscriptionIsAQueueGroup(t *testing.T) {
	addr := startBroker(t, startNATS(t), func(o *natsmqtt5.Options) { o.DisableOfflineQueue = true })

	a := dialRaw(t, addr)
	a.connect(rawConnect("plain-a", 300))
	a.subscribe("$share/plain/jobs/#", packet.QoS1)

	pub, _ := connectClient(t, addr, connectOpts("producer-8"))
	const jobs = 10
	for _, n := range numbered(jobs) {
		pub.publish(&paho.Publish{Topic: "jobs/p", QoS: 1, Payload: []byte(n)})
	}
	assert.Equal(t, numbered(jobs), a.collectPublishes(jobs))
	a.expectNothing()
}
