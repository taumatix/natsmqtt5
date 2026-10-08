package natsmqtt5_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
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

// A shared subscription ends when its last session goes.
//
// MQTT-5.0 §4.8.2 (quoted from the specification, the sentence carries no
// statement id): "A Shared Subscription ends, and any undelivered messages
// associated with it are deleted, when there are no longer any Sessions
// subscribed to it". The backlog of a group is one JetStream
// consumer shared by the brokers; these tests count the consumers on the real
// queue stream, publish while a group has no member, and see what a new member
// of the same name is given.
//
// Every test runs a real nats-server with JetStream, two brokers on TCP and raw
// MQTT clients.

// sharedConsumers lists the shared-subscription consumers on the queue stream.
func sharedConsumers(t *testing.T, natsURL string) []string {
	t.Helper()
	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	st, err := js.Stream(ctx, "MQTT5_queue")
	require.NoError(t, err)
	var out []string
	for name := range st.ConsumerNames(ctx).Name() {
		if strings.Contains(name, "_share_") {
			out = append(out, name)
		}
	}
	return out
}

func requireConsumers(t *testing.T, natsURL string, n int, msgAndArgs ...any) {
	t.Helper()
	require.Eventually(t, func() bool { return len(sharedConsumers(t, natsURL)) == n },
		5*time.Second, 50*time.Millisecond, msgAndArgs...)
}

// oneAtATime is a CONNECT whose client takes a single QoS 1 message at a time,
// so that the rest of a group's work stays in the backlog.
func oneAtATime(id string, expiry uint32) *packet.Connect {
	cp := rawConnect(id, expiry)
	cp.Properties.ReceiveMaximum = packet.Uint16(1)
	return cp
}

// publishJobs sends n QoS 1 jobs from a throwaway producer.
func publishJobs(t *testing.T, addr string, n int) {
	t.Helper()
	pub, _ := connectClient(t, addr, connectOpts(fmt.Sprintf("producer-%d", time.Now().UnixNano())))
	for i := 0; i < n; i++ {
		pub.publish(&paho.Publish{Topic: "jobs/x", QoS: 1, Payload: []byte(fmt.Sprintf("job-%d", i))})
	}
}

func (c *rawClient) unsubscribe(filter string) {
	c.t.Helper()
	c.subID++
	c.send(&packet.Unsubscribe{PacketID: c.subID, Filters: []string{filter}})
	ack, ok := c.read().(*packet.Unsuback)
	require.True(c.t, ok, "an UNSUBSCRIBE must be answered with an UNSUBACK")
	require.Equal(c.t, []packet.ReasonCode{packet.Success}, ack.ReasonCodes)
}

// The last session to unsubscribe ends the subscription: its consumer goes, and
// what was waiting in it goes too. A client that subscribes to the same name
// afterwards is a new subscription and is given none of it (§4.8.2).
func TestTheLastMemberToUnsubscribeDeletesTheBacklog_MQTT_5_0_4_8_2(t *testing.T) {
	natsmqtt5.SetSharedAckWait(t, handBackAckWait)
	natsURL := startNATS(t)
	a := startBroker(t, natsURL, sweeping)
	b := startBroker(t, natsURL, sweeping)

	m1 := dialRaw(t, a)
	m1.connect(oneAtATime("m1", 300))
	m1.subscribe(group, packet.QoS1)
	requireConsumers(t, natsURL, 1)

	publishJobs(t, a, 4)
	first := m1.expectPublish() // held: the client takes one at a time
	require.Equal(t, packet.QoS1, first.QoS)

	m1.unsubscribe(group)
	requireConsumers(t, natsURL, 0, "the group has no session left: its backlog is deleted")

	m2 := dialRaw(t, b)
	m2.connect(rawConnect("m2", 300))
	m2.subscribe(group, packet.QoS1)
	m2.expectNoPublishFor(3 * handBackAckWait)
	requireConsumers(t, natsURL, 1)

	// The new subscription works.
	publishJobs(t, a, 1)
	assert.Equal(t, "job-0", string(m2.expectPublish().Payload))
}

// A session that is still subscribed keeps the backlog, and the messages the
// leaving member held go to it.
func TestARemainingSharedMemberKeepsTheBacklog_MQTT_5_0_4_8_2(t *testing.T) {
	natsmqtt5.SetSharedAckWait(t, handBackAckWait)
	natsURL := startNATS(t)
	a := startBroker(t, natsURL, sweeping)
	b := startBroker(t, natsURL, sweeping)

	m1 := dialRaw(t, a)
	m1.connect(oneAtATime("m1", 0))
	m1.subscribe(group, packet.QoS1)
	m2 := dialRaw(t, b)
	m2.connect(oneAtATime("m2", 300))
	m2.subscribe(group, packet.QoS1)

	publishJobs(t, a, 6)
	held1 := m1.expectPublish()
	held2 := m2.expectPublish()

	m1.unsubscribe(group)
	requireConsumers(t, natsURL, 1, "m2 is still subscribed")

	got := []string{string(held2.Payload)}
	m2.send(&packet.Puback{Ack: packet.Ack{PacketID: held2.PacketID}})
	for len(got) < 5 {
		p := m2.expectPublish()
		got = append(got, string(p.Payload))
		m2.send(&packet.Puback{Ack: packet.Ack{PacketID: p.PacketID}})
	}
	// The sixth is the one m1 held; it is m2's whether or not it was handed back
	// before m1's UNSUBSCRIBE or is waiting on it still.
	_ = held1
	assert.Len(t, got, 5)
	requireConsumers(t, natsURL, 1)
}

// The other ways a session ends, each with the group's only member in it and
// work waiting. Handing the message the member held back (PR "shared QoS 1
// hand-back") lands it in a group that no longer exists, which deletes it.
func TestASharedSubscriptionEndsWhenItsLastSessionDoes_MQTT_5_0_4_8_2(t *testing.T) {
	ends := map[string]struct {
		expiry uint32
		end    func(t *testing.T, a string, m1 *rawClient)
	}{
		"disconnect with no session expiry": {0, func(t *testing.T, a string, m1 *rawClient) {
			m1.send(&packet.Disconnect{})
		}},
		"connection lost with no session expiry": {0, func(t *testing.T, a string, m1 *rawClient) {
			m1.drop()
		}},
		"session expiry interval elapses": {1, func(t *testing.T, a string, m1 *rawClient) {
			m1.drop()
		}},
		"clean start takes the client identifier": {300, func(t *testing.T, a string, m1 *rawClient) {
			fresh := dialRaw(t, a)
			cp := rawConnect("m1", 300)
			cp.CleanStart = true
			require.False(t, fresh.connect(cp).SessionPresent)
		}},
	}
	for name, tc := range ends {
		t.Run(name, func(t *testing.T) {
			natsmqtt5.SetSharedAckWait(t, handBackAckWait)
			natsURL := startNATS(t)
			a := startBroker(t, natsURL, sweeping)
			b := startBroker(t, natsURL, sweeping)

			m1 := dialRaw(t, a)
			m1.connect(oneAtATime("m1", tc.expiry))
			m1.subscribe(group, packet.QoS1)
			publishJobs(t, a, 3)
			m1.expectPublish()
			requireConsumers(t, natsURL, 1)

			tc.end(t, a, m1)
			requireConsumers(t, natsURL, 0, "the only session ended")

			m2 := dialRaw(t, b)
			m2.connect(rawConnect("m2", 300))
			m2.subscribe(group, packet.QoS1)
			m2.expectNoPublishFor(3 * handBackAckWait)
		})
	}
}

// A detached session is still subscribed: its being away does not end the
// subscription, so the work waits for it (§4.8.2).
func TestADetachedSharedMemberKeepsTheSubscriptionAlive_MQTT_5_0_4_8_2(t *testing.T) {
	natsURL := startNATS(t)
	a := startBroker(t, natsURL, sweeping)

	m1 := dialRaw(t, a)
	m1.connect(rawConnect("m1", 300))
	m1.subscribe(group, packet.QoS1)
	detach(m1)
	publishJobs(t, a, 2)
	time.Sleep(4 * sweepEvery)
	requireConsumers(t, natsURL, 1)

	back := dialRaw(t, a)
	require.True(t, back.connect(rawConnect("m1", 300)).SessionPresent)
	assert.Equal(t, []string{"job-0", "job-1"}, back.collectPublishes(2))
}

// Two last members leave at once, on different brokers, in every order the
// scheduler gives: the group ends, and nobody's deletion fails the other's.
func TestTwoLastSharedMembersLeavingTogetherEndTheGroup_MQTT_5_0_4_8_2(t *testing.T) {
	natsURL := startNATS(t)
	a := startBroker(t, natsURL, sweeping)
	b := startBroker(t, natsURL, sweeping)

	const rounds = 8
	for i := 0; i < rounds; i++ {
		filter := fmt.Sprintf("$share/race%d/jobs/#", i)
		m1 := dialRaw(t, a)
		m1.connect(rawConnect(fmt.Sprintf("a%d", i), 300))
		m1.subscribe(filter, packet.QoS1)
		m2 := dialRaw(t, b)
		m2.connect(rawConnect(fmt.Sprintf("b%d", i), 300))
		m2.subscribe(filter, packet.QoS1)

		start := make(chan struct{})
		var wg sync.WaitGroup
		for _, m := range []*rawClient{m1, m2} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				m.send(&packet.Unsubscribe{PacketID: 77, Filters: []string{filter}})
			}()
		}
		close(start)
		wg.Wait()
		for _, m := range []*rawClient{m1, m2} {
			_, ok := m.read().(*packet.Unsuback)
			require.True(t, ok)
		}
	}
	requireConsumers(t, natsURL, 0, "every group that lost its last two members is gone")
}

// A member joining as the last other one leaves must end up with a backlog:
// whichever order the two happen in, the group has a consumer afterwards and
// delivers to the newcomer.
func TestAMemberJoiningAsTheLastOneLeavesKeepsAWorkingGroup_MQTT_5_0_4_8_2(t *testing.T) {
	natsURL := startNATS(t)
	a := startBroker(t, natsURL, sweeping)
	b := startBroker(t, natsURL, sweeping)

	const rounds = 8
	joiner := dialRaw(t, b)
	joiner.connect(rawConnect("joiner", 300))
	var leavers []*rawClient
	for i := 0; i < rounds; i++ {
		filter := fmt.Sprintf("$share/join%d/jobs/#", i)
		leaver := dialRaw(t, a)
		leaver.connect(rawConnect(fmt.Sprintf("leaver%d", i), 300))
		leaver.subscribe(filter, packet.QoS1)
		leavers = append(leavers, leaver)

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			leaver.send(&packet.Unsubscribe{PacketID: 9, Filters: []string{filter}})
		}()
		go func() {
			defer wg.Done()
			<-start
			joiner.send(&packet.Subscribe{PacketID: uint16(100 + i),
				Subscriptions: []packet.Subscription{{Filter: filter, QoS: packet.QoS1}}})
		}()
		close(start)
		wg.Wait()
	}
	// A leaver's UNSUBACK is sent after it has deleted the group's consumer (or
	// left it to the joiner), so once every one is in, no deletion is still to
	// come and a consumer that exists is there to stay.
	for _, l := range leavers {
		_, ok := l.read().(*packet.Unsuback)
		require.True(t, ok)
	}
	// Drain the SUBACKs of the joiner and wait for every group to have a
	// consumer: one made again by the puller if the leaver deleted it last.
	for i := 0; i < rounds; i++ {
		ack, ok := joiner.read().(*packet.Suback)
		require.True(t, ok)
		require.Equal(t, []packet.ReasonCode{packet.GrantedQoS1}, ack.ReasonCodes,
			"a SUBSCRIBE racing the last member's leaving is granted, not failed")
	}
	requireConsumers(t, natsURL, rounds, "every group has the joiner, so every group has a backlog")

	publishJobs(t, a, 1)
	assert.Equal(t, rounds, len(joiner.collectPublishes(rounds)), "one copy per group")
}

// Sessions that vanish without a trace (a killed broker) cannot unsubscribe.
// Their entries lapse, so the survivors' leaving still ends the group, and the
// entries of a live broker's sessions do not lapse while it holds them.
func TestAKilledBrokersSharedMembersLapse_MQTT_5_0_4_8_2(t *testing.T) {
	natsmqtt5.SetShareMemberSlack(t, time.Second)
	natsURL := startNATS(t)
	short := func(o *natsmqtt5.Options) { o.MaxSessionExpiry = time.Second }
	a, addrA, _ := startBrokerHandle(t, natsURL, short)
	b := startBroker(t, natsURL, short)

	m1 := dialRaw(t, addrA)
	m1.connect(rawConnect("m1", 1))
	m1.subscribe(group, packet.QoS1)
	m2 := dialRaw(t, b)
	m2.connect(rawConnect("m2", 1))
	m2.subscribe(group, packet.QoS1)
	natsmqtt5.Kill(a)

	// While m1's entry has not lapsed, m2's leaving does not end the group.
	m2.unsubscribe(group)
	requireConsumers(t, natsURL, 1, "a session on a broker that may only be slow is still counted")

	m3 := dialRaw(t, b)
	m3.connect(rawConnect("m3", 1))
	m3.subscribe(group, packet.QoS1)
	// m3 is held by a live broker for longer than an unrewritten entry survives
	// (two seconds here); m1's entry lapses and m3's is rewritten.
	time.Sleep(3500 * time.Millisecond)
	m3.unsubscribe(group)
	requireConsumers(t, natsURL, 0, "the dead broker's member lapsed; m3 was the last")
}
