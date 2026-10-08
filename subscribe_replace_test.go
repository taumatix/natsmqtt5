package natsmqtt5_test

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// [MQTT-3.8.4-4] "If the Retain Handling option is 0, any existing retained
// messages matching the Topic Filter MUST be re-sent, but Application Messages
// MUST NOT be lost due to replacing the Subscription." (OASIS MQTT 5.0, read
// 2026-10-08.) The first half is in broker_test.go; these tests are the second:
// a stream of publishes while the same filter is subscribed again and again
// loses none of them, over a real TCP socket to a broker on a JetStream-enabled
// NATS server.

// replaceStream publishes total messages on topic while the subscriber
// re-subscribes to filter replacements times, and returns the payloads the
// subscriber received, in order.
func replaceStream(t *testing.T, addr, filter, topic string, qos packet.QoS, total, replacements int) []string {
	t.Helper()
	sub, _ := p2Connect(t, addr, "sub")
	opts := packet.Subscription{Filter: filter, QoS: qos, RetainHandling: packet.RetainSendNever}
	require.Equal(t, packet.ReasonCode(qos), p2Sub(t, sub.rawClient, opts))

	var (
		mu   sync.Mutex
		got  []string
		subs = make(chan *packet.Suback, 32)
		done = make(chan struct{})
	)
	go func() {
		defer close(done)
		seen := map[string]bool{}
		for {
			_ = sub.nc.SetReadDeadline(time.Now().Add(3 * time.Second))
			p, err := packet.Read(sub.r, 0)
			if err != nil {
				return
			}
			switch v := p.(type) {
			case *packet.Publish:
				if v.QoS == packet.QoS1 {
					sub.send(&packet.Puback{Ack: packet.Ack{PacketID: v.PacketID}})
				}
				mu.Lock()
				got = append(got, string(v.Payload))
				seen[string(v.Payload)] = true
				n := len(seen)
				mu.Unlock()
				if n == total {
					return
				}
			case *packet.Suback:
				subs <- v
			}
		}
	}()

	pub, _ := p2Connect(t, addr, "pub")
	go func() {
		for i := 0; i < total; i++ {
			m := &packet.Publish{Topic: topic, Payload: []byte(strconv.Itoa(i))}
			if qos == packet.QoS1 {
				m.QoS = packet.QoS1
				m.PacketID = uint16(i + 1)
			}
			pub.send(m)
			time.Sleep(time.Millisecond)
		}
	}()
	for i := 0; i < replacements; i++ {
		sub.subID++
		sub.send(&packet.Subscribe{PacketID: sub.subID, Subscriptions: []packet.Subscription{opts}})
		select {
		case ack := <-subs:
			assert.Equal(t, sub.subID, ack.PacketID)
		case <-time.After(5 * time.Second):
			t.Fatal("no SUBACK for a replacing SUBSCRIBE")
		}
		time.Sleep(10 * time.Millisecond)
	}
	<-done
	mu.Lock()
	defer mu.Unlock()
	return append([]string(nil), got...)
}

func missingFrom(got []string, total int) []int {
	seen := map[string]bool{}
	for _, g := range got {
		seen[g] = true
	}
	var missing []int
	for i := 0; i < total; i++ {
		if !seen[strconv.Itoa(i)] {
			missing = append(missing, i)
		}
	}
	return missing
}

func duplicatesIn(got []string) []string {
	seen := map[string]bool{}
	var dups []string
	for _, g := range got {
		if seen[g] {
			dups = append(dups, g)
		}
		seen[g] = true
	}
	return dups
}

func TestSubscribe_MQTT_3_8_4_4(t *testing.T) {
	const total = 300

	t.Run("QoS 0 loses nothing and repeats nothing", func(t *testing.T) {
		got := replaceStream(t, p2Start(t), "loss/#", "loss/n", packet.QoS0, total, 12)
		assert.Empty(t, missingFrom(got, total), "messages lost while the subscription was being replaced")
		assert.Empty(t, duplicatesIn(got), "a replaced subscription must not deliver a message twice")
	})

	t.Run("QoS 1 loses nothing", func(t *testing.T) {
		got := replaceStream(t, p2Start(t), "loss/#", "loss/n", packet.QoS1, total, 12)
		assert.Empty(t, missingFrom(got, total), "messages lost while the subscription was being replaced")
	})

	// With the offline queue and persistent sessions on, QoS 1 travels through a
	// JetStream stream and the replacement also rewrites the session record.
	t.Run("on a JetStream broker with persistent sessions", func(t *testing.T) {
		addr := p2Start(t, func(o *natsmqtt5.Options) {
			o.OfflineQueue = true
			o.PersistentSessions = true
		})
		got := replaceStream(t, addr, "loss/#", "loss/n", packet.QoS1, total, 12)
		assert.Empty(t, missingFrom(got, total), "messages lost while the subscription was being replaced")
	})

	t.Run("an exact filter, not only a wildcard", func(t *testing.T) {
		got := replaceStream(t, p2Start(t), "loss/n", "loss/n", packet.QoS0, total, 12)
		assert.Empty(t, missingFrom(got, total))
		assert.Empty(t, duplicatesIn(got))
	})
}

// [MQTT-3.8.4-3] "If a Server receives a SUBSCRIBE packet containing a Topic
// Filter that is identical to a Non-shared Subscription's Topic Filter for the
// current Session, then it MUST replace that existing Subscription with a new
// Subscription." The replacement's options govern what follows: its
// Subscription Identifier is on the next message, and the old one is not.
func TestSubscribe_MQTT_3_8_4_3_ReplacementOptionsGovern(t *testing.T) {
	addr := p2Start(t)
	sub, _ := p2Connect(t, addr, "sub")
	pub, _ := p2Connect(t, addr, "pub")

	subscribeWithID := func(id int, qos packet.QoS) packet.ReasonCode {
		sub.subID++
		sub.send(&packet.Subscribe{PacketID: sub.subID,
			Properties:    &packet.Properties{SubscriptionIdentifiers: []int{id}},
			Subscriptions: []packet.Subscription{{Filter: "opt/#", QoS: qos}}})
		ack := p2Next[*packet.Suback](t, sub.rawClient)
		return ack.ReasonCodes[0]
	}
	deliver := func(payload string) *packet.Publish {
		pub.send(&packet.Publish{Topic: "opt/x", Payload: []byte(payload)})
		return p2Next[*packet.Publish](t, sub.rawClient)
	}

	require.Equal(t, packet.Success, subscribeWithID(7, packet.QoS0))
	require.Equal(t, []int{7}, deliver("one").Properties.SubscriptionIdentifiers)

	require.Equal(t, packet.Success, subscribeWithID(9, packet.QoS0))
	assert.Equal(t, []int{9}, deliver("two").Properties.SubscriptionIdentifiers,
		"the replacing Subscription's identifier, and only it")

	// The granted QoS is the replacement's as well [MQTT-3.8.4-8]: a QoS 1
	// message is now sent at QoS 1.
	require.Equal(t, packet.ReasonCode(packet.QoS1), subscribeWithID(9, packet.QoS1))
	pub.send(&packet.Publish{Topic: "opt/x", QoS: packet.QoS1, PacketID: 1, Payload: []byte("three")})
	got := p2Next[*packet.Publish](t, sub.rawClient)
	assert.Equal(t, packet.QoS1, got.QoS)
	assert.Equal(t, "three", string(got.Payload))
}

// [MQTT-3.8.4-4] for a Shared Subscription: the members of a group that a
// member leaves and rejoins by subscribing again lose nothing either.
func TestSubscribe_MQTT_3_8_4_4_SharedLoses_Nothing(t *testing.T) {
	const total = 300
	got := replaceStream(t, p2Start(t), "$share/g/loss/#", "loss/n", packet.QoS0, total, 12)
	assert.Empty(t, missingFrom(got, total), "messages lost while the shared subscription was being replaced")
}

func TestSubscribe_MQTT_3_8_4_4_SharedBacklogLosesNothing(t *testing.T) {
	const total = 300
	addr := p2Start(t, func(o *natsmqtt5.Options) {
		o.OfflineQueue = true
		o.PersistentSessions = true
	})
	got := replaceStream(t, addr, "$share/g/loss/#", "loss/n", packet.QoS1, total, 12)
	assert.Empty(t, missingFrom(got, total), "messages lost while the shared subscription was being replaced")
}
