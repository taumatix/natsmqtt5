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

// A connected client that falls behind used to lose QoS 1 and 2 messages: past
// 2048 waiting for it, the broker dropped them [MQTT-4.1.0-1],
// [MQTT-4.5.0-1]. With the offline queue every such message has a copy in the
// stream, so a connection that cannot keep up catches up from there instead.

// slowSubscriber connects with a small Receive Maximum, so the broker can have
// only a few messages in flight to it and the rest wait in the broker.
func slowSubscriber(t *testing.T, addr, id, filter string) *rawClient {
	t.Helper()
	c := dialRaw(t, addr)
	cp := rawConnect(id, 300)
	cp.Properties.ReceiveMaximum = packet.Uint16(5)
	c.connect(cp)
	c.subscribe(filter, packet.QoS1)
	return c
}

// readInOrder reads n PUBLISH packets, acknowledging each, and fails on a
// gap, a repeat or a reordering of the payloads want.
func (c *rawClient) readInOrder(want []string) {
	c.t.Helper()
	for i, w := range want {
		p := c.expectPublish()
		require.Equal(c.t, w, string(p.Payload), "message %d of %d", i, len(want))
		c.send(&packet.Puback{Ack: packet.Ack{PacketID: p.PacketID}})
	}
}

func sequence(from, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("%05d", from+i)
	}
	return out
}

// Thousands more messages than the broker holds for one connection, published
// while the client reads none: every one arrives, once, in publish order,
// since a topic is an ordered topic [MQTT-4.6.0-6].
func TestAClientThatFallsBehindLosesNothing(t *testing.T) {
	addr := startBroker(t, startNATS(t))
	slow := slowSubscriber(t, addr, "slow-1", "s/#")

	pub, _ := connectClient(t, addr, connectOpts("fast-pub-1"))
	const total = 5000
	msgs := sequence(0, total)
	for _, m := range msgs {
		pub.publish(&paho.Publish{Topic: "s/one", QoS: 1, Payload: []byte(m)})
	}

	slow.readInOrder(msgs)
	slow.expectNothing()
}

// Publishing goes on while the client catches up, so the catch-up has to hand
// back to live delivery without a gap or a repeat at the seam.
func TestCatchingUpHandsBackToLiveDelivery(t *testing.T) {
	addr := startBroker(t, startNATS(t))
	slow := slowSubscriber(t, addr, "slow-2", "s/#")

	pub, _ := connectClient(t, addr, connectOpts("fast-pub-2"))
	const first, second = 3000, 2000
	for _, m := range sequence(0, first) {
		pub.publish(&paho.Publish{Topic: "s/two", QoS: 1, Payload: []byte(m)})
	}
	go func() {
		for _, m := range sequence(first, second) {
			pub.publish(&paho.Publish{Topic: "s/two", QoS: 1, Payload: []byte(m)})
			time.Sleep(100 * time.Microsecond)
		}
	}()

	slow.readInOrder(sequence(0, first+second))
	slow.expectNothing()

	pub.publish(&paho.Publish{Topic: "s/two", QoS: 1, Payload: []byte("live")})
	p := slow.expectPublish()
	assert.Equal(t, "live", string(p.Payload), "live delivery resumes after the catch-up")
}

// QoS 0 is "at most once" and has no copy to catch up from, so a client that
// falls behind may lose it, as before; without the queue, so may QoS 1. Neither
// costs the client its connection.
func TestWithoutTheQueueAClientThatFallsBehindStaysConnected(t *testing.T) {
	addr := startBroker(t, startNATS(t), func(o *natsmqtt5.Options) { o.DisableOfflineQueue = true })
	slow := slowSubscriber(t, addr, "slow-3", "s/#")

	pub, _ := connectClient(t, addr, connectOpts("fast-pub-3"))
	for _, m := range sequence(0, 3000) {
		pub.publish(&paho.Publish{Topic: "s/three", QoS: 1, Payload: []byte(m)})
	}
	got := 0
	for {
		require.NoError(t, slow.nc.SetReadDeadline(time.Now().Add(time.Second)))
		pk, err := packet.Read(slow.r, 0)
		if err != nil {
			break
		}
		if p, ok := pk.(*packet.Publish); ok {
			got++
			slow.send(&packet.Puback{Ack: packet.Ack{PacketID: p.PacketID}})
		}
	}
	assert.Greater(t, got, 2000, "the client kept receiving")
	assert.Less(t, got, 3000, "without the queue, what overflowed is gone")
}

// Several publishers at once: their messages' live copies need not arrive in
// the order their queued copies were stored, so what was already waiting for
// the client can be newer than the first message turned away. Each must still
// arrive exactly once.
func TestCatchingUpWithSeveralPublishersDeliversEachOnce(t *testing.T) {
	addr := startBroker(t, startNATS(t))
	slow := slowSubscriber(t, addr, "slow-4", "m/#")

	const publishers, each = 4, 1500
	done := make(chan struct{})
	for p := 0; p < publishers; p++ {
		pub, _ := connectClient(t, addr, connectOpts(fmt.Sprintf("many-pub-%d", p)))
		go func(p int) {
			defer func() { done <- struct{}{} }()
			for _, m := range sequence(0, each) {
				pub.publish(&paho.Publish{Topic: fmt.Sprintf("m/%d", p), QoS: 1, Payload: []byte(fmt.Sprintf("%d-%s", p, m))})
			}
		}(p)
	}
	for p := 0; p < publishers; p++ {
		<-done
	}

	seen := map[string]int{}
	next := map[string]int{}
	for i := 0; i < publishers*each; i++ {
		p := slow.expectPublish()
		seen[string(p.Payload)]++
		var from, n int
		_, err := fmt.Sscanf(string(p.Payload), "%d-%d", &from, &n)
		require.NoError(t, err)
		require.Equal(t, next[p.Topic], n, "%s out of order [MQTT-4.6.0-6]", p.Topic)
		next[p.Topic]++
		slow.send(&packet.Puback{Ack: packet.Ack{PacketID: p.PacketID}})
	}
	slow.expectNothing()
	assert.Len(t, seen, publishers*each)
}
