package natsmqtt5_test

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// floodPast publishes more QoS 1 messages than the broker holds for one connection (2048) to a client that
// has read none of them, through a broker with no offline queue, so none of them has a copy to catch up from.
func floodPast(t *testing.T, addr string, topic string) {
	t.Helper()
	pub, _ := connectClient(t, addr, connectOpts("cb-pub-"+topic))
	for _, m := range sequence(0, 2300) {
		pub.publish(&paho.Publish{Topic: topic, QoS: 1, Payload: []byte(m)})
	}
}

// readUntilClosed reads what the broker sends until it closes the connection and returns the DISCONNECT it
// sent before that, or nil.
func (c *rawClient) readUntilClosed(within time.Duration) *packet.Disconnect {
	c.t.Helper()
	var last *packet.Disconnect
	deadline := time.Now().Add(within)
	for {
		require.NoError(c.t, c.nc.SetReadDeadline(deadline))
		p, err := packet.Read(c.r, 0)
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				c.t.Fatal("the broker did not close the connection")
			}
			return last
		}
		if d, ok := p.(*packet.Disconnect); ok {
			last = d
		}
	}
}

// With CloseBehindClients and no offline queue, a client that is 2048 messages behind is disconnected with
// 0x97 (Quota exceeded) instead of silently losing the QoS 1 messages past that point, and its session is
// kept, so it can resume and see the loss as a reconnect.
func TestCloseBehindClientsDisconnectsAClientThatFallsBehindWithoutTheQueue(t *testing.T) {
	t.Parallel()
	addr := startBroker(t, startNATS(t), func(o *natsmqtt5.Options) {
		o.DisableOfflineQueue = true
		o.CloseBehindClients = true
	})
	slow := slowSubscriber(t, addr, "cb-slow", "cb/#")
	floodPast(t, addr, "cb/one")

	d := slow.readUntilClosed(20 * time.Second)
	require.NotNil(t, d, "the broker sent a DISCONNECT before it closed")
	assert.Equal(t, packet.QuotaExceeded, d.ReasonCode)

	back := dialRaw(t, addr)
	cp := rawConnect("cb-slow", 300)
	cp.CleanStart = false
	ack := back.connect(cp)
	assert.True(t, ack.SessionPresent, "the session was left to resume")
}

// Off by default: the connection stays open and the broker drops, as before.
func TestAClientThatFallsBehindWithoutTheQueueStaysConnectedByDefault(t *testing.T) {
	t.Parallel()
	addr := startBroker(t, startNATS(t), func(o *natsmqtt5.Options) { o.DisableOfflineQueue = true })
	slow := slowSubscriber(t, addr, "cb-default", "cb/#")
	floodPast(t, addr, "cb/two")

	slow.expectPublish() // still connected and still served
}

// A client that keeps up is never disconnected.
func TestCloseBehindClientsLeavesAClientThatKeepsUp(t *testing.T) {
	t.Parallel()
	addr := startBroker(t, startNATS(t), func(o *natsmqtt5.Options) {
		o.DisableOfflineQueue = true
		o.CloseBehindClients = true
	})
	sub := dialRaw(t, addr)
	sub.connect(rawConnect("cb-fast", 300))
	sub.subscribe("cb/#", packet.QoS1)
	pub, _ := connectClient(t, addr, connectOpts("cb-fast-pub"))
	msgs := sequence(0, 300)
	for _, m := range msgs {
		pub.publish(&paho.Publish{Topic: "cb/three", QoS: 1, Payload: []byte(m)})
	}
	sub.readInOrder(msgs)
	sub.expectNothing()
}

// With the offline queue a client that falls behind catches up from it and loses nothing, so the option has
// nothing to close it for.
func TestCloseBehindClientsDoesNotCloseAClientThatCanCatchUpFromTheQueue(t *testing.T) {
	t.Parallel()
	addr := startBroker(t, startNATS(t), func(o *natsmqtt5.Options) { o.CloseBehindClients = true })
	slow := slowSubscriber(t, addr, "cb-queue", "cb/#")
	pub, _ := connectClient(t, addr, connectOpts("cb-queue-pub"))
	msgs := sequence(0, 3000)
	for _, m := range msgs {
		pub.publish(&paho.Publish{Topic: "cb/four", QoS: 1, Payload: []byte(m)})
	}
	slow.readInOrder(msgs)
}
