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

// A catch-up read whose next fetch gets no answer after the broker's NATS connection reconnected is started
// again rather than ending the client's connection (conn.retryAfterDrop). nats.go's ordered consumer rides
// over a drop, so the real drop here (the proxy closing the broker's real NATS connection) does not fail a
// fetch by itself; SetUnansweredQueueFetches makes one fetch fail as a cut request would. The broker, the
// NATS server and the client's socket are real.
func TestReconnectCatchUpReadWhoseFetchIsUnansweredAfterADropStartsAgain(t *testing.T) {
	natsURL := startNATS(t)
	proxy := newDropProxy(t, natsURL)
	subBroker, subAddr, _ := startBrokerHandle(t, proxy.url(), quickReconnect, withReconnectCatchUp)
	pubAddr := startBroker(t, natsURL)

	cp := connectOpts("rad-sub")
	cp.Properties = &paho.ConnectProperties{ReceiveMaximum: packet.Uint16(2)}
	sub, _ := connectClient(t, subAddr, cp, manualAck)
	sub.subscribe(paho.SubscribeOptions{Topic: "rad/t", QoS: 1})
	pub, _ := connectClient(t, pubAddr, connectOpts("rad-pub"))

	var want []string
	for i := 0; i < 300; i++ {
		want = append(want, fmt.Sprintf("d%03d", i))
	}
	cutOffAndPublish(t, proxy, subBroker, pub, "rad/t", 1, want...)

	// The read holds at its third message, inside the first fetch; the next fetch fails.
	held := []received{sub.expectMessage(), sub.expectMessage()}
	sub.expectNoMessage()
	// The connection drops and comes back while the read is held, so the read's failure is a reconnect's.
	proxy.drop()
	require.Eventually(t, func() bool { return !subBroker.NATS().IsConnected() }, 10*time.Second, 5*time.Millisecond)
	proxy.restore()
	require.Eventually(t, func() bool { return subBroker.NATS().IsConnected() }, 10*time.Second, 5*time.Millisecond)
	time.Sleep(time.Second) // the reconnect's own read of where to start has finished
	natsmqtt5.SetUnansweredQueueFetches(subBroker, 21)

	var got []string
	for _, m := range held {
		got = append(got, m.Payload)
		require.NoError(t, m.Ack())
	}
	for len(got) < len(want) {
		select {
		case m := <-sub.messages:
			got = append(got, m.Payload)
			require.NoError(t, m.Ack())
		case d := <-sub.disconnects:
			t.Fatalf("the broker closed the client: %v", d)
		case <-time.After(15 * time.Second):
			t.Fatalf("got %d of %d messages, then nothing", len(got), len(want))
		}
	}
	assert.Equal(t, want, got, "every message once and in order")
	sub.expectNoMessage()
}
