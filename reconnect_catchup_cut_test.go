package natsmqtt5_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5/packet"
)

// A catch-up read that a second NATS drop cuts in the middle (Options.ReconnectCatchUp) starts again after
// the drop rather than closing the client, and what the client was already sent is not sent twice
// [MQTT-4.3.2-2], [MQTT-4.6.0-5]. A read fetches the stream 256 messages at a time (offlineQueue.fetch), so
// a drop is only seen by a read between two fetches, or when a fetch is on the wire. The first drop loses
// 300 messages; its reconnect starts a read, and a client with a Receive Maximum of 2 that does not
// acknowledge holds it at its third message, inside the first fetch. The second drop comes then, and the
// client acknowledges while NATS is down, so the read goes on into its second fetch with no connection.
// Both drops are the proxy closing the real broker's real NATS connection; the client is a socket on the
// real broker. The read does not return an error here: nats.go's ordered consumer rides over a dropped
// connection (the server keeps the consumer), so what this proves is the end state, not catchUp's
// retryAfterDrop branch, which a read ends in only when the consumer is lost (the cluster test that
// restarts the subscriber's server).

// Every message reaches the client once, in order, on the connection it never lost.
func TestReconnectCatchUpReadCutBySecondDropStartsAgainWithoutLossOrRepeat(t *testing.T) {
	natsURL := startNATS(t)
	proxy := newDropProxy(t, natsURL)
	subBroker, subAddr, _ := startBrokerHandle(t, proxy.url(), quickReconnect, withReconnectCatchUp)
	pubAddr := startBroker(t, natsURL)

	cp := connectOpts("rcut-sub")
	cp.Properties = &paho.ConnectProperties{ReceiveMaximum: packet.Uint16(2)}
	sub, _ := connectClient(t, subAddr, cp, manualAck)
	sub.subscribe(paho.SubscribeOptions{Topic: "rc/cut", QoS: 1})
	pub, _ := connectClient(t, pubAddr, connectOpts("rcut-pub"))

	pub.publish(&paho.Publish{Topic: "rc/cut", QoS: 1, Payload: []byte("before")})
	got := []string{}
	next := func() {
		t.Helper()
		select {
		case m := <-sub.messages:
			got = append(got, m.Payload)
			require.NoError(t, m.Ack())
		case <-time.After(15 * time.Second):
			t.Fatalf("got %d messages, then nothing; last %q", len(got), got[max(0, len(got)-1):])
		}
	}
	next()

	var during []string
	for i := 0; i < 300; i++ {
		during = append(during, fmt.Sprintf("d%03d", i))
	}
	cutOffAndPublish(t, proxy, subBroker, pub, "rc/cut", 1, during...)

	// The read has sent two and waits for the client.
	held := []received{sub.expectMessage(), sub.expectMessage()}
	sub.expectNoMessage()

	// The second drop; the client acknowledges what it holds while NATS is down, and the read goes on.
	proxy.drop()
	require.Eventually(t, func() bool { return !subBroker.NATS().IsConnected() }, 10*time.Second, 5*time.Millisecond)
	for _, m := range held {
		got = append(got, m.Payload)
		require.NoError(t, m.Ack())
	}
	// The read goes on from its buffered fetch (256 messages) and then waits for the stream, which cannot
	// answer; the client acknowledges whatever it is sent meanwhile.
	for quiet := false; !quiet; {
		select {
		case m := <-sub.messages:
			got = append(got, m.Payload)
			require.NoError(t, m.Ack())
		case <-time.After(1500 * time.Millisecond):
			quiet = true
		}
	}
	require.Less(t, len(got), 1+300, "the read was cut by the drop with messages still unread")
	pub.publish(&paho.Publish{Topic: "rc/cut", QoS: 1, Payload: []byte("e1")})
	proxy.restore()
	require.Eventually(t, func() bool { return subBroker.NATS().IsConnected() }, 10*time.Second, 5*time.Millisecond)
	pub.publish(&paho.Publish{Topic: "rc/cut", QoS: 1, Payload: []byte("after")})

	want := append(append([]string{"before"}, during...), "e1", "after")
	for len(got) < len(want) {
		next()
	}
	assert.Equal(t, want, got, "every message once and in order across the cut read")
	sub.expectNoMessage()
	select {
	case d := <-sub.disconnects:
		t.Fatalf("the broker closed the client: %v", d)
	default:
	}
}
