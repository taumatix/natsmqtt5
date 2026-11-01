package natsmqtt5_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5/v2/packet"
)

// Close returns while a catch-up read is held by a client that acknowledges nothing and NATS has dropped again.
func TestCloseReturnsWhileACatchUpReadIsHeldAndNATSIsDown(t *testing.T) {
	natsURL := startNATS(t)
	proxy := newDropProxy(t, natsURL)
	b, subAddr, _ := startBrokerHandle(t, proxy.url(), quickReconnect, withReconnectCatchUp)
	pubAddr := startBroker(t, natsURL)

	cp := connectOpts("cdh-sub")
	cp.Properties = &paho.ConnectProperties{ReceiveMaximum: packet.Uint16(2)}
	sub, _ := connectClient(t, subAddr, cp, manualAck)
	sub.subscribe(paho.SubscribeOptions{Topic: "cdh/t", QoS: 1})
	pub, _ := connectClient(t, pubAddr, connectOpts("cdh-pub"))

	var during []string
	for i := 0; i < 300; i++ {
		during = append(during, fmt.Sprintf("d%03d", i))
	}
	cutOffAndPublish(t, proxy, b, pub, "cdh/t", 1, during...)
	sub.expectMessage()
	sub.expectMessage()
	proxy.drop()
	require.Eventually(t, func() bool { return !b.NATS().IsConnected() }, 10*time.Second, 5*time.Millisecond)

	done := make(chan error, 1)
	go func() { done <- b.Close() }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(20 * time.Second):
		t.Fatal("Close did not return within 20s with a held catch-up read and NATS down")
	}
}

// Close returns while NATS is down and a client is connected, bounded by a deadline.
func TestCloseReturnsWhileNATSIsDown(t *testing.T) {
	natsURL := startNATS(t)
	proxy := newDropProxy(t, natsURL)
	b, addr, _ := startBrokerHandle(t, proxy.url(), quickReconnect, withReconnectCatchUp)
	sub, _ := connectClient(t, addr, connectOpts("cd-sub"), manualAck)
	sub.subscribe(paho.SubscribeOptions{Topic: "cd/t", QoS: 1})

	proxy.drop()
	require.Eventually(t, func() bool { return !b.NATS().IsConnected() }, 10*time.Second, 5*time.Millisecond)

	start := time.Now()
	done := make(chan error, 1)
	go func() { done <- b.Close() }()
	select {
	case err := <-done:
		require.NoError(t, err)
		t.Logf("Close took %s", time.Since(start))
	case <-time.After(20 * time.Second):
		t.Fatal("Close did not return within 20s with NATS down")
	}
}
