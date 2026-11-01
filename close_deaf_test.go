package natsmqtt5_test

import (
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/require"
)

// Broker.Close returns when a client never reads its socket. Its sender sits inside Write holding the
// connection's write lock once the kernel buffers are full, and the DISCONNECT Close sends waited for
// that lock for ever (found against a cluster, 2026-11-01); the DISCONNECT now has a write deadline.
func TestCloseReturnsWhileAClientThatNeverReadsHasAFullSocket(t *testing.T) {
	t.Parallel()
	b, addr, _ := startBrokerHandle(t, startNATS(t))
	deaf := dialRaw(t, addr)
	deaf.connect(rawConnect("deaf-close", 0))
	deaf.subscribe("deaf/t", 0)

	pub, _ := connectClient(t, addr, connectOpts("deaf-close-pub"))
	payload := make([]byte, 64<<10)
	for range 400 { // far past what the socket buffers hold
		pub.publish(&paho.Publish{Topic: "deaf/t", QoS: 0, Payload: payload})
	}

	done := make(chan error, 1)
	go func() { done <- b.Close() }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(20 * time.Second):
		t.Fatal("Broker.Close did not return with a client that never reads")
	}
}
