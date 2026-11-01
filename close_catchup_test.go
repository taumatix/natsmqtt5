package natsmqtt5_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
)

// Close waits for a catch-up after a NATS reconnect, which reads the offline
// queue and releases the connected sessions' held live copies: returning
// while it runs would close the queue and the connection under it.
func TestCloseWaitsForACatchUpAfterAReconnectThatIsStillRunning(t *testing.T) {
	natsURL := startNATS(t)
	b, _, _ := startBrokerHandle(t, natsURL, persistentWithQueue)

	started, release := make(chan struct{}), make(chan struct{})
	natsmqtt5.SetCatchUpHook(b, func() {
		close(started)
		<-release
	})
	natsmqtt5.NATSLostAndBack(b)
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("the catch-up never started")
	}

	closed := make(chan error, 1)
	go func() { closed <- b.Close() }()
	select {
	case <-closed:
		t.Fatal("Close returned while a catch-up was still running")
	case <-time.After(300 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return after the catch-up finished")
	}
}
