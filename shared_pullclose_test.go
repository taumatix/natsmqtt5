package natsmqtt5_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// Closing a broker stops the goroutines pulling a shared subscription's backlog
// and waits for them: nothing of a closed broker is still running, so nothing of
// it can touch another test's (or another caller's) state.
func TestClosingABrokerJoinsTheGoroutinesPullingASharedBacklog(t *testing.T) {
	natsURL := startNATS(t)
	b, addr, stop := startBrokerHandle(t, natsURL)
	var calls atomic.Int64
	natsmqtt5.SetSharedFetchHook(b, func(_, _ int) { calls.Add(1) })

	m := dialRaw(t, addr)
	m.connect(rawConnect("closer", 300))
	m.subscribe(group, packet.QoS1)
	require.Eventually(t, func() bool { return natsmqtt5.PullingGoroutines(b) > 0 }, 5*time.Second, time.Millisecond)

	stop()

	require.Zero(t, natsmqtt5.PullingGoroutines(b), "Close returned with a puller still running")
}
