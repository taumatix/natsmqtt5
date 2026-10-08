package natsmqtt5

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A live copy that reaches a connection after it has ended, and before the
// session has detached it, is owed to the client like any other [MQTT-4.4.0-1]:
// the replay that follows must start at or before it. The connection's queue
// and its done channel are both ready then, and a select between them picks at
// random, so half of these used to vanish without a trace; a later message that
// did reach the queue then set the replay's start above them, and the
// time-based replay that would have covered them never ran (found on CI and,
// as a lost message, by the stress test in offline_test.go).
func TestALiveCopyArrivingAtAnEndedConnectionIsStillOwedByTheReplay(t *testing.T) {
	for i := 0; i < 64; i++ {
		c := &conn{deliveries: make(chan *delivery, 8), done: make(chan struct{}), loopDone: make(chan struct{})}
		c.closeOnce.Do(func() { close(c.done) })
		close(c.loopDone)
		c.loopStarted.Store(true)

		c.enqueueQueued(&delivery{seq: 630})

		require.Equal(t, uint64(630), c.awayFloor().fromSeq,
			"attempt %d: the replay would start after a message that arrived as the connection ended", i)
	}
}
