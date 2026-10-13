package natsmqtt5

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

// A SUBSCRIBE that replaces a subscription must not make a message in flight
// vanish [MQTT-3.8.4-4]: some subscription is live at every instant, so a
// message that resolves one has to find it. This runs the session's real
// installSubscription against concurrent resolutions, the way the NATS handler
// does, and counts the resolutions that found nothing.
func TestAMessageResolvedWhileASubscriptionIsReplacedFindsALiveOne(t *testing.T) {
	c := &conn{}
	s := &session{subs: map[string]*subscription{}, conn: c}
	first := &subscription{filter: "f", natsSubs: []*nats.Subscription{{}}}
	first.cell = new(atomic.Pointer[subscription])
	first.cell.Store(first)
	_, installed, rebind := s.installSubscription(c, first)
	require.True(t, installed && !rebind)
	cell := first.cell

	var stop atomic.Bool
	var lost, seen atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				seen.Add(1)
				if deliverable(cell) == nil {
					lost.Add(1)
				}
			}
		}()
	}
	for i := 0; i < 200000; i++ {
		_, ok, _ := s.installSubscription(c, &subscription{filter: "f"})
		require.True(t, ok)
	}
	stop.Store(true)
	wg.Wait()
	require.Positive(t, seen.Load())
	require.Zero(t, lost.Load(), "resolutions that found no live subscription during replacement")
}
