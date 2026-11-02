package natsmqtt5_test

import (
	"sync/atomic"
	"testing"
	"time"

	"errors"
	"github.com/taumatix/natsmqtt5"
)

// An outage of the Will bucket's writes shorter than twice WillCheckInterval,
// counted from when the writes began to fail, closes no client, wherever the
// outage falls between two checks. #220 found a leader election doing this to
// a broker at the default interval: the fence counted its twice-the-interval
// from the last check that succeeded, which is up to a check interval (half
// WillCheckInterval) before the failure, so the margin shrank by that much.
// The checks run every half interval; an outage of 3.5 of them is within the
// documented 4, and past the 4 the fence once counted from a check that
// succeeded just before it.
var errProbeDown = errors.New("fence probe down")

func TestAWillBucketOutageShorterThanTwiceTheCheckIntervalClosesNoClientWhereverItFalls(t *testing.T) {
	natsURL := startNATS(t)
	const every = 600 * time.Millisecond
	b, addr, _ := startBrokerHandle(t, natsURL, func(o *natsmqtt5.Options) {
		o.DurableWills = true
		o.WillCheckInterval = every
	})
	client, _ := connectClient(t, addr, durableWillConnect("fence-margin", "status/fence-margin", "gone", 0, 0))

	var down atomic.Bool
	natsmqtt5.SetFenceProbeFault(b, func() error {
		if down.Load() {
			return errProbeDown
		}
		return nil
	})
	outage := 7 * every / 4 // 3.5 checks, which the old margin of 4 counted from the last success did not hold
	// Starts 1.75 s apart move each outage 250 ms through the 300 ms between checks.
	for range 8 {
		down.Store(true)
		time.Sleep(outage)
		down.Store(false)
		time.Sleep(1750*time.Millisecond - outage)
		select {
		case <-client.Done():
			t.Fatal("an outage shorter than twice WillCheckInterval closed the client's connection")
		default:
		}
	}
}
