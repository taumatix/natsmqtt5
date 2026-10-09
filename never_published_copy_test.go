package natsmqtt5_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/taumatix/natsmqtt5"
)

// A queue copy is stored before its live copy is published, so a publisher
// that stops between the two (killed, or stalled past the session's release)
// leaves a copy no broker is ever told about. The session was owed it. The
// replay finds it only if it starts early enough: a start at the lowest
// sequence the connection knew it owed is above it.
//
//	[MQTT-4.4.0-1] "both the Client and Server MUST resend any unacknowledged
//	  PUBLISH packets" for a resumed session.
//	[MQTT-3.1.2-23] what a session was owed is Session State.

func rewindOf(d time.Duration) func(*natsmqtt5.Options) {
	return func(o *natsmqtt5.Options) { o.OfflineQueueRewind = d }
}

// The copy was stored 3 s before the connection ended and its live copy never
// came; a rewind of 5 s reaches it, on the broker that held the session.
func TestACopyWhoseLiveCopyNeverCameIsReplayedWithinTheRewind(t *testing.T) {
	natsURL := startNATS(t)
	_, addr, _ := startBrokerHandle(t, natsURL, persistentWithQueue, rewindOf(5*time.Second))
	f := newLateCopyFixture(t, natsURL, addr, "np-mem")

	// "later" is delivered live; "earlier" is stored below it and never published.
	f.publishLive(t, f.later)
	time.Sleep(3 * time.Second)
	f.c.drop()
	time.Sleep(500 * time.Millisecond)

	assert.Equal(t, []string{"seed", "earlier", "later"}, resumeAndRead(t, addr, "np-mem"),
		"[MQTT-4.4.0-1] a copy stored within the rewind is replayed though no live copy came")
}

// The same when another broker claims the session from its record.
func TestACopyWhoseLiveCopyNeverCameIsReplayedByTheBrokerThatClaimsTheSession(t *testing.T) {
	natsURL := startNATS(t)
	_, addrA, _ := startBrokerHandle(t, natsURL, persistentWithQueue, rewindOf(5*time.Second))
	addrB := startBroker(t, natsURL, persistentWithQueue, rewindOf(5*time.Second))
	f := newLateCopyFixture(t, natsURL, addrA, "np-restored")

	f.publishLive(t, f.later)
	time.Sleep(3 * time.Second)
	f.c.drop()
	time.Sleep(500 * time.Millisecond)

	assert.Equal(t, []string{"seed", "earlier", "later"}, resumeAndRead(t, addrB, "np-restored"),
		"[MQTT-4.4.0-1] the record says how far back its delivered ids reach, so the claim rewinds")
}

// A copy stored longer ago than the rewind is outside what it promises, and a
// replay that went that far back would send what the connection delivered
// again.
func TestARewindDoesNotReachBeyondItsWindow(t *testing.T) {
	natsURL := startNATS(t)
	_, addr, _ := startBrokerHandle(t, natsURL, persistentWithQueue, rewindOf(time.Second))
	f := newLateCopyFixture(t, natsURL, addr, "np-short")

	f.publishLive(t, f.later)
	time.Sleep(3 * time.Second)
	f.c.drop()
	time.Sleep(500 * time.Millisecond)

	assert.Equal(t, []string{"seed", "later"}, resumeAndRead(t, addr, "np-short"))
}
