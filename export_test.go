package natsmqtt5

import (
	"context"
	"time"
)

// Seams for the external tests. They exist only in the test binary.

// SetResendGate makes a connection's resend call f(id) after it has claimed
// the in-flight entry for id and before it writes the packet, which is the
// window in which an acknowledgement can complete an exchange that is about to
// be resent. nil removes it.
func SetResendGate(b *Broker, f func(id uint16)) {
	if f == nil {
		b.resendGate.Store(nil)
		return
	}
	b.resendGate.Store(&f)
}

// SetPublishFlushGate makes a durable publish call f between handing the live
// publish to the NATS client and flushing it, which is where the connection
// can be lost with the publish already written to it. nil removes it.
func SetPublishFlushGate(b *Broker, f func()) {
	if f == nil {
		b.publishFlushGate.Store(nil)
		return
	}
	b.publishFlushGate.Store(&f)
}

// AwaitNATSReconnect waits until the broker's NATS connection has reconnected
// at least once and is connected again.
func AwaitNATSReconnect(b *Broker, within time.Duration) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if b.nc.Stats().Reconnects >= 1 && b.nc.IsConnected() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

// SetResumeGate makes a resume call f(filter) for each stored filter after the
// Authorizer has passed it and before it is rebuilt, which is where a second
// CONNECT can take the session over. nil removes it.
func SetResumeGate(b *Broker, f func(filter string)) {
	if f == nil {
		b.resumeGate.Store(nil)
		return
	}
	b.resumeGate.Store(&f)
}

// SetSessionExpiredHook makes the session sweep call f with the Client
// Identifier and the number of subscriptions of every session it expires. nil
// removes it.
func SetSessionExpiredHook(b *Broker, f func(clientID string, subscriptions int)) {
	if f == nil {
		b.onSessionExpired.Store(nil)
		return
	}
	hook := func(s *session, subs []*subscription) { f(s.clientID, len(subs)) }
	b.onSessionExpired.Store(&hook)
}

// SessionCount is the number of sessions the broker holds in memory, connected
// or not.
func SessionCount(b *Broker) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.sessions)
}

// InflightCount is the number of unacknowledged messages the broker holds for
// clientID, for a test that must know an acknowledgement has been applied.
func InflightCount(b *Broker, clientID string) int {
	b.mu.Lock()
	s := b.sessions[clientID]
	b.mu.Unlock()
	if s == nil {
		return -1
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.inflight)
}

// LiveCopies is the number of live NATS copies the broker's subscription
// handlers have finished with, delivered to a connection or dropped.
func LiveCopies(b *Broker) int64 { return b.liveSeen.Load() }

// SetNextPacketID makes the next Packet Identifier the session for clientID
// allocates the one after id. It stands in for the 65535 allocations it would
// take to come round to an identifier again.
func SetNextPacketID(b *Broker, clientID string, id uint16) {
	b.mu.Lock()
	s := b.sessions[clientID]
	b.mu.Unlock()
	s.mu.Lock()
	s.nextPacketID = id
	s.mu.Unlock()
}

// Kill makes a broker vanish the way a killed process does: its NATS
// connection and every socket are closed under it, so none of its disconnect
// handling can reach NATS or JetStream, and the clients see their connection
// drop. The handlers still run in this process, but they find nothing to talk
// to, which is what a process that never ran them looks like from outside.
// TestWillOfAKilledProcess does it with a real SIGKILL for the one case this
// stands in for.
func Kill(b *Broker) {
	// Marked closed so Serve treats the dead listener as the end of the broker,
	// not as an accept failure; shutdown stays open, as nothing signals it in a
	// process that is simply gone.
	b.mu.Lock()
	b.closed = true
	b.mu.Unlock()
	b.nc.Close()
	if b.listener != nil {
		_ = b.listener.Close()
	}
	b.mu.Lock()
	conns := make([]*conn, 0, len(b.conns))
	for c := range b.conns {
		conns = append(conns, c)
	}
	b.mu.Unlock()
	for _, c := range conns {
		_ = c.nc.Close()
	}
}

// Reap ends the goroutines of a broker that Kill left running. Kill marks the
// broker closed, so Close returns at once and never signals shutdown; without
// this, a killed broker's refresh and sweep loops tick on into later tests.
func Reap(b *Broker) {
	b.mu.Lock()
	select {
	case <-b.shutdown:
	default:
		close(b.shutdown)
	}
	b.mu.Unlock()
	b.wg.Wait()
}

// SetSharedAckWait changes how long JetStream waits for a sign of life from the
// member holding a shared subscription's message, for the rest of the test. Set
// it before the broker starts.
func SetSharedAckWait(t interface {
	Cleanup(func())
}, d time.Duration) {
	was := sharedAckWait
	sharedAckWait = d
	t.Cleanup(func() { sharedAckWait = was })
}

// SetShareMemberSlack changes how long a shared subscription's membership entry
// outlives MaxSessionExpiry when nothing rewrites it, for the rest of the test.
// Set it before the broker starts.
func SetShareMemberSlack(t interface {
	Cleanup(func())
}, d time.Duration) {
	was := shareMemberSlack
	shareMemberSlack = d
	t.Cleanup(func() { shareMemberSlack = was })
}

// SweepStore runs one pass of the session store's sweep, which otherwise runs
// every sessionSweepInterval.
func SweepStore(ctx context.Context, b *Broker) error { return b.store.sweep(ctx) }

// SetBlobGrace shortens how old a payload must be before the sweep may call it
// an orphan.
func SetBlobGrace(b *Broker, d time.Duration) { b.store.blobGrace = d }

// BlobOwner is the key prefix payloads of clientID are kept under.
func BlobOwner(clientID string) string { return blobOwner(clientID) }

// RefreshMembers runs one pass of the shared-subscription membership refresh.
func RefreshMembers(b *Broker) { b.refreshMembers(b.queue.Load()) }

// SetBeforeMemberRefreshWrite runs fn between the refresh reading a session's
// subscription and writing its membership entry, for the rest of the test.
func SetBeforeMemberRefreshWrite(t interface {
	Cleanup(func())
}, fn func()) {
	beforeMemberRefreshWrite.Store(&fn)
	t.Cleanup(func() { beforeMemberRefreshWrite.Store(nil) })
}

// SetQueueRetryEvery changes how soon the broker retries a default offline queue
// that failed at start-up, for the rest of the test. Set it before the broker starts.
func SetQueueRetryEvery(t interface {
	Cleanup(func())
}, first, max time.Duration) {
	wasFirst, wasMax := queueRetryEvery, queueRetryMax
	queueRetryEvery, queueRetryMax = first, max
	t.Cleanup(func() { queueRetryEvery, queueRetryMax = wasFirst, wasMax })
}

// SetStoredDeliveredBound changes how many delivered ids a session record keeps,
// for the rest of the test, so a test need not deliver thousands of messages.
func SetStoredDeliveredBound(t interface {
	Cleanup(func())
}, n int) {
	was := storedDeliveredBound.Swap(int64(n))
	t.Cleanup(func() { storedDeliveredBound.Store(was) })
}

// SetAfterConnackWrite runs fn after a CONNACK has been written and before the
// connection counts as connected, for the rest of the test.
func SetAfterConnackWrite(t interface {
	Cleanup(func())
}, fn func()) {
	afterConnackWrite.Store(&fn)
	t.Cleanup(func() { afterConnackWrite.Store(nil) })
}

// SetWillSweepAfter shortens how long past its due time an ended Will record is kept before a
// broker deletes it unpublished. It is a no-op on a broker without Durable Wills.
func SetWillSweepAfter(b *Broker, d time.Duration) {
	if b.wills != nil {
		b.wills.sweepAfter.Store(int64(d))
	}
}
