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
