package natsmqtt5

import (
	"context"
	"sort"
	"time"

	"github.com/nats-io/nats.go"
)

// Seams for the external tests. They exist only in the test binary.

// SetSharedFetchHook makes every batch pull of a shared subscription's backlog
// call f(asked, got) until the test ends. Set it before the broker starts.
func SetSharedFetchHook(t interface{ Cleanup(func()) }, f func(asked, got int)) {
	sharedFetchHook = f
	t.Cleanup(func() { sharedFetchHook = nil })
}

// SetPubrecGate makes a connection call f(id) when it has read a PUBREC that
// accepts the QoS 2 message id, before anything records that the PUBREL is
// owed: the window in which a broker killed leaves a record that still names
// the PUBLISH. nil removes it.
func SetPubrecGate(b *Broker, f func(id uint16)) {
	if f == nil {
		b.pubrecGate.Store(nil)
		return
	}
	b.pubrecGate.Store(&f)
}

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

// SetSubackGate makes a SUBSCRIBE call f after its SUBACK is written and before the
// retained messages go out, the window in which a publisher that waited for the
// SUBACK can store a message. nil removes it.
func SetSubackGate(b *Broker, f func()) {
	if f == nil {
		b.subackGate.Store(nil)
		return
	}
	b.subackGate.Store(&f)
}

// SetPublishQueuedGate makes a publish call f with the message's payload after its
// offline-queue copy is stored and before its live publish: the window in which
// another publisher's copies can overtake this one. nil removes it.
func SetPublishQueuedGate(b *Broker, f func(payload []byte)) {
	if f == nil {
		b.publishQueuedGate.Store(nil)
		return
	}
	b.publishQueuedGate.Store(&f)
}

// AwaitNATSDisconnect waits until the broker's NATS client has noticed that its connection is gone.
func AwaitNATSDisconnect(b *Broker, within time.Duration) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if !b.nc.IsConnected() {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
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

// SetShareMemberSweepGrace changes how old a membership entry must be before the
// sweep may remove it for want of a session record, for the rest of the test.
func SetShareMemberSweepGrace(t interface {
	Cleanup(func())
}, d time.Duration) {
	was := shareMemberSweepGrace
	shareMemberSweepGrace = d
	t.Cleanup(func() { shareMemberSweepGrace = was })
}

// SweepShareMembers runs one pass of the membership sweep, which otherwise runs
// with the session sweep.
func SweepShareMembers(ctx context.Context, b *Broker) error { return b.sweepShareMembers(ctx) }

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

// SetBeforeGroupDelete makes a leaving session that found its shared
// subscription empty call fn before it deletes the group's consumer, until the
// test ends.
func SetBeforeGroupDelete(t interface {
	Cleanup(func())
}, fn func()) {
	beforeGroupDelete.Store(&fn)
	t.Cleanup(func() { beforeGroupDelete.Store(nil) })
}

// SetEmptyQueueFetches makes the next n fetches of b's offline-queue reads come
// back empty, as a loaded server's can.
func SetEmptyQueueFetches(b *Broker, n int) { b.queue.Load().emptyFetches.Store(int64(n)) }

// SetUnansweredQueueFetches makes the next n fetches of b's offline-queue reads fail with no
// responders, as a pull to a consumer just made on another NATS server does before its interest
// has reached the broker's server.
func SetUnansweredQueueFetches(b *Broker, n int) { b.queue.Load().unansweredFetches.Store(int64(n)) }

// SetQueueRetryEvery changes how soon the broker retries a default offline queue
// that failed at start-up, for the rest of the test. Set it before the broker starts.
func SetQueueRetryEvery(t interface {
	Cleanup(func())
}, first, max time.Duration) {
	wasFirst, wasMax := queueRetryEvery, queueRetryMax
	queueRetryEvery, queueRetryMax = first, max
	t.Cleanup(func() { queueRetryEvery, queueRetryMax = wasFirst, wasMax })
}

// HasSharedBacklog reports whether the session clientID, held by b, has a
// shared subscription with a backlog consumer.
func HasSharedBacklog(b *Broker, clientID string) bool {
	s := b.peekSession(clientID)
	if s == nil {
		return false
	}
	for _, sub := range s.subscriptions() {
		if sub.share != "" && sub.backlog() != nil {
			return true
		}
	}
	return false
}

// SetQueueRetryGate makes the broker's retry of a default queue that failed at
// start-up ask f first, and wait for its next turn while f says no.
func SetQueueRetryGate(b *Broker, f func() bool) { b.queueRetryGate.Store(&f) }

// SetStoredDeliveredBound changes how many delivered ids a session record keeps
// in all, chunks beside it included, for the rest of the test, so a test need
// not deliver thousands of messages to see what the bound costs.
func SetStoredDeliveredBound(t interface {
	Cleanup(func())
}, n int) {
	was, wasSpilled := storedDeliveredBound.Swap(int64(n)), spilledDeliveredBound.Swap(0)
	t.Cleanup(func() { storedDeliveredBound.Store(was); spilledDeliveredBound.Store(wasSpilled) })
}

// SetInlineDeliveredBound changes how many delivered ids a session record keeps
// in its value, for the rest of the test; the rest go to chunks beside it.
func SetInlineDeliveredBound(t interface {
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

// RetainedPlant puts an entry into the broker's retained view that the stream does not
// hold (seq names a stream sequence that does not exist), as a clear missed during a
// NATS outage leaves one.
func RetainedPlant(b *Broker, topic, payload string, seq uint64) {
	b.retain.mu.Lock()
	defer b.retain.mu.Unlock()
	b.retain.byTopic[topic] = &retained{topicName: topic, payload: []byte(payload), seq: seq, stored: time.Now()}
}

// RetainedForget removes a topic from the broker's retained view, as a message its
// consumer has not delivered yet is absent from it.
func RetainedForget(b *Broker, topic string) {
	b.retain.mu.Lock()
	defer b.retain.mu.Unlock()
	delete(b.retain.byTopic, topic)
}

// RetainedPayload is what the broker's retained view holds for topic.
func RetainedPayload(b *Broker, topic string) (string, bool) {
	b.retain.mu.RLock()
	defer b.retain.mu.RUnlock()
	r, ok := b.retain.byTopic[topic]
	if !ok {
		return "", false
	}
	return string(r.payload), true
}

// RetainedSweep runs the retained store's sweep as the broker would with the NATS
// client having reconnected that many times.
func RetainedSweep(b *Broker, reconnects uint64) { b.retain.sweep(reconnects) }

// SetLiveCopyGate makes a publish call f between storing its queue copy and
// publishing its live copy, and leave the live copy out when f returns false:
// the publisher was killed, or stalled, in between. f gets the live copy, which
// carries its queue sequence, so a test can publish it itself later. nil
// removes the gate.
func SetLiveCopyGate(b *Broker, f func(live *nats.Msg) bool) {
	if f == nil {
		b.liveCopyGate.Store(nil)
		return
	}
	b.liveCopyGate.Store(&f)
}

// CheckpointSkew is how far before a release a session record's delivered ids reach, beyond the
// replay rewind.
const CheckpointSkew = checkpointSkew

// SetCursorScanAfter changes how many delivered ids pile up above a session's
// replay cursor before a scan moves it, for the rest of the test.
func SetCursorScanAfter(t interface{ Cleanup(func()) }, n int) {
	was := cursorScanAfter.Swap(int64(n))
	t.Cleanup(func() { cursorScanAfter.Store(was) })
}

// SetMaxReplayHoles changes how many holes a replay cursor keeps before it is
// dropped, for the rest of the test.
func SetMaxReplayHoles(t interface{ Cleanup(func()) }, n int) {
	was := maxReplayHoles.Swap(int64(n))
	t.Cleanup(func() { maxReplayHoles.Store(was) })
}

// ReplayCursor is the replay cursor of a session this broker holds and its
// holes' sequences; 0 when it has none.
func ReplayCursor(b *Broker, clientID string) (next uint64, holes []uint64) {
	s := b.peekSession(clientID)
	if s == nil {
		return 0, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for seq := range s.cur.holes {
		holes = append(holes, seq)
	}
	sort.Slice(holes, func(i, j int) bool { return holes[i] < holes[j] })
	return s.cur.next, holes
}
