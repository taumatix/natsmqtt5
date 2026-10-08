package natsmqtt5

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
