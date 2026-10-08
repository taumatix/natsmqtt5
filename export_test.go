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
