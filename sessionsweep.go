package natsmqtt5

import "time"

// DefaultSessionSweepInterval is how often a broker looks for detached sessions
// whose Session Expiry Interval has passed; see Options.SessionSweepInterval.
const DefaultSessionSweepInterval = time.Minute

// sweepSessionsLoop discards the sessions nobody came back for. A session whose
// client disconnected with a non-zero Session Expiry Interval keeps its NATS
// subscriptions until the client returns; without this, a workload that churns
// through Client Identifiers grows the broker's subscription set without bound.
func (b *Broker) sweepSessionsLoop(interval time.Duration) {
	defer b.wg.Done()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-b.shutdown:
			return
		case <-t.C:
			b.sweepSessions()
			b.retain.sweep()
		}
	}
}

// sweepSessions expires every detached session that is due and returns how many
// it expired. A session is due when its connection has been gone for longer than
// its Session Expiry Interval [MQTT-3.1.2-23] (the interval was capped by
// Options.MaxSessionExpiry when the client asked for it); a connected session,
// one with an interval of 0 and one never detached are not touched.
//
// Expiring tears down the session's NATS subscriptions, which also leaves a
// shared subscription's queue group, and drops the session from memory with the
// in-flight state, the replay position and the delivered ids it holds. A shared
// subscription's backlog consumer belongs to the whole group: the session
// leaves the group's members, and the consumer is deleted if it was the last
// (sharedmembers.go).
func (b *Broker) sweepSessions() int {
	b.mu.Lock()
	candidates := make([]*session, 0, len(b.sessions))
	for _, s := range b.sessions {
		candidates = append(candidates, s)
	}
	b.mu.Unlock()

	expired := 0
	for _, s := range candidates {
		subs, ok := s.expireIfDue()
		if !ok {
			continue
		}
		expired++
		s.handBackHeld()
		b.mu.Lock()
		if cur, held := b.sessions[s.clientID]; held && cur == s {
			delete(b.sessions, s.clientID)
		}
		b.mu.Unlock()
		for _, sub := range subs {
			unsubscribeAll(sub)
		}
		b.leaveGroups(s, subs)
		b.logger.Info("a detached session expired", "client_id", s.clientID, "subscriptions", len(subs))
		if hook := b.onSessionExpired.Load(); hook != nil {
			(*hook)(s, subs)
		}
	}
	return expired
}
