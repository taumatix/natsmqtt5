package natsmqtt5

import (
	"context"
	"time"
)

// queueRetryEvery is how long the broker waits before trying again to create a
// default offline queue that could not be created at start-up; it doubles up to
// queueRetryMax. A var so that a test can shorten it.
var (
	queueRetryEvery = 5 * time.Second
	queueRetryMax   = time.Minute
)

// OfflineQueueActive reports whether the broker is keeping QoS 1 and 2 messages
// for disconnected sessions. It is false when the queue is off by option, and
// when the queue is on only by default and could not be created at start-up
// (the broker keeps trying, and the value turns true once it succeeds).
func (b *Broker) OfflineQueueActive() bool {
	return b.queue.Load() != nil
}

// startQueueLoops starts what depends on the queue existing: handing held
// shared messages back, and keeping shared-subscription membership alive. It is
// called once, when the queue first exists, from Open or from the retry.
func (b *Broker) startQueueLoops(q *offlineQueue) {
	b.wg.Add(1)
	// The interval is read here, on the caller's goroutine: read inside the
	// loop it races with a later test changing sharedAckWait.
	go b.holdLoop(sharedAckWait / 3)
	if q.members != nil {
		b.wg.Add(1)
		go b.refreshMembersLoop(q)
	}
}

// retryQueueLoop keeps trying to create the default offline queue that Open could
// not, so that the deviation is temporary. A session that comes and goes while
// the queue is off is not replayed anything published in that time, which is what
// the start-up warning already said.
func (b *Broker) retryQueueLoop(r *resolved) {
	defer b.wg.Done()
	wait := queueRetryEvery
	for {
		select {
		case <-b.shutdown:
			return
		case <-time.After(wait):
		}
		ctx, cancel := context.WithTimeout(context.Background(), natsFlushTimeout)
		q, err := newOfflineQueue(ctx, b.js, r)
		cancel()
		if err != nil {
			b.logger.Debug("the offline queue still cannot be created", "error", err)
			wait = min(wait*2, queueRetryMax)
			continue
		}
		b.queue.Store(q)
		b.logger.Info("the offline queue is now active: QoS 1 and 2 messages published while a session is " +
			"disconnected are kept for it from here on")
		b.startQueueLoops(q)
		b.giveBacklogs(q)
		return
	}
}

// giveBacklogs gives each shared subscription made while the queue was off what
// a SUBSCRIBE made now would have: an entry among its group's members and the
// group's backlog consumer, from which its connection pulls. Until then the
// subscription was a plain queue-group member: the group's backlog, which holds
// a member's share while it is away (MQTT-5.0 §4.8.2), and the last-member rule
// that ends the group did not count it. It runs once the queue is up, before
// any later SUBSCRIBE could read the queue as off; refreshMembers catches one
// that read it just before.
func (b *Broker) giveBacklogs(q *offlineQueue) {
	b.mu.Lock()
	sessions := make([]*session, 0, len(b.sessions))
	for _, s := range b.sessions {
		sessions = append(sessions, s)
	}
	b.mu.Unlock()
	for _, s := range sessions {
		for _, sub := range s.subscriptions() {
			if sub.share != "" && sub.backlog() == nil {
				b.giveBacklog(q, s, sub)
			}
		}
	}
}

// giveBacklog makes s, which holds the shared subscription sub, a member of its
// group with a backlog. The backlog starts at the queue's last sequence read
// once the consumer exists: a queued message above it is in the backlog, so its
// live copy is skipped as it is for any member; one at or below it was stored
// before and may not be, so its live copy is still delivered, which is what the
// subscription did until now. A failure is left to the next refresh.
func (b *Broker) giveBacklog(q *offlineQueue, s *session, sub *subscription) {
	if !sub.live.Load() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), natsFlushTimeout)
	defer cancel()
	cons, err := q.joinGroup(ctx, b.opts, s, sub)
	if err != nil {
		b.logger.Debug("could not give a shared subscription made while the queue was off its backlog",
			"client_id", s.clientID, "filter", sub.filter, "error", err)
		return
	}
	above, err := q.lastSeq(ctx)
	switch {
	case err != nil:
		b.logger.Debug("could not give a shared subscription made while the queue was off its backlog",
			"client_id", s.clientID, "filter", sub.filter, "error", err)
	case !sub.backlogAt.CompareAndSwap(nil, &sharedBacklog{cons: cons, above: above}):
		return // given meanwhile, by the other caller
	}
	if !sub.live.Load() {
		// Unsubscribed while this ran: the entry just written is not wanted.
		b.leaveGroup(s, sub.filter)
		return
	}
	if c := s.currentConn(); c != nil {
		c.startPull(sub) // nothing to pull when the backlog was not given
	}
}
