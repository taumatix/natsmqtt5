package natsmqtt5

import (
	"context"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Catching up after the broker's NATS connection comes back
// (Options.ReconnectCatchUp).
//
// A broker without a NATS connection has no interest on the server, so a live
// copy published meanwhile reaches none of its sessions. Its queue copy is in
// the stream, stored before the live copy was published (offlineQueue.keep).
// So the drop holds every connected session's live QoS 1 and 2 copies back, as
// a catch-up turns them away (catchup.go), and the reconnect starts each
// session's catch-up at the first message stored OfflineQueueRewind before the
// drop (a live copy can still be on its way when the connection goes), or at
// the one after the newest the broker had seen, if lower (a server that died
// behind an open socket is noticed only when the pings fail). The catch-up
// starts no lower than the session can read without sending a message twice
// (session.catchUpFloor), and sends a subscription only what was stored after
// it was made (subscription.since). It skips what the session delivered by id, so the
// rewind sends nothing twice, and reads in stream order up to the copies held
// back, so nothing newer overtakes what was missed [MQTT-4.6.0-5].
//
// Nothing here changes a session that is away: its resume replays as before.

// natsWatch is the broker's record of its NATS connection being down.
type natsWatch struct {
	mu sync.Mutex
	// lostAt is when the connection dropped; zero while it is up or once a
	// reconnect has started the sessions' catch-up.
	lostAt time.Time
	// seenAt is the highest queue sequence the broker had stored or been
	// handed a live copy of when the connection dropped (offlineQueue.seen).
	// A dead server can go unnoticed until the pings fail, minutes later and
	// past the rewind; nothing above seenAt reached the broker live, so the
	// catch-up starts at or below the one after it too.
	seenAt uint64
	// gen counts drops, so a catch-up computed for one is not applied after
	// another.
	gen uint64
	// prevDisconnect and prevReconnect are the handlers the connection had
	// before the broker chained its own; they are called after it and put back
	// on Close.
	prevDisconnect nats.ConnErrHandler
	prevReconnect  nats.ConnHandler
}

// watchReconnects chains the broker's handlers onto its NATS connection's.
func (b *Broker) watchReconnects() {
	w := &b.natsWatch
	w.prevDisconnect, w.prevReconnect = b.nc.Opts.DisconnectedErrCB, b.nc.Opts.ReconnectedCB
	b.nc.SetDisconnectErrHandler(func(nc *nats.Conn, err error) {
		b.natsLost()
		if w.prevDisconnect != nil {
			w.prevDisconnect(nc, err)
		}
	})
	b.nc.SetReconnectHandler(func(nc *nats.Conn) {
		b.natsBack()
		if w.prevReconnect != nil {
			w.prevReconnect(nc)
		}
	})
}

// unwatchReconnects puts back the handlers watchReconnects replaced, for a
// connection the caller passed in and keeps.
func (b *Broker) unwatchReconnects() {
	if !b.opts.ReconnectCatchUp || b.ownsNC {
		return
	}
	b.nc.SetDisconnectErrHandler(b.natsWatch.prevDisconnect)
	b.nc.SetReconnectHandler(b.natsWatch.prevReconnect)
}

// connectedConns is every connection currently serving a session.
func (b *Broker) connectedConns() []*conn {
	b.mu.Lock()
	sessions := make([]*session, 0, len(b.sessions))
	for _, s := range b.sessions {
		sessions = append(sessions, s)
	}
	b.mu.Unlock()
	conns := make([]*conn, 0, len(sessions))
	for _, s := range sessions {
		if c := s.currentConn(); c != nil {
			conns = append(conns, c)
		}
	}
	return conns
}

// natsLost runs when the NATS connection drops: every connected session holds
// its live copies back until the reconnect says where its catch-up starts.
func (b *Broker) natsLost() {
	q := b.queue.Load()
	if q == nil {
		// No live copy has a queue sequence, so there is nothing to hold.
		return
	}
	w := &b.natsWatch
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.lostAt.IsZero() {
		w.lostAt, w.seenAt = time.Now(), q.seen.Load()
	}
	w.gen++
	for _, c := range b.connectedConns() {
		c.holdLive()
	}
}

// natsBack runs when the NATS connection is back. Finding where the catch-up
// starts reads the stream, which the handler must not wait for.
func (b *Broker) natsBack() {
	w := &b.natsWatch
	w.mu.Lock()
	at, seen, gen := w.lostAt, w.seenAt, w.gen
	w.mu.Unlock()
	if at.IsZero() {
		return
	}
	go b.catchUpAfterReconnect(at, seen, gen)
}

// catchUpAfterReconnect finds the first queue sequence stored a rewind before
// the drop, or the one after seen if that is lower, and starts every connected
// session's catch-up there. It tries until it can read the stream (a cluster
// may still be electing its leader), the broker shuts down, or the connection
// drops again, whose reconnect takes over.
func (b *Broker) catchUpAfterReconnect(lostAt time.Time, seen, gen uint64) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-b.shutdown:
			cancel()
		case <-ctx.Done():
		}
	}()
	from, wait := uint64(0), 100*time.Millisecond
	for {
		q := b.queue.Load()
		if q == nil {
			break
		}
		seq, err := q.firstSeqSince(ctx, lostAt.Add(-q.window()))
		if err == nil {
			from = seq
			if seen != 0 && seen+1 < from {
				from = seen + 1
			}
			break
		}
		if b.natsWatch.dropped(gen) {
			return
		}
		select {
		case <-b.shutdown:
			return
		case <-time.After(wait):
		}
		b.logger.Warn("could not read the offline queue after reconnecting to NATS; retrying", "error", err)
		wait = min(2*wait, 2*time.Second)
	}
	w := &b.natsWatch
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.gen != gen {
		return
	}
	w.lostAt = time.Time{}
	conns := b.connectedConns()
	for _, c := range conns {
		c.releaseLive(from)
	}
	b.logger.Info("reconnected to NATS; connected sessions catch up from the offline queue",
		"sessions", len(conns), "from_seq", from, "down_for", time.Since(lostAt).Round(time.Millisecond))
}

// dropped reports whether the connection dropped again since drop gen.
func (w *natsWatch) dropped(gen uint64) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.gen != gen
}

// firstSeqReadTimeout bounds one try at reading where a catch-up starts: a
// request sent as the connection dropped again is never answered.
const firstSeqReadTimeout = 3 * time.Second

// firstSeqSince is the sequence of the first message the queue stored at or
// after t, or the one after the stream's end when there is none.
func (q *offlineQueue) firstSeqSince(parent context.Context, t time.Time) (uint64, error) {
	ctx, cancel := context.WithTimeout(parent, firstSeqReadTimeout)
	defer cancel()
	stop, stopRead := context.WithCancel(ctx)
	defer stopRead()
	var first uint64
	last, err := q.replayFrom(stop, jetstream.OrderedConsumerConfig{
		DeliverPolicy: jetstream.DeliverByStartTimePolicy,
		OptStartTime:  &t,
	}, func(msg *nats.Msg) {
		if first == 0 {
			first = queueSeq(msg)
			stopRead()
		}
	})
	switch {
	case first != 0:
		return first, nil
	case err != nil:
		return 0, err
	}
	return last + 1, nil
}

// holdLive stops the connection handing its client live QoS 1 and 2 copies:
// they are noted, as a catch-up notes them, and read from the stream once
// releaseLive says where to start.
func (c *conn) holdLive() {
	c.catchMu.Lock()
	defer c.catchMu.Unlock()
	c.held, c.behind = true, true
}

// releaseLive starts the connection's catch-up at from, or at the lowest
// sequence the session can be read from without sending a delivered message
// twice, whichever is higher (session.catchUpFloor). from 0 only lets go.
func (c *conn) releaseLive(from uint64) {
	if from != 0 {
		from = c.sess.catchUpFloor(from)
	}
	c.catchMu.Lock()
	defer c.catchMu.Unlock()
	c.held = false
	if from != 0 && c.broker.queue.Load() != nil {
		c.behind = true
		c.noteOwedLocked(from)
		// Read from before the drop: a message stored before one of the
		// session's subscriptions was made is not owed to it.
		c.owedOnly = true
	} else if c.fromSeq == 0 {
		c.behind = false
	}
	if c.behind {
		select {
		case c.catchup <- struct{}{}:
		default:
		}
	}
}

// catchUpFloor is from, raised to the lowest queue sequence a read may start
// at without sending a delivered message again [MQTT-4.3.3-2]: the replay
// cursor, which keeps every id above it, or deliveredSeqWindow below the
// highest delivered sequence (session.lateSeqSafeLocked's rule).
func (s *session) catchUpFloor(from uint64) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cur.next != 0 {
		return max(from, s.cur.next)
	}
	if s.maxDeliveredSeq > deliveredSeqWindow {
		return max(from, s.maxDeliveredSeq-deliveredSeqWindow)
	}
	return from
}
