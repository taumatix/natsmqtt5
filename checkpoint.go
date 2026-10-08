package natsmqtt5

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Checkpointing a connected session (Options.SessionCheckpointInterval).
//
// A session record is written when a connection ends (releaseStoredSession), and
// that is where the replay position a successor needs is put: the lowest
// offline-queue sequence the client was owed, and the ids delivered above it.
// A broker killed outright never gets there, so the record it leaves says
// Attached with no position, and the broker that claims it next has to assume
// the client is owed everything the queue still holds, up to
// OfflineQueueMaxAge. A client that was caught up then gets a day of repeats,
// QoS 2 included, which [MQTT-4.3.3-2] does not allow.
//
// So a live connection writes the position it would release with, while
// something moves, at most once an interval. The record then always says where
// the client stood within the last interval, and a successor of a dead broker
// replays from there. What a kill still costs is what the client was sent in
// that last interval (and checkpointSkew before it), which is sent again.

// checkpointSkew is how far before the moment of a checkpoint its replay time
// reaches. A message stored in the queue just before the checkpoint can still
// be on its way to this broker as a live copy, owed to the client and neither
// delivered nor waiting; replaying from the checkpoint time exactly would lose
// it if the broker died in that moment. The ids delivered in the skew are
// written with the position, so the replay does not repeat them.
const checkpointSkew = 500 * time.Millisecond

// seqSet counts queue sequences. A message that matches two subscriptions of a
// session is delivered twice, so one sequence can be in it twice.
type seqSet struct {
	mu sync.Mutex
	n  map[uint64]int
}

func (s *seqSet) add(seq uint64) {
	if seq == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.n == nil {
		s.n = make(map[uint64]int)
	}
	s.n[seq]++
}

func (s *seqSet) remove(seq uint64) {
	if seq == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.n[seq] <= 1 {
		delete(s.n, seq)
		return
	}
	s.n[seq]--
}

// min is the lowest sequence in the set, 0 when it is empty.
func (s *seqSet) min() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var lowest uint64
	for seq := range s.n {
		if lowest == 0 || seq < lowest {
			lowest = seq
		}
	}
	return lowest
}

// markPositioned says the replay position this connection was resumed with has
// been taken, so the checkpoint may start replacing it.
func (c *conn) markPositioned() {
	c.positionedOnce.Do(func() { close(c.positioned) })
}

// livePosition is awayFloor for a connection that has not ended: the lowest
// queue sequence the client is owed and has not been sent, or the time to
// replay from when there is none. It reads state the delivery goroutine
// changes, so it can be a little behind; every read is ordered against the
// moves a message makes (waiting, in hand, in flight) so that it is never
// ahead of a message the client is owed.
func (c *conn) livePosition() away {
	now := time.Now()
	a := away{at: now.Add(-checkpointSkew)}
	lowest := func(seq uint64) {
		if seq != 0 && (a.fromSeq == 0 || seq < a.fromSeq) {
			a.fromSeq = seq
		}
	}
	// A message moves from the queue of waiting deliveries to the one in hand,
	// so they are read in that order: it is in at least one of them.
	lowest(c.pending.min())
	lowest(c.inHand.Load())
	c.catchMu.Lock()
	lowest(c.fromSeq)
	next := c.streamNext.Load()
	lowest(next)
	c.catchMu.Unlock()
	if r := c.resume.Load(); r != nil && next == 0 {
		// The resume replay has not handled a message: what it was owed is still
		// owed, from the time it was to start at.
		pos := *r
		if !pos.restored {
			// A restored replay starts exactly at its time; one that was in memory
			// rewinds, and the record cannot say which ids were delivered in the
			// rewind.
			pos.at = pos.at.Add(-offlineRewind)
		}
		if pos.fromSeq == 0 || (a.fromSeq != 0 && a.fromSeq < pos.fromSeq) {
			pos.fromSeq = a.fromSeq
		}
		return pos
	}
	return a
}

// checkpointMark is what the last checkpoint written stood for, so that the
// next one is skipped when nothing has moved.
type checkpointMark struct {
	activity uint64
	fromSeq  uint64
	written  bool
}

// checkpointLoop writes the connection's replay position to the session record
// every interval while it changes, until the connection ends.
func (c *conn) checkpointLoop(interval time.Duration) {
	select {
	case <-c.positioned:
	case <-c.done:
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	var mark checkpointMark
	for {
		select {
		case <-c.done:
			return
		case <-t.C:
		}
		if err := c.broker.checkpointSession(c, &mark); errors.Is(err, errLostSession) {
			return
		}
	}
}

// checkpointSession writes the replay position of c's session into its record,
// unless nothing has moved since mark. The record stays Attached and owned: only
// the position changes. It fails with errLostSession when another broker has
// claimed the record, which ends the connection's checkpointing.
func (b *Broker) checkpointSession(c *conn, mark *checkpointMark) error {
	s := c.sess
	if b.store == nil || b.queue == nil || s.expiry() == 0 {
		return nil
	}
	s.persistMu.Lock()
	defer s.persistMu.Unlock()
	if c.isClosed() {
		// The release has run or is about to, and writes the final position.
		return nil
	}

	activity := c.activity.Load()
	pos := c.livePosition()
	// A message sent and not acknowledged is owed a resend a restored session
	// cannot make, so the replay starts at it and delivers it again.
	unacked := s.unackedQueueSeqs()
	for _, seq := range unacked {
		if pos.fromSeq == 0 || seq < pos.fromSeq {
			pos.fromSeq = seq
		}
	}
	if mark.written && activity == mark.activity && pos.fromSeq == mark.fromSeq {
		return nil
	}

	rec, rev := s.snapshot(c.claimGen)
	if rec == nil {
		return nil
	}
	rec.AwayAt, rec.AwayFromSeq = pos.at, pos.fromSeq
	var dropped int
	rec.Delivered, dropped = s.deliveredSince(pos.fromSeq, pos.at, unacked)
	if dropped > 0 {
		b.logger.Warn("the session record keeps only some of the delivered message ids; "+
			"a restored replay may repeat the rest", "client_id", s.clientID, "dropped", dropped)
	}

	ctx, cancel := context.WithTimeout(context.Background(), sessionOpTimeout)
	defer cancel()
	newRev, err := b.store.save(ctx, rec, rev)
	if err != nil {
		if errors.Is(err, errLostSession) {
			b.logger.Debug("could not checkpoint the stored session", "client_id", s.clientID, "error", err)
		}
		return err
	}
	s.commitRecord(c.claimGen, rec, newRev)
	*mark = checkpointMark{activity: activity, fromSeq: pos.fromSeq, written: true}
	return nil
}
