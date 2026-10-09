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
//
// The record also gets the in-flight state: the messages sent and not
// acknowledged, which the successor resends with their identifiers
// [MQTT-4.4.0-1], and the QoS 2 identifiers received and not released, which it
// holds so that a resent PUBLISH is not forwarded twice [MQTT-4.3.3-10]. Those
// two are written at once, not on the tick, when the broker is about to promise
// something about them (checkpointNow). So is the state of a QoS 2 message sent
// to the client: before its PUBLISH goes out (deliver) and before its PUBREL does
// (handlePubrec), so that a successor resends that packet and never delivers the
// message again as a new one [MQTT-4.3.3-6]. A QoS 1 message acknowledged in the
// last interval is still sent again: the broker cannot record a PUBACK before
// it has received it.

// checkpointSkew is how far before the moment of a checkpoint its replay time
// reaches. A message stored in the queue just before the checkpoint can still
// be on its way to this broker as a live copy, owed to the client and neither
// delivered nor waiting; replaying from the checkpoint time exactly would lose
// it if the broker died in that moment. The ids delivered in the skew are
// written with the position, so the replay does not repeat them.
const checkpointSkew = 500 * time.Millisecond

// checkpointPositionWait bounds how long checkpointNow waits for a resumed
// connection's replay position to be taken.
const checkpointPositionWait = 2 * time.Second

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
// next one is skipped when nothing has moved. It belongs to the connection and
// is guarded by the session's persistMu.
type checkpointMark struct {
	activity uint64
	fromSeq  uint64
	written  bool
}

// checkpointLoop writes the connection's replay position and in-flight state to
// the session record every interval while they change, until the connection
// ends.
func (c *conn) checkpointLoop(interval time.Duration) {
	select {
	case <-c.positioned:
	case <-c.done:
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-t.C:
		}
		if err := c.broker.checkpointSession(c); errors.Is(err, errLostSession) {
			return
		}
	}
}

// checkpointNow writes the checkpoint before an acknowledgement the client acts
// on, for the state that must not be older than what the client was told: the
// QoS 2 identifiers received (before the PUBREC that promises not to forward
// the message again [MQTT-4.3.3-10]) and released (before the PUBCOMP after
// which the identifier is a new message again [MQTT-4.3.3-12]). A failed write
// is logged by checkpointSession and does not stop the acknowledgement: the
// session is served as one without persistence would be.
func (c *conn) checkpointNow() {
	if c.broker.opts.SessionCheckpointInterval <= 0 {
		return
	}
	c.activity.Add(1)
	// A write before the resumed position is taken would replace it, so wait for
	// that, which is quick: it follows the predecessor's end.
	select {
	case <-c.positioned:
		_ = c.broker.checkpointSession(c)
	case <-c.done:
	case <-time.After(checkpointPositionWait):
		c.logger.Warn("the session was not checkpointed before an acknowledgement: "+
			"its replay position has not been taken", "client_id", c.sess.clientID)
	}
}

// checkpointSession writes the replay position and the in-flight state of c's
// session into its record, unless nothing has moved since the last write. The
// record stays Attached and owned: only the state changes. It fails with
// errLostSession when another broker has claimed the record, which ends the
// connection's checkpointing.
func (b *Broker) checkpointSession(c *conn) error {
	s := c.sess
	if b.store == nil || s.expiry() == 0 {
		return nil
	}
	s.persistMu.Lock()
	defer s.persistMu.Unlock()
	if c.isClosed() {
		// The release has run or is about to, and writes the final state.
		return nil
	}

	mark := &c.cpMark
	activity := c.activity.Load()
	pos := c.livePosition()
	if mark.written && activity == mark.activity && pos.fromSeq == mark.fromSeq && !s.blobsStale(b.store) {
		return nil
	}

	rec, rev := s.snapshot(c.claimGen)
	if rec == nil {
		return nil
	}
	if b.queue != nil {
		// With the queue off there is no replay, and nothing to say where it
		// starts.
		rec.AwayAt, rec.AwayFromSeq, rec.AwayLateSeq = pos.at, pos.fromSeq, 0
		var dropped int
		rec.Delivered, dropped = s.deliveredSince(pos.fromSeq, 0, pos.at)
		if dropped > 0 {
			b.logger.Warn("the session record keeps only some of the delivered message ids; "+
				"a restored replay may repeat the rest", "client_id", s.clientID, "dropped", dropped)
		}
	}
	// What the client has not acknowledged and what it sent that is not yet
	// released, for a successor that finds this broker dead [MQTT-4.4.0-1],
	// [MQTT-4.3.3-10]. The release logs what it cannot keep; a checkpoint every
	// interval would repeat it, so it does not.
	var pending []pendingBlob
	rec.Inflight, rec.ReceivedQoS2, _, pending = s.inflightState(b.store)
	notStored, err := s.stashBlobs(b.store, rec, pending)
	if err != nil {
		b.logger.Warn("could not store the payload of an unacknowledged message beside the session record",
			"client_id", s.clientID, "error", err)
	}
	spillStart := time.Now()
	if _, err := s.spillInto(b.store, rec); err != nil {
		b.logger.Warn("could not store unacknowledged messages beside the session record",
			"client_id", s.clientID, "error", err)
	}
	fitRecord(rec, b.store.valueLimit())

	ctx, cancel := context.WithTimeout(context.Background(), sessionOpTimeout)
	defer cancel()
	newRev, err := b.store.save(ctx, rec, rev)
	if err != nil {
		if errors.Is(err, errLostSession) {
			b.logger.Debug("could not checkpoint the stored session", "client_id", s.clientID, "error", err)
		} else {
			b.logger.Warn("could not checkpoint the stored session", "client_id", s.clientID, "error", err)
		}
		return err
	}
	s.commitRecord(c.claimGen, rec, newRev)
	s.commitSpill(b.store, rec, spillStart)
	s.reapBlobs(b.store)
	if notStored > 0 {
		// The record is written without them; the next tick tries again rather
		// than treating the state as recorded.
		*mark = checkpointMark{}
		return nil
	}
	*mark = checkpointMark{activity: activity, fromSeq: pos.fromSeq, written: true}
	return nil
}
