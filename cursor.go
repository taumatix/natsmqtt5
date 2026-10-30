package natsmqtt5

import (
	"context"
	"math"
	"sort"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/taumatix/natsmqtt5/packet"
	"github.com/taumatix/natsmqtt5/topic"
)

// The replay cursor.
//
// A replay used to start at a time (Options.OfflineQueueRewind before the
// connection ended) or at the lowest sequence the connection knew it owed, and
// so lost a message whose live copy never reached the connection, or reached it
// late, once that message was older than the rewind: nothing said the session
// was owed it. The cursor says so by reading the stream rather than waiting to
// be told.
//
// A session's cursor is a sequence, next, and a short list of holes below it.
// Two invariants make a replay that starts there exactly once:
//
//   - Nothing owed is skipped: every message below next that the session was
//     owed is delivered, or is a hole. A hole is a message (sequence and id)
//     the session was owed and has not been sent.
//   - Nothing delivered is repeated: every message at or above next that was
//     delivered has its id in session.delivered, which keeps them while the
//     cursor is set (noteDelivered), and a hole is removed the moment its
//     message is delivered.
//
// A replay then reads from the lowest hole, or next, and sends the holes and
// whatever at or above next it has no id for [MQTT-4.4.0-1], [MQTT-4.3.3-2].
//
// next only moves forward, and only past what the stream has been read through:
//
//   - A scan (scanCursor) reads the session's subjects from next to the stream's
//     end, without delivering, once enough ids have piled up above next. A
//     message the session's subscriptions owe it (matching, at QoS 1 or 2, and
//     stored after the subscription was made: subscription.since) that it has
//     no id for becomes a hole. next stops at the lowest message the connection
//     holds and has not sent (owedFloor), so a live copy waiting in the
//     connection is never passed.
//   - A replay that read the stream to its end moves next there.
//   - A session that subscribes with no other non-shared subscription starts
//     its cursor at the stream's end.
//
// A live copy that arrives below next, which the scan cannot have seen in the
// connection, is made a hole (noteLiveCopy). The two are ordered by cursorMu:
// next moves, and the connection's holdings are read, under it, and a live copy
// checks next under it after it has been placed in the connection.
//
// Releasing the cursor is conservative. It is dropped, and the session falls
// back to the time rewind, when the ids above it or the holes outgrow their
// bound, when the stream was reset under it, or when a record cannot carry all
// of the ids above it.

// sinceUnknown marks a subscription whose since has not been read yet.
const sinceUnknown = math.MaxUint64

// cursorScanAfter is how many ids may pile up above next before a scan runs.
var cursorScanAfter atomic.Int64

// cursorIDBound is how many ids above the cursor a session keeps before it
// drops the cursor. A record keeps at most maxStoredDelivered ids, so a cursor
// with more than that cannot be handed to another broker anyway.
var cursorIDBound atomic.Int64

// maxReplayHoles bounds the holes a cursor keeps. Each is a message the session
// is owed and has not been sent; a connection that leaves more than this behind
// falls back to the time rewind.
var maxReplayHoles atomic.Int64

func init() {
	cursorScanAfter.Store(1024)
	cursorIDBound.Store(4 * maxStoredDelivered)
	maxReplayHoles.Store(1024)
}

// replayCursor is a session's cursor. It is guarded by session.mu, and next is
// only changed with session.cursorMu held as well.
type replayCursor struct {
	// next is the lowest sequence not yet accounted for; 0 when the session has
	// no cursor.
	next uint64
	// holes maps the sequence of each message below next that the session is
	// owed and has not been sent to its id; holeAt is the reverse.
	holes  map[uint64]string
	holeAt map[string]uint64
	// gen changes whenever the cursor is set or dropped other than by a scan
	// that started from it, so a scan that raced one throws its result away.
	gen uint64
	// above counts ids noted at or above next since the last scan.
	above int
	// scanning is set while a scan runs.
	scanning bool
	// pin keeps the ids at or above it while a replay that may start a cursor
	// runs; 0 when there is none.
	pin uint64
}

func (r *replayCursor) addHole(seq uint64, id string) {
	if r.holes == nil {
		r.holes = make(map[uint64]string)
		r.holeAt = make(map[string]uint64)
	}
	r.holes[seq] = id
	r.holeAt[id] = seq
}

func (r *replayCursor) removeHole(id string) {
	if seq, ok := r.holeAt[id]; ok {
		delete(r.holeAt, id)
		delete(r.holes, seq)
	}
}

// cursorView is a copy of a cursor, for a replay or a record.
type cursorView struct {
	next  uint64
	holes map[uint64]string
}

func (v cursorView) start() uint64 {
	start := v.next
	for seq := range v.holes {
		if seq < start {
			start = seq
		}
	}
	return start
}

func (v cursorView) stored() []storedDelivered {
	if len(v.holes) == 0 {
		return nil
	}
	out := make([]storedDelivered, 0, len(v.holes))
	for seq, id := range v.holes {
		out = append(out, storedDelivered{ID: id, Seq: seq})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out
}

func (s *session) cursorViewLocked() cursorView {
	v := cursorView{next: s.cur.next}
	if len(s.cur.holes) > 0 {
		v.holes = make(map[uint64]string, len(s.cur.holes))
		for seq, id := range s.cur.holes {
			v.holes[seq] = id
		}
	}
	return v
}

// setCursorLocked replaces the cursor. s.mu and s.cursorMu are held.
func (s *session) setCursorLocked(next uint64) {
	s.cur.next = next
	s.cur.holes, s.cur.holeAt = nil, nil
	s.cur.gen++
	s.cur.above = 0
	s.cur.pin = 0
}

// dropCursorLocked forgets the cursor: the session's replays start at a time
// again. A late copy noted under the cursor's rule, which the ids kept without
// it cannot make safe, is forgotten as the rule without it would have done.
// s.mu is held; next changes without s.cursorMu, which is safe in this
// direction: a live copy that reads the old next only notes a hole that the
// cleared map no longer holds.
func (s *session) dropCursorLocked() {
	s.cur.next = 0
	s.cur.holes, s.cur.holeAt = nil, nil
	s.cur.gen++
	s.cur.above = 0
	s.cur.pin = 0
	if s.lateSeq != 0 && !s.windowSafeLocked(s.lateSeq) {
		s.lateSeq = 0
	}
	if s.awayLateSeq != 0 && !s.windowSafeLocked(s.awayLateSeq) {
		s.awayLateSeq = 0
	}
}

// addHoleLocked records a message the session is owed below next, unless it was
// delivered. It drops the cursor when the holes outgrow their bound and reports
// whether the cursor is still set.
func (s *session) addHoleLocked(seq uint64, id string) bool {
	if id == "" {
		// A copy without an id cannot be told from a delivered one.
		s.dropCursorLocked()
		return false
	}
	if _, delivered := s.delivered[id]; delivered {
		return true
	}
	if _, ok := s.cur.holes[seq]; ok {
		return true
	}
	if int64(len(s.cur.holes)) >= maxReplayHoles.Load() {
		s.dropCursorLocked()
		return false
	}
	s.cur.addHole(seq, id)
	return true
}

// noteLiveCopy is told of each live copy of a queued message that reaches the
// session, after the copy has been placed where owedFloor finds it (or handed
// to lateCopy). One below next was not in the connection when the cursor passed
// it, and is a hole until it is delivered.
func (s *session) noteLiveCopy(seq uint64, id string) {
	if s == nil || seq == 0 {
		return
	}
	s.cursorMu.Lock()
	next := s.cur.next
	s.cursorMu.Unlock()
	if next == 0 || seq >= next {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cur.next != 0 && seq < s.cur.next {
		s.addHoleLocked(seq, id)
	}
}

// owedFloor is the lowest queue sequence this connection holds and has not
// sent: waiting, in hand, turned away, or the position of a stream read. 0 when
// there is none.
func (c *conn) owedFloor() uint64 {
	floor := lowestSeq(c.pending.min(), c.inHand.Load())
	c.catchMu.Lock()
	floor = lowestSeq(floor, c.fromSeq, c.streamNext.Load())
	c.catchMu.Unlock()
	return floor
}

// sinceKnown reports whether every non-shared subscription has its since. s.mu
// is held.
func (s *session) sinceKnownLocked() bool {
	for _, sub := range s.subs {
		if sub.share == "" && sub.since.Load() == sinceUnknown {
			return false
		}
	}
	return true
}

// scanned is a message a scan read that one of the session's subscriptions
// matches at QoS 1 or 2.
type scanned struct {
	seq uint64
	id  string
	// owed is true when a subscription that matches it was made before it was
	// stored.
	owed bool
}

// owedBy reports whether one of subs matches msg at QoS 1 or 2, and whether
// one that does was made before msg was stored. It follows deliverQueued.
func (c *conn) owedBy(subs []*subscription, msg *nats.Msg, seq uint64) (matched, owed bool) {
	qos, _, origin, _ := fromNATS(msg)
	if qos == packet.QoS0 {
		return false, false
	}
	subject, ok := topic.TrimPrefix(c.broker.opts.SubjectPrefix, msg.Subject)
	if !ok {
		return false, false
	}
	name := c.broker.codec.subjectToName(subject)
	for _, sub := range subs {
		if sub.share != "" || !topic.Match(sub.filter, name) {
			continue
		}
		if sub.opts.NoLocal && origin == c.sess.clientID {
			continue
		}
		if minQoS(qos, sub.grantedQoS) == packet.QoS0 {
			continue
		}
		matched = true
		if since := sub.since.Load(); since != sinceUnknown && seq > since {
			return true, true
		}
	}
	return matched, false
}

// scanCursor moves the session's cursor forward: it reads what the session's
// subscriptions match from next to the stream's end, without delivering, and
// accounts for each message. It runs on its own goroutine, one at a time per
// session, and changes nothing a client is sent. Ids noted while a scan runs
// do not start another, so it scans again while it moved the cursor and enough
// have piled up above it.
func (c *conn) scanCursor() {
	for c.scanOnce() && c.sess.rescanDue() {
	}
}

// rescanDue reports whether enough ids are above the cursor for another scan,
// and otherwise ends this one.
func (s *session) rescanDue() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cur.next != 0 && int64(s.cur.above) >= cursorScanAfter.Load() {
		return true
	}
	s.cur.scanning = false
	return false
}

// scanOnce is one scan. It reports whether it moved the cursor; when it did
// not, it has ended the scan.
func (c *conn) scanOnce() (moved bool) {
	s := c.sess
	defer func() {
		if !moved {
			s.mu.Lock()
			s.cur.scanning = false
			s.mu.Unlock()
		}
	}()
	q := c.broker.queue.Load()
	if q == nil || c.isClosed() || c.resume.Load() != nil {
		return false
	}
	s.mu.Lock()
	start, gen := s.cur.next, s.cur.gen
	known := s.sinceKnownLocked()
	s.mu.Unlock()
	if start == 0 || !known {
		return false
	}
	subs := s.subscriptions()
	ctx, cancel := c.streamContext()
	defer cancel()

	var read []scanned
	var last uint64
	var err error
	if filters := q.replayFilters(subs); filters != nil {
		last, err = q.replaySeq(ctx, start, filters, func(msg *nats.Msg) {
			seq := queueSeq(msg)
			if matched, owed := c.owedBy(subs, msg, seq); matched {
				read = append(read, scanned{seq: seq, id: messageID(msg), owed: owed})
			}
		})
	} else if !hasNonShared(subs) {
		// Nothing to read: no subscription the replay serves.
		last, err = q.lastSeq(ctx)
	} else {
		// A filter that cannot be narrowed to subjects: not worth a read of
		// the whole stream on every scan.
		return false
	}
	if err != nil {
		c.logger.Debug("could not scan the offline queue for the replay cursor", "error", err)
		return false
	}
	return c.applyScan(gen, start, last, read)
}

func hasNonShared(subs []*subscription) bool {
	for _, sub := range subs {
		if sub.share == "" {
			return true
		}
	}
	return false
}

// applyScan moves the cursor past what a scan from start read up to last,
// unless the cursor or the session's connection changed meanwhile. It reports
// whether the cursor moved.
func (c *conn) applyScan(gen, start, last uint64, read []scanned) (moved bool) {
	s := c.sess
	s.cursorMu.Lock()
	defer s.cursorMu.Unlock()
	floor := c.owedFloor()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cur.gen != gen || s.cur.next != start || s.conn != c || c.isClosed() {
		return false
	}
	if last+1 < start {
		// The stream ends below the cursor: it was purged or made again, and
		// the sequences the cursor names are not the ones it counted.
		c.logger.Warn("the offline queue restarted below the session's replay cursor; "+
			"the session falls back to the time rewind", "client_id", s.clientID)
		s.dropCursorLocked()
		return false
	}
	if !s.sinceKnownLocked() {
		return false
	}
	next := last + 1
	if floor != 0 && floor < next {
		next = max(floor, start)
	}
	for _, m := range read {
		if m.seq >= next {
			break
		}
		if !m.owed {
			continue
		}
		if !s.addHoleLocked(m.seq, m.id) {
			c.logger.Warn("the session is owed more messages than its replay cursor keeps; "+
				"it falls back to the time rewind", "client_id", s.clientID)
			return false
		}
	}
	moved = next > s.cur.next
	s.cur.next = next
	s.cur.above = s.countAboveLocked(next)
	return moved
}

// countAboveLocked counts the delivered ids at or above seq.
func (s *session) countAboveLocked(seq uint64) int {
	n := 0
	for _, m := range s.delivered {
		if m.seq >= seq {
			n++
		}
	}
	return n
}

// cursorForReplay is the cursor a resume replay starts from, and false when
// the session has none.
func (s *session) cursorForReplay() (cursorView, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cur.next == 0 {
		return cursorView{}, false
	}
	return s.cursorViewLocked(), true
}

// dropCursor forgets the cursor; see dropCursorLocked.
func (s *session) dropCursor() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dropCursorLocked()
}

// pinForReplay keeps the ids at or above seq while a replay without a cursor
// runs, so that one that reads the stream to its end can start a cursor.
func (s *session) pinForReplay(seq uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cur.next == 0 {
		s.cur.pin = max(seq, 1)
	}
}

// replayedTo moves the cursor once a resume replay has read the stream from
// start up to last and handled every message: everything in that span the
// session was owed has been sent. had is the cursor the replay started from,
// with ok false when it had none; its holes the replay passed are settled,
// sent or no longer owed.
func (c *conn) replayedTo(had cursorView, ok bool, start, last uint64) {
	s := c.sess
	s.cursorMu.Lock()
	defer s.cursorMu.Unlock()
	floor := c.owedFloor()
	s.mu.Lock()
	defer s.mu.Unlock()
	pinned := s.cur.pin
	s.cur.pin = 0
	if s.conn != c || c.isClosed() {
		return
	}
	for _, sub := range s.subs {
		// The replay sent what these owed up to last; past it, they are owed
		// what is stored.
		sub.since.CompareAndSwap(sinceUnknown, last)
	}
	next := last + 1
	if floor != 0 && floor < next {
		next = floor
	}
	if !ok {
		// Starting a cursor: the ids at or above next have to be all there,
		// which the pin has kept since the replay began at start.
		if s.cur.next != 0 || pinned == 0 || pinned > start || (floor != 0 && floor < start) {
			return
		}
		s.setCursorLocked(next)
		s.cur.above = s.countAboveLocked(next)
		return
	}
	if s.cur.next == 0 {
		return
	}
	for seq, id := range had.holes {
		if seq <= last && s.cur.holes[seq] == id {
			s.cur.removeHole(id)
		}
	}
	if next > s.cur.next {
		s.cur.next = next
		s.cur.above = s.countAboveLocked(next)
	}
}

// setSince records, for the subscriptions a SUBSCRIBE made, the stream's last
// sequence once their interest reached NATS (exact), or a sequence at or below
// it: anything stored after it reaches them live. A session that had no other
// non-shared subscription and holds nothing unsent starts its cursor there,
// when last is exact.
func (c *conn) setSince(subs []*subscription, last uint64, exact, hadNonShared bool) {
	s := c.sess
	s.cursorMu.Lock()
	defer s.cursorMu.Unlock()
	floor := c.owedFloor()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sub := range subs {
		sub.since.CompareAndSwap(sinceUnknown, last)
	}
	if exact && s.cur.next == 0 && !hadNonShared && floor == 0 && s.conn == c && s.sinceKnownLocked() {
		s.setCursorLocked(last + 1)
	}
}

// restoreCursorLocked sets the cursor a stored record names. s.mu is held.
func (s *session) restoreCursorLocked(next uint64, holes []storedDelivered) {
	if next == 0 {
		return
	}
	s.cur.next = next
	s.cur.holes, s.cur.holeAt = nil, nil
	s.cur.gen++
	for _, h := range holes {
		// A hole whose message the record also lists in flight was sent between
		// the two snapshots a write takes; the resend owes it, the replay must
		// not send it again as new [MQTT-4.3.3-2]. The in-flight messages are
		// noted delivered before this runs (restore.go).
		if _, sent := s.delivered[h.ID]; h.Seq < next && h.ID != "" && !sent {
			s.cur.addHole(h.Seq, h.ID)
		}
	}
}

// lastSeq is the stream's last sequence.
func (q *offlineQueue) lastSeq(ctx context.Context) (uint64, error) {
	q.infoMu.Lock()
	defer q.infoMu.Unlock()
	info, err := q.stream.Info(ctx)
	if err != nil {
		return 0, err
	}
	q.saw(info.State.LastSeq)
	return info.State.LastSeq, nil
}

// unpin ends a pinForReplay whose replay did not read to the stream's end.
func (s *session) unpin() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cur.pin = 0
}

// hasNonSharedSub reports whether the session holds a non-shared subscription.
func (s *session) hasNonSharedSub() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sub := range s.subs {
		if sub.share == "" {
			return true
		}
	}
	return false
}

// noteDelivered is session.noteDelivered for a message this connection sent,
// starting the scan it asks for.
func (c *conn) noteDelivered(id string, seq uint64) {
	if c.sess.noteDelivered(id, seq) {
		go c.scanCursor()
	}
}

// readSince sets since for the non-shared subscriptions a SUBSCRIBE made (one
// that replaced a subscription may have kept its predecessor's).
func (c *conn) readSince(ctx context.Context, granted []*subscription, hadNonShared bool) {
	q := c.broker.queue.Load()
	if q == nil {
		return
	}
	var fresh []*subscription
	for _, sub := range granted {
		if sub != nil && sub.share == "" && sub.since.Load() == sinceUnknown {
			fresh = append(fresh, sub)
		}
	}
	if len(fresh) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, sinceReadTimeout)
	defer cancel()
	last, err := q.lastSeq(ctx)
	if err != nil {
		// JetStream is slow or unreachable, which a SUBSCRIBE does not otherwise
		// depend on, so it is not held up. The highest sequence this broker has
		// seen is at or below the stream's end, so with it the subscription is
		// owed at least what it should be: a message stored in between and never
		// sent is a hole, never skipped.
		c.logger.Debug("could not read the offline queue's position for a new subscription; "+
			"using the highest sequence this broker has seen", "client_id", c.sess.clientID, "error", err)
		last = q.seen.Load()
	}
	c.setSince(fresh, last, err == nil, hadNonShared)
}

// sinceReadTimeout bounds the stream read a SUBSCRIBE makes for since.
const sinceReadTimeout = 250 * time.Millisecond

// saw records that the stream holds seq, for offlineQueue.seen.
func (q *offlineQueue) saw(seq uint64) {
	for {
		cur := q.seen.Load()
		if seq <= cur || q.seen.CompareAndSwap(cur, seq) {
			return
		}
	}
}

// keepsSince reports whether sub, replacing old on the same filter, is owed
// what old was: the same messages reached old at QoS 1 or 2, and none old
// skipped as the client's own is owed now. Otherwise sub is owed only what is
// stored after it, as a new subscription is.
func keepsSince(old, sub *subscription) bool {
	return sub.share == "" && old.share == "" && old.grantedQoS > packet.QoS0 &&
		(!old.opts.NoLocal || sub.opts.NoLocal) && old.since.Load() != sinceUnknown
}
