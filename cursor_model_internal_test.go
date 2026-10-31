package natsmqtt5

import (
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A randomized model of the replay cursor against what a resume owes.
//
// The model holds a stream of queued messages, every one owed to the session
// (one subscription, made before any of them), the live copies on their way
// (some never arrive), the connection's unsent messages, and what was
// delivered. Between those events it runs scans that read the stream and apply
// later, with other events in between: a scan of a cursor (applyScanLocked),
// and, once the cursor was dropped, a restart scan (restartCursorLocked). It
// drives the session's own code for everything the broker does, and at the end
// releases the session, lets some late copies reach it, and compares the replay
// the cursor would make with two references:
//
//   - a cursor that was never dropped sends every message owed and not
//     delivered: nothing is lost [MQTT-4.4.0-1];
//   - a cursor that was restarted sends at least what the time rewind would
//     (replayOffline's choice of start, by time or by sequence), so restarting
//     one never loses a message falling back would have sent.
//
// After every event it checks the invariant the replay rests on: every message
// in [base, next) not delivered is a hole, and the connection holds nothing
// below base.

const modelWindow = 100 * time.Second

type modelMsg struct {
	seq uint64
	id  string
	at  time.Time
}

type modelScan struct {
	restart bool
	r       restartScan
	start   uint64
}

type cursorModel struct {
	t       *testing.T
	rng     *rand.Rand
	s       *session
	logger  *slog.Logger
	clock   time.Time
	stream  []modelMsg
	transit map[uint64]bool
	pending map[uint64]bool
	done    map[uint64]bool
	scan    *modelScan
	dropped bool
	stats   *modelStats
}

type modelStats struct {
	restarts, refused, drops, belowBase int
}

func newCursorModel(t *testing.T, seed uint64, stats *modelStats) *cursorModel {
	s := newSession("model")
	s.rewind = modelWindow
	sub := &subscription{filter: "m/#"}
	sub.since.Store(0)
	s.subs[sub.filter] = sub
	s.cur.next = 1 // subscribed on an empty stream: setSince's cursor
	return &cursorModel{
		t: t, rng: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)), s: s,
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		clock:   time.Unix(1_000_000, 0),
		transit: map[uint64]bool{}, pending: map[uint64]bool{}, done: map[uint64]bool{},
		stats: stats,
	}
}

func (m *cursorModel) msg(seq uint64) modelMsg { return m.stream[seq-1] }

func (m *cursorModel) pick(set map[uint64]bool) (uint64, bool) {
	if len(set) == 0 {
		return 0, false
	}
	keys := make([]uint64, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	if m.rng.IntN(3) == 0 {
		return keys[0], true
	}
	return keys[m.rng.IntN(len(keys))], true
}

func (m *cursorModel) floor() uint64 {
	var f uint64
	for seq := range m.pending {
		f = lowestSeq(f, seq)
	}
	return f
}

func (m *cursorModel) step() {
	switch n := m.rng.IntN(100); {
	case n < 30: // publish: the queue copy is stored, the live copy is on its way or never comes
		m.clock = m.clock.Add(time.Duration(m.rng.IntN(int(modelWindow/time.Second)/4)) * time.Second)
		seq := uint64(len(m.stream) + 1)
		m.stream = append(m.stream, modelMsg{seq: seq, id: fmt.Sprintf("id%d", seq), at: m.clock})
		if m.rng.IntN(8) != 0 {
			m.transit[seq] = true
		}
	case n < 52: // a live copy reaches the connection, in any order
		if seq, ok := m.pick(m.transit); ok {
			delete(m.transit, seq)
			m.pending[seq] = true
			m.s.noteLiveCopy(seq, m.msg(seq).id)
		}
	case n < 76: // the connection sends one and the client completes it
		if seq, ok := m.pick(m.pending); ok {
			delete(m.pending, seq)
			m.done[seq] = true
			m.s.noteDelivered(m.msg(seq).id, seq)
			m.s.mu.Lock()
			m.s.cur.scanning = false // the model runs its own scans
			m.s.mu.Unlock()
		}
	case n < 79: // a bound is outgrown: the cursor goes
		m.s.dropCursor()
		m.dropped = true
		m.stats.drops++
	case n < 88:
		m.beginScan()
	case n < 97:
		m.applyScan()
	case n < 98: // a SUBSCRIBE replaces the subscription with one that keeps what it was owed
		m.s.mu.Lock()
		old := m.s.subs["m/#"]
		sub := &subscription{filter: old.filter}
		sub.since.Store(old.since.Load())
		m.s.subs["m/#"] = sub
		m.s.mu.Unlock()
	default: // time passes with nothing published, sometimes past a rewind
		m.clock = m.clock.Add(time.Duration(m.rng.IntN(int(modelWindow/time.Second)*2)) * time.Second)
	}
	m.checkInvariant()
}

func (m *cursorModel) beginScan() {
	if m.scan != nil {
		return
	}
	m.s.mu.Lock()
	next, gen := m.s.cur.next, m.s.cur.gen
	m.s.mu.Unlock()
	subs := m.s.subscriptions()
	last := uint64(len(m.stream))
	if next == 0 {
		r := restartScan{gen: gen, subs: subs, began: m.clock, last: last}
		since := m.clock.Add(-modelWindow)
		for _, msg := range m.stream {
			if msg.at.Before(since) {
				continue
			}
			if r.base == 0 {
				r.base = msg.seq
			}
			r.read = append(r.read, scanned{seq: msg.seq, id: msg.id, owed: true})
		}
		if r.base == 0 {
			r.base = last + 1
		}
		m.scan = &modelScan{restart: true, r: r}
		return
	}
	sc := &modelScan{start: next, r: restartScan{gen: gen, subs: subs, last: last}}
	for _, msg := range m.stream {
		if msg.seq >= next {
			sc.r.read = append(sc.r.read, scanned{seq: msg.seq, id: msg.id, owed: true})
		}
	}
	m.scan = sc
}

func (m *cursorModel) applyScan() {
	sc := m.scan
	if sc == nil {
		return
	}
	m.scan = nil
	s := m.s
	s.cursorMu.Lock()
	defer s.cursorMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if sc.restart {
		if s.restartCursorLocked(sc.r, m.floor(), m.clock) {
			m.stats.restarts++
		} else {
			m.stats.refused++
		}
		return
	}
	s.applyScanLocked(m.logger, sc.r.gen, sc.start, sc.r.last, sc.r.subs, sc.r.read, m.floor())
}

// checkInvariant: every message in [base, next) not delivered is a hole, and
// the connection holds nothing below base.
func (m *cursorModel) checkInvariant() {
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	next, base := m.s.cur.next, m.s.cur.base
	if next == 0 {
		return
	}
	for seq := base; seq < next && seq <= uint64(len(m.stream)); seq++ {
		if seq == 0 || m.done[seq] {
			continue
		}
		_, hole := m.s.cur.holes[seq]
		require.True(m.t, hole, "message %d in [%d, %d) is neither delivered nor a hole", seq, base, next)
	}
	for seq := range m.pending {
		require.GreaterOrEqual(m.t, seq, base, "the connection holds %d below the cursor's base", seq)
	}
}

// release ends the connection, lets some late copies reach the released
// session, and compares the replays.
func (m *cursorModel) release() {
	s := m.s
	s.mu.Lock()
	s.markAwayLocked(away{at: m.clock, fromSeq: m.floor()})
	s.mu.Unlock()
	for seq := range m.transit {
		if m.rng.IntN(2) == 0 {
			if seq < m.s.cur.base {
				m.stats.belowBase++
			}
			s.lateCopy(nil, seq, m.msg(seq).id)
		}
	}
	a, ok := s.takeAway()
	require.True(m.t, ok)

	owed := map[uint64]bool{}
	for _, msg := range m.stream {
		if !m.done[msg.seq] {
			owed[msg.seq] = true
		}
	}
	cur, hasCursor := s.cursorForReplay()
	if !hasCursor {
		return // the replay is the rewind itself
	}
	sent := map[uint64]bool{}
	for seq := range cur.holes {
		if owed[seq] {
			sent[seq] = true
		}
	}
	for seq := range owed {
		if seq >= cur.next {
			sent[seq] = true
		}
	}
	for seq := range m.rewind(a) {
		require.True(m.t, sent[seq], "the time rewind sends %d and the cursor (next %d, base %d) does not", seq, cur.next, s.cur.base)
	}
	if !m.dropped {
		for seq := range owed {
			require.True(m.t, sent[seq], "a cursor never dropped loses %d", seq)
		}
	}
}

// rewind is what replayOffline sends without a cursor, for a session held in
// memory: from the rewind time, or from a sequence the absence names when that
// was stored before it.
func (m *cursorModel) rewind(a away) map[uint64]bool {
	since := a.at.Add(-modelWindow)
	storedAfter := func(seq uint64) bool { return m.msg(seq).at.After(since) }
	fromSeq := a.fromSeq
	if a.lateSeq != 0 && (fromSeq == 0 || a.lateSeq < fromSeq) && !(fromSeq == 0 && storedAfter(a.lateSeq)) {
		fromSeq = a.lateSeq
	}
	byTime := fromSeq == 0 || storedAfter(fromSeq)
	out := map[uint64]bool{}
	for _, msg := range m.stream {
		in := !msg.at.Before(since)
		if !byTime {
			in = msg.seq >= fromSeq
		}
		if in && !m.done[msg.seq] {
			out[msg.seq] = true
		}
	}
	return out
}

func TestTheCursorNeverSendsLessThanTheRewindAndAFullOneLosesNothing_Model(t *testing.T) {
	stats := &modelStats{}
	seeds := 400
	if testing.Short() {
		seeds = 50
	}
	for seed := uint64(1); seed <= uint64(seeds); seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			m := newCursorModel(t, seed, stats)
			steps := 50 + m.rng.IntN(400)
			for range steps {
				m.step()
			}
			m.release()
		})
	}
	t.Logf("restarts %d, refused %d, drops %d, late copies below a base %d",
		stats.restarts, stats.refused, stats.drops, stats.belowBase)
	require.Positive(t, stats.restarts, "the model reaches restarts")
	require.Positive(t, stats.refused, "and refusals")
}
