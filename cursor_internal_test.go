package natsmqtt5

import (
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5/v2/packet"
)

// The parts of the replay cursor (cursor.go) a socket cannot reach in one
// state. Each is driven over the wire in replay_cursor_test.go.

func sessionWithCursor(next uint64) *session {
	s := newSession("c")
	s.cur.next = next
	return s
}

func cursorOf(s *session) (next uint64, holes map[uint64]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cur.next, s.cursorViewLocked().holes
}

// A live copy below the cursor was not in the connection when the cursor passed
// it: it is a hole until it is delivered.
func TestALiveCopyBelowTheCursorIsAHoleUntilDelivered(t *testing.T) {
	s := sessionWithCursor(10)

	s.noteLiveCopy(4, "four")
	s.noteLiveCopy(12, "twelve")
	_, holes := cursorOf(s)
	assert.Equal(t, map[uint64]string{4: "four"}, holes, "at or above the cursor the replay reads it anyway")

	s.noteDelivered("four", 4)
	_, holes = cursorOf(s)
	assert.Empty(t, holes)
}

// A copy below the cursor whose id was delivered is not owed.
func TestADeliveredCopyBelowTheCursorIsNotAHole(t *testing.T) {
	s := sessionWithCursor(10)
	s.noteDelivered("four", 4)

	s.noteLiveCopy(4, "four")

	_, holes := cursorOf(s)
	assert.Empty(t, holes)
}

// The ids at or above the cursor are what keeps its replay from repeating a QoS
// 2 message [MQTT-4.3.3-2], so the pruning that forgets old ids leaves them,
// however far below the newest they are.
func TestIDsAtOrAboveTheCursorOutliveTheWindow(t *testing.T) {
	s := sessionWithCursor(5)
	s.rewind = time.Millisecond
	s.noteDelivered("below", 4)
	s.noteDelivered("at", 5)
	time.Sleep(5 * time.Millisecond)
	for i := 0; i < 200; i++ {
		s.noteDelivered(fmt.Sprintf("new%d", i), uint64(deliveredSeqWindow+100+i))
	}

	ids := s.deliveredIDs()
	assert.True(t, ids["at"])
	assert.False(t, ids["below"], "below the cursor the window's rule applies")
}

// Past cursorIDBound the cursor goes, and the window's rule with it.
func TestTooManyIDsAboveTheCursorDropIt(t *testing.T) {
	was := cursorIDBound.Swap(100)
	t.Cleanup(func() { cursorIDBound.Store(was) })
	s := sessionWithCursor(1)
	for i := 1; i <= 192; i++ {
		s.noteDelivered(fmt.Sprintf("id%d", i), uint64(i))
	}
	next, _ := cursorOf(s)
	assert.Zero(t, next)
}

// Past maxReplayHoles the cursor goes.
func TestTooManyHolesDropTheCursor(t *testing.T) {
	was := maxReplayHoles.Swap(2)
	t.Cleanup(func() { maxReplayHoles.Store(was) })
	s := sessionWithCursor(100)
	s.noteLiveCopy(1, "a")
	s.noteLiveCopy(2, "b")
	next, _ := cursorOf(s)
	require.Equal(t, uint64(100), next)

	s.noteLiveCopy(3, "c")
	next, holes := cursorOf(s)
	assert.Zero(t, next)
	assert.Empty(t, holes)
}

// A delivery noted while a scan runs starts none, and the scan may already have
// read past it or not, and end without moving the cursor (it stopped at the
// connection's floor, or found the cursor changed): it scans once more, or a
// hole that delivery would have found waits for a delivery that may never come.
// Without one, a scan that did not move ends.
func TestADeliveryNotedWhileAScanRunsScansOnceMore(t *testing.T) {
	was := cursorScanAfter.Swap(1)
	t.Cleanup(func() { cursorScanAfter.Store(was) })
	s := sessionWithCursor(10)
	c := &conn{sess: s}
	s.conn = c

	require.True(t, s.noteDelivered("a", 10), "the first delivery past the cursor starts a scan")
	assert.False(t, s.noteDelivered("b", 11), "one noted while it runs starts none")

	assert.Same(t, c, s.scanAgain(c, false), "the scan that did not move runs once more for it")
	assert.Nil(t, s.scanAgain(c, false), "and then ends")
	assert.True(t, s.noteDelivered("c", 12), "so the next delivery starts one")
}

// A record carries the cursor only with every id at or above it: one that left
// ids out for the bound is replayed from the time rewind instead.
func TestARecordThatCannotCarryEveryIDAboveTheCursorCarriesNoCursor(t *testing.T) {
	s := sessionWithCursor(3)
	s.noteLiveCopy(1, "hole")
	for i := 3; i < 13; i++ {
		s.noteDelivered(fmt.Sprintf("id%d", i), uint64(i))
	}
	s.markAwayLocked(away{at: time.Now()})

	var rec sessionRecord
	require.Zero(t, s.awayStateInto(&rec))
	assert.Equal(t, uint64(3), rec.ReplayCursor)
	assert.Equal(t, []storedDelivered{{ID: "hole", Seq: 1}}, rec.ReplayHoles)
	assert.Len(t, rec.Delivered, 10)

	was, wasSpilled := storedDeliveredBound.Swap(5), spilledDeliveredBound.Swap(0)
	t.Cleanup(func() { storedDeliveredBound.Store(was); spilledDeliveredBound.Store(wasSpilled) })
	rec = sessionRecord{}
	assert.NotZero(t, s.awayStateInto(&rec))
	assert.Zero(t, rec.ReplayCursor)
	assert.Empty(t, rec.ReplayHoles)
}

// Trimming the ids to fit a value takes the cursor out of the record too.
func TestTrimmingTheStoredIDsTakesTheCursorOut(t *testing.T) {
	rec := &sessionRecord{ClientID: "c", ReplayCursor: 3, ReplayHoles: []storedDelivered{{ID: "h", Seq: 1}}}
	for i := 3; i < 200; i++ {
		rec.Delivered = append(rec.Delivered, storedDelivered{ID: fmt.Sprintf("%024d", i), Seq: uint64(i)})
	}
	store := &sessionStore{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}

	_, err := store.spillRecord(rec, 2000)
	require.NoError(t, err)
	assert.Less(t, len(rec.Delivered), 197)
	assert.Zero(t, rec.ReplayCursor)
	assert.Nil(t, rec.ReplayHoles)
}

// With a cursor, a late copy below it that was delivered is not noted; one at
// or above it is, however far below the newest delivered sequence, because
// every id above the cursor is kept.
func TestALateCopyIsJudgedByTheCursor(t *testing.T) {
	s := sessionWithCursor(50)
	s.noteDelivered("old", 40)
	s.noteDelivered("high", deliveredSeqWindow+1000)
	s.markAwayLocked(away{at: time.Now()})

	s.lateCopy(nil, 40, "old")
	s.lateCopy(nil, 60, "late")

	a, _ := s.takeAway()
	assert.Equal(t, uint64(60), a.lateSeq)
	_, holes := cursorOf(s)
	assert.Empty(t, holes)
}

// A record's cursor is restored with the holes below it that were not sent.
func TestARestoredCursorKeepsOnlyHolesBelowIt(t *testing.T) {
	s := newSession("c")
	s.noteDelivered("in-flight", 5) // restore.go notes the record's in-flight messages first
	s.markAwayRestored(time.Now(), 0, 0, time.Time{}, nil, 10,
		[]storedDelivered{{ID: "a", Seq: 3}, {ID: "b", Seq: 10}, {ID: "", Seq: 4}, {ID: "in-flight", Seq: 5}})

	next, holes := cursorOf(s)
	assert.Equal(t, uint64(10), next)
	assert.Equal(t, map[uint64]string{3: "a"}, holes)
}

// A replacing SUBSCRIBE keeps what the subscription was owed, unless it now
// owes messages the old one skipped (QoS 0, or the client's own).
func TestReplacingASubscriptionKeepsItsSinceUnlessItNowOwesMore(t *testing.T) {
	for _, tc := range []struct {
		name     string
		old, new packet.Subscription
		keep     bool
	}{
		{"same", packet.Subscription{QoS: 1}, packet.Subscription{QoS: 2}, true},
		{"from QoS 0", packet.Subscription{QoS: 0}, packet.Subscription{QoS: 1}, false},
		{"No Local lifted", packet.Subscription{QoS: 1, NoLocal: true}, packet.Subscription{QoS: 1}, false},
		{"No Local set", packet.Subscription{QoS: 1}, packet.Subscription{QoS: 1, NoLocal: true}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			old := &subscription{filter: "f", opts: tc.old, grantedQoS: tc.old.QoS}
			old.since.Store(7)
			sub := &subscription{filter: "f", opts: tc.new, grantedQoS: tc.new.QoS}
			assert.Equal(t, tc.keep, keepsSince(old, sub))
		})
	}
}

// sessionWithoutCursor is a connected session's state after its cursor was
// dropped: one subscription whose since is known.
func sessionWithoutCursor() (*session, []*subscription) {
	s := newSession("c")
	s.rewind = time.Minute
	sub := &subscription{filter: "f"}
	sub.since.Store(0)
	s.subs[sub.filter] = sub
	return s, []*subscription{sub}
}

func restartAt(s *session, r restartScan, floor uint64, now time.Time) bool {
	s.cursorMu.Lock()
	defer s.cursorMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.restartCursorLocked(r, floor, now)
}

// A restart scan read from base to last: what it found owed and not delivered
// below next is a hole, and a live copy below base drops the cursor, since the
// rewind would start there.
func TestARestartedCursorKeepsWhatItReadAndDropsForACopyBelowItsBase(t *testing.T) {
	s, subs := sessionWithoutCursor()
	s.noteDelivered("eleven", 11)
	now := time.Now()
	read := []scanned{{seq: 10, id: "ten", owed: true}, {seq: 11, id: "eleven", owed: true}, {seq: 12, id: "twelve"}}

	require.True(t, restartAt(s, restartScan{subs: subs, began: now, base: 10, last: 12, read: read}, 0, now))
	next, holes := cursorOf(s)
	assert.Equal(t, uint64(13), next)
	assert.Equal(t, map[uint64]string{10: "ten"}, holes, "delivered and never-owed messages are not holes")

	s.noteLiveCopy(11, "eleven")
	next, _ = cursorOf(s)
	require.Equal(t, uint64(13), next, "at or above base a copy is judged as under any cursor")
	s.noteLiveCopy(9, "nine")
	next, _ = cursorOf(s)
	assert.Zero(t, next, "below base the rewind would start at it; the cursor goes")
}

// Each thing a restart depends on refuses it when it does not hold.
func TestARestartIsRefusedUnlessItIsAsSafeAsTheRewind(t *testing.T) {
	now := time.Now()
	scan := func(subs []*subscription) restartScan {
		return restartScan{subs: subs, began: now, base: 10, last: 20}
	}
	t.Run("the connection holds a message below base", func(t *testing.T) {
		s, subs := sessionWithoutCursor()
		assert.False(t, restartAt(s, scan(subs), 9, now))
	})
	t.Run("the scan took a rewind, so ids from base up may be gone", func(t *testing.T) {
		s, subs := sessionWithoutCursor()
		assert.False(t, restartAt(s, scan(subs), 0, now.Add(time.Minute)))
	})
	t.Run("a subscription changed while it read", func(t *testing.T) {
		s, subs := sessionWithoutCursor()
		other := &subscription{filter: "g"}
		other.since.Store(0)
		s.subs["g"] = other
		assert.False(t, restartAt(s, scan(subs), 0, now))
	})
	t.Run("a cursor was set meanwhile", func(t *testing.T) {
		s, subs := sessionWithoutCursor()
		r := scan(subs)
		s.cur.gen++
		assert.False(t, restartAt(s, r, 0, now))
	})
	t.Run("a resume replay holds a pin", func(t *testing.T) {
		s, subs := sessionWithoutCursor()
		s.pinForReplay(5)
		assert.False(t, restartAt(s, scan(subs), 0, now))
	})
	t.Run("more holes than a cursor keeps", func(t *testing.T) {
		was := maxReplayHoles.Swap(1)
		t.Cleanup(func() { maxReplayHoles.Store(was) })
		s, subs := sessionWithoutCursor()
		r := scan(subs)
		r.read = []scanned{{seq: 10, id: "a", owed: true}, {seq: 11, id: "b", owed: true}}
		assert.False(t, restartAt(s, r, 0, now))
	})
	t.Run("all hold", func(t *testing.T) {
		s, subs := sessionWithoutCursor()
		assert.True(t, restartAt(s, scan(subs), 15, now))
		next, _ := cursorOf(s)
		assert.Equal(t, uint64(15), next, "next stops at what the connection holds")
	})
}

// A restarted cursor is not written to the record, which then reads as one
// without a cursor; and a late copy reaching the released session is noted for
// that record by the rule without one.
func TestARestartedCursorStaysOutOfTheRecord(t *testing.T) {
	s, subs := sessionWithoutCursor()
	now := time.Now()
	require.True(t, restartAt(s, restartScan{subs: subs, began: now, base: 10, last: 20}, 0, now))
	s.markAwayLocked(away{at: now})

	s.lateCopy(nil, 15, "late")
	_, holes := cursorOf(s)
	assert.Equal(t, map[uint64]string{15: "late"}, holes, "the cursor held in memory keeps it as a hole")

	var rec sessionRecord
	s.awayStateInto(&rec)
	assert.Zero(t, rec.ReplayCursor)
	assert.Equal(t, uint64(15), rec.AwayLateSeq, "the record names the late copy, as one without a cursor does")
}
