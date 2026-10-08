package natsmqtt5

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A copy that reaches a released session lowers the sequence its next replay
// starts at, and only ever lowers it [MQTT-4.4.0-1].
func TestALateCopyOnAReleasedSessionLowersTheReplayStartOnly(t *testing.T) {
	s := newSession("c")
	s.markAwayLocked(away{at: time.Now(), fromSeq: 50})

	_, persist := s.lateCopy(nil, 30)
	assert.False(t, persist, "no stored record, nothing to rewrite")
	_, persist = s.lateCopy(nil, 40)
	assert.False(t, persist)

	a, ok := s.takeAway()
	require.True(t, ok)
	assert.Equal(t, uint64(30), a.lateSeq)
	assert.Equal(t, uint64(50), a.fromSeq)
}

// With no absence to correct, a copy is not noted: a session that never lost
// its connection is not owed a replay.
func TestALateCopyOnASessionWithNoAbsenceIsNotNoted(t *testing.T) {
	s := newSession("c")
	_, persist := s.lateCopy(nil, 30)
	assert.False(t, persist)
	_, ok := s.takeAway()
	assert.False(t, ok)
}

// A copy so far below what the session has delivered that a replay from it
// could repeat a QoS 2 message is left to the live path [MQTT-4.3.3-2].
func TestALateCopyBelowTheDeliveredWindowIsNotNoted(t *testing.T) {
	s := newSession("c")
	s.noteDelivered("high", deliveredSeqWindow+100)
	s.markAwayLocked(away{at: time.Now(), fromSeq: deliveredSeqWindow + 50})

	s.lateCopy(nil, 10)

	a, _ := s.takeAway()
	assert.Zero(t, a.lateSeq)
}

// The connection the session has now, not the ended one, takes the copy.
func TestALateCopyReachingAnEndedConnectionGoesToTheSuccessor(t *testing.T) {
	s := newSession("c")
	old, next := &conn{}, &conn{}
	s.conn = next

	got, persist := s.lateCopy(old, 30)

	assert.Same(t, next, got)
	assert.False(t, persist)
}
