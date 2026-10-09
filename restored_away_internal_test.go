package natsmqtt5

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A record written before it held the replay position must still decode, with
// the position empty, so the replay falls back to AwayAt as it did. The version
// stays 1: a field an older broker does not know is ignored by it.
func TestARecordWrittenBeforeTheReplayPositionStillDecodes(t *testing.T) {
	old := `{"Version":1,"ClientID":"c","Owner":"o","Attached":false,"Identity":"","Username":"",` +
		`"Subscriptions":null,"ExpirySeconds":300,"ExpiresAt":"2030-01-01T00:00:00Z","AwayAt":"2029-12-31T00:00:00Z"}`
	rec, err := decodeRecord([]byte(old))
	require.NoError(t, err)
	assert.False(t, rec.AwayAt.IsZero())
	assert.Zero(t, rec.AwayFromSeq)
	assert.Empty(t, rec.Delivered)
}

func TestTheReplayPositionAndRecentIDsRoundTripThroughARecord(t *testing.T) {
	rec := &sessionRecord{Version: sessionRecordVersion, ClientID: "c",
		AwayFromSeq: 7, Delivered: []storedDelivered{{ID: "a", Seq: 9}}}
	back, err := decodeRecord(encodeRecord(rec))
	require.NoError(t, err)
	assert.Equal(t, uint64(7), back.AwayFromSeq)
	assert.Equal(t, []storedDelivered{{ID: "a", Seq: 9}}, back.Delivered)

	empty, err := decodeRecord(encodeRecord(&sessionRecord{Version: sessionRecordVersion}))
	require.NoError(t, err)
	assert.Zero(t, empty.AwayFromSeq)
	assert.NotContains(t, string(encodeRecord(&sessionRecord{Version: sessionRecordVersion})), "Delivered")
}

// An id can be met again by the replay if its queue copy is at or above the
// replay's start, or if the replay's time rewind reaches it (delivered within
// OfflineQueueRewind), so those are kept; the rest of the session's memory of
// what it delivered is irrelevant to the next broker.
func TestOnlyIDsTheReplayCanMeetAgainAreStored(t *testing.T) {
	s := newSession("c")
	long := time.Now().Add(-time.Hour)
	s.delivered = map[string]deliveredMark{
		"below-old":       {at: long, seq: 4},
		"unqueued-old":    {at: long},
		"at":              {at: long, seq: 5},
		"above":           {at: long, seq: 8},
		"below-recent":    {at: time.Now(), seq: 3},
		"unqueued-recent": {at: time.Now()},
	}
	s.markAwayLocked(away{at: time.Now(), fromSeq: 5})

	from, _, ids, dropped := s.awayState()
	assert.Equal(t, uint64(5), from)
	var got []string
	for _, d := range ids {
		got = append(got, d.ID)
	}
	assert.ElementsMatch(t, []string{"at", "above", "below-recent", "unqueued-recent"}, got)
	assert.Zero(t, dropped)
}

func TestOnlyRecentIDsAreStoredWhenThereIsNoReplayPosition(t *testing.T) {
	s := newSession("c")
	s.delivered = map[string]deliveredMark{
		"old":    {at: time.Now().Add(-time.Hour), seq: 5},
		"recent": {at: time.Now(), seq: 6},
	}
	s.markAwayLocked(away{at: time.Now()})

	from, _, ids, _ := s.awayState()
	assert.Zero(t, from)
	assert.Equal(t, []storedDelivered{{ID: "recent", Seq: 6}}, ids)
}

func TestTheStoredIDsAreBounded(t *testing.T) {
	s := newSession("c")
	for i := 1; i <= maxStoredDelivered+10; i++ {
		s.noteDelivered(fmt.Sprintf("id%d", i), uint64(i))
	}
	s.markAwayLocked(away{at: time.Now(), fromSeq: 1})

	_, _, ids, dropped := s.awayState()
	assert.Len(t, ids, maxStoredDelivered)
	assert.Equal(t, 10, dropped)
}

// What the record carries is what keeps the rewind from repeating a message: a
// restored session knows the ids, so the replay that starts below them skips them.
func TestARestoredSessionKnowsWhatItAlreadyDelivered(t *testing.T) {
	s := newSession("c")
	s.markAwayRestored(time.Now(), 5, 0, time.Time{}, []storedDelivered{{ID: "seen", Seq: 8}})

	a, ok := s.takeAway()
	require.True(t, ok)
	assert.Equal(t, uint64(5), a.fromSeq)
	assert.True(t, a.restored)
	assert.True(t, s.deliveredIDs()["seen"])
}
