package natsmqtt5

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The delivered ids a record cannot hold in its value go to chunks beside it,
// so a record can keep the replay cursor however many ids lie above it
// [MQTT-4.4.0-1]: the cursor promises every one.

func deliveredIDs(from, to int) []storedDelivered {
	var ids []storedDelivered
	for i := from; i < to; i++ {
		ids = append(ids, storedDelivered{ID: fmt.Sprintf("msg-%06d", i), Seq: uint64(i)})
	}
	return ids
}

func withCursor(clientID string, inline, over []storedDelivered) *sessionRecord {
	return &sessionRecord{
		ClientID: clientID, Delivered: inline, deliveredOver: over,
		ReplayCursor: 3, ReplayHoles: []storedDelivered{{ID: "hole", Seq: 1}},
	}
}

func TestSpillDeliveredKeepsEveryIDAndTheCursor(t *testing.T) {
	s := smallPayloadStore(t)
	all := deliveredIDs(3, 3003)
	rec := withCursor("spill-all", all[:100], all[100:])

	require.Zero(t, s.spillDelivered(rec, s.valueLimit(), nil))
	assert.LessOrEqual(t, len(rec.Delivered), 100, "a small value holds fewer still")
	assert.Greater(t, len(rec.DeliveredSpill), 1, "a small max_payload cuts several chunks")
	assert.Equal(t, uint64(3), rec.ReplayCursor, "every id is kept, so the cursor is")
	assert.NotEmpty(t, rec.ReplayHoles)

	back, unreadable := s.loadDeliveredSpill(context.Background(), rec.DeliveredSpill)
	require.Zero(t, unreadable)
	assert.Equal(t, all, append(append([]storedDelivered(nil), rec.Delivered...), back...), "in order, none missing")
}

// A write after ids were trimmed off the front and others appended at the back
// names the chunks in between again without writing them.
func TestSpillDeliveredLeavesTheChunksBetweenTheEndsAlone(t *testing.T) {
	s := smallPayloadStore(t)
	first := withCursor("spill-stable", nil, deliveredIDs(0, 3000))
	require.Zero(t, s.spillDelivered(first, s.valueLimit(), nil))
	require.Greater(t, len(first.DeliveredSpill), 4)

	second := withCursor("spill-stable", nil, deliveredIDs(300, 3300))
	require.Zero(t, s.spillDelivered(second, s.valueLimit(), nil))
	shared := 0
	have := map[string]bool{}
	for _, k := range first.DeliveredSpill {
		have[k] = true
	}
	for _, k := range second.DeliveredSpill {
		if have[k] {
			shared++
		}
	}
	assert.Greater(t, shared, len(first.DeliveredSpill)/2, "most chunks keep their key, so most are not written again")
}

func TestSpillDeliveredWithNothingOverLeavesTheRecordAlone(t *testing.T) {
	s := smallPayloadStore(t)
	rec := withCursor("spill-none", deliveredIDs(3, 10), nil)

	require.Zero(t, s.spillDelivered(rec, s.valueLimit(), nil))
	assert.Empty(t, rec.DeliveredSpill, "a record that fits is written as it always was")
	assert.Equal(t, uint64(3), rec.ReplayCursor)
}

// Ids that are not written leave the record without a cursor, which would
// otherwise promise them.
func TestSpillDeliveredThatCannotWriteDropsTheCursor(t *testing.T) {
	s := smallPayloadStore(t)
	rec := withCursor("spill-fail", deliveredIDs(3, 10), deliveredIDs(10, 2000))
	s.nc.Close()

	assert.Positive(t, s.spillDelivered(rec, s.valueLimit(), nil))
	assert.Zero(t, rec.ReplayCursor)
	assert.Nil(t, rec.ReplayHoles)
}

// A record from a broker that predates the chunks has no DeliveredSpill and
// restores exactly as it did: its ids, and its cursor.
func TestARecordWithoutDeliveredSpillRestoresAsItDid(t *testing.T) {
	old := []byte(`{"Version":1,"ClientID":"old","Owner":"x","ReplayCursor":7,` +
		`"ReplayHoles":[{"ID":"h","Seq":2}],"Delivered":[{"ID":"a","Seq":7},{"ID":"b","Seq":9}]}`)
	rec, err := decodeRecord(old)
	require.NoError(t, err)
	assert.Empty(t, rec.DeliveredSpill)

	s := smallPayloadStore(t)
	b := &Broker{store: s, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	rec.deliveredWas, rec.deliveredSpillWas = rec.Delivered, rec.DeliveredSpill
	rec.replayCursorWas, rec.replayHolesWas = rec.ReplayCursor, rec.ReplayHoles
	b.restoreDeliveredSpill(context.Background(), newSession("old"), rec)

	assert.Equal(t, []storedDelivered{{ID: "a", Seq: 7}, {ID: "b", Seq: 9}}, rec.deliveredWas)
	assert.Equal(t, uint64(7), rec.replayCursorWas)
	assert.Len(t, rec.replayHolesWas, 1)
}

// A chunk that is gone leaves ids out, so the restored replay does not use the
// cursor: it starts at the time rewind and may repeat, never loses.
func TestAnUnreadableDeliveredChunkDropsTheRestoredCursor(t *testing.T) {
	s := smallPayloadStore(t)
	rec := withCursor("spill-gone", deliveredIDs(3, 10), deliveredIDs(10, 1500))
	require.Zero(t, s.spillDelivered(rec, s.valueLimit(), nil))
	require.NoError(t, s.deleteBlob(context.Background(), rec.DeliveredSpill[0]))

	b := &Broker{store: s, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	rec.deliveredWas, rec.deliveredSpillWas = rec.Delivered, rec.DeliveredSpill
	rec.replayCursorWas, rec.replayHolesWas = rec.ReplayCursor, rec.ReplayHoles
	b.restoreDeliveredSpill(context.Background(), newSession("spill-gone"), rec)

	assert.Zero(t, rec.replayCursorWas)
	assert.Nil(t, rec.replayHolesWas)
	assert.Greater(t, len(rec.deliveredWas), 10, "the chunks that could be read are still used")
}
