package natsmqtt5

import (
	"context"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// smallPayloadStore is a store on a NATS server whose max_payload is small, so
// that a few hundred in-flight entries spill into several chunks.
func smallPayloadStore(t *testing.T) *sessionStore {
	t.Helper()
	srv, err := natsserver.NewServer(&natsserver.Options{
		Host: "127.0.0.1", Port: -1, JetStream: true, MaxPayload: 4096,
		StoreDir: t.TempDir(), NoLog: true, NoSigs: true,
	})
	require.NoError(t, err)
	go srv.Start()
	require.True(t, srv.ReadyForConnections(10*time.Second))
	t.Cleanup(srv.Shutdown)
	s, _ := storeOn(t, srv.ClientURL())
	return s
}

func spillEntries(n int) []storedInflight {
	var es []storedInflight
	for i := 1; i <= n; i++ {
		es = append(es, storedInflight{ID: uint16(i), QoS: 1, Seq: uint64(i)})
	}
	return es
}

func blobRevisions(t *testing.T, s *sessionStore, keys []string) map[string]uint64 {
	t.Helper()
	kv, err := s.blobBucket(context.Background(), false)
	require.NoError(t, err)
	revs := map[string]uint64{}
	for _, k := range keys {
		e, err := kv.Get(context.Background(), k)
		require.NoError(t, err, k)
		revs[k] = e.Revision()
	}
	return revs
}

// A chunk a record names again with the same content is not written again: its
// key is the digest of its content, so naming it is enough.
func TestSpillRecordNamesAnUnchangedChunkWithoutWritingIt(t *testing.T) {
	s := smallPayloadStore(t)
	first := &sessionRecord{ClientID: "reuse", Inflight: spillEntries(400)}
	_, err := s.spillRecord(first, s.valueLimit())
	require.NoError(t, err)
	require.Greater(t, len(first.Spill), 1)
	before := blobRevisions(t, s, first.Spill)

	known := map[string]struct{}{}
	for _, k := range first.Spill {
		known[k] = struct{}{}
	}
	second := &sessionRecord{ClientID: "reuse", Inflight: spillEntries(400)}
	lost, err := s.spillRecordReusing(second, s.valueLimit(), known)
	require.NoError(t, err)
	assert.Zero(t, lost)
	assert.Equal(t, first.Spill, second.Spill)
	assert.Equal(t, before, blobRevisions(t, s, second.Spill), "no chunk was written again")

	third := &sessionRecord{ClientID: "reuse", Inflight: spillEntries(400)}
	_, err = s.spillRecordReusing(third, s.valueLimit(), nil)
	require.NoError(t, err)
	assert.NotEqual(t, before, blobRevisions(t, s, third.Spill), "without the last record's names every chunk is written")
}

// A checkpoint writes the chunks whose content changed and no others, and
// commitSpill deletes the ones the new record stopped naming.
func TestSessionSpillWritesOnlyTheChunksThatChanged(t *testing.T) {
	s := smallPayloadStore(t)
	sess := &session{clientID: "reuse-session"}
	rec := &sessionRecord{ClientID: "reuse-session", Inflight: spillEntries(400)}
	at := time.Now()
	_, err := sess.spillInto(s, rec)
	require.NoError(t, err)
	sess.commitSpill(s, rec, at)
	before := blobRevisions(t, s, rec.Spill)

	next := &sessionRecord{ClientID: "reuse-session", Inflight: spillEntries(400)}
	_, err = sess.spillInto(s, next)
	require.NoError(t, err)
	sess.commitSpill(s, next, time.Now())
	assert.Equal(t, before, blobRevisions(t, s, next.Spill), "unchanged: nothing written")
	assert.True(t, sess.spillAt.Equal(at), "chunks carried over keep the time they were written")

	appended := &sessionRecord{ClientID: "reuse-session", Inflight: spillEntries(401)}
	_, err = sess.spillInto(s, appended)
	require.NoError(t, err)
	sess.commitSpill(s, appended, time.Now())
	same := 0
	for _, k := range appended.Spill {
		if _, ok := before[k]; ok {
			same++
		}
	}
	assert.Equal(t, len(appended.Spill)-1, same, "a message appended changes the last chunk only")
	for k, rev := range blobRevisions(t, s, appended.Spill[:same]) {
		assert.Equal(t, before[k], rev, "an unchanged chunk was not written again")
	}
	next = appended
	changed := &sessionRecord{ClientID: "reuse-session", Inflight: spillEntries(400)[1:]}
	_, err = sess.spillInto(s, changed)
	require.NoError(t, err)
	sess.commitSpill(s, changed, time.Now())
	kv, err := s.blobBucket(context.Background(), false)
	require.NoError(t, err)
	for _, k := range next.Spill {
		named := false
		for _, n := range changed.Spill {
			named = named || n == k
		}
		_, err := kv.Get(context.Background(), k)
		if named {
			assert.NoError(t, err, "a chunk still named stays")
		} else {
			assert.ErrorIs(t, err, jetstream.ErrKeyNotFound, "a chunk no longer named is deleted")
		}
	}
}

// A message acknowledged at the front of the in-flight list moves every size-cut boundary after it, so
// every chunk changed and a checkpoint wrote them all again. Chunks cut at identifiers (a chunk ends
// after an entry whose identifier is a multiple of the chunk's identifier count) keep their boundaries
// when an entry before them goes, so only the chunk the entry was in is written again.
func TestSpillAnAcknowledgementAtTheFrontWritesFewChunks(t *testing.T) {
	s := smallPayloadStore(t)
	s.spillChunkIDs = 32
	sess := &session{clientID: "front-ack"}
	rec := &sessionRecord{ClientID: "front-ack", Inflight: spillEntries(400)}
	_, err := sess.spillInto(s, rec)
	require.NoError(t, err)
	sess.commitSpill(s, rec, time.Now())
	require.Greater(t, len(rec.Spill), 5)

	acked := &sessionRecord{ClientID: "front-ack", Inflight: spillEntries(400)[1:]}
	lost, err := sess.spillInto(s, acked)
	require.NoError(t, err)
	assert.Zero(t, lost)
	sess.commitSpill(s, acked, time.Now())
	before := map[string]bool{}
	for _, k := range rec.Spill {
		before[k] = true
	}
	changed := 0
	for _, k := range acked.Spill {
		if !before[k] {
			changed++
		}
	}
	assert.LessOrEqual(t, changed, 2, "chunks written again after the front entry was acknowledged: %d of %d", changed, len(acked.Spill))

	loaded, unreadable := s.loadSpill(context.Background(), acked.Spill)
	assert.Zero(t, unreadable)
	assert.Equal(t, spillEntries(400)[1+len(acked.Inflight):], loaded, "the chunks read back in order")
}

// The sweep deletes a payload of an owner whose record was released and does not
// name it, keeps the ones the record names (in-flight entries and chunks), and
// leaves an attached owner's payloads to the TTL.
func TestSweepDeletesAReleasedOwnersUnnamedPayloads(t *testing.T) {
	s, _ := storeFixture(t)
	s.blobGrace = time.Millisecond
	ctx := context.Background()

	rec, rev, _, err := s.claim(ctx, "released", "", "", false)
	require.NoError(t, err)
	rec.Attached = false
	rec.ExpirySeconds = 3600
	rec.ExpiresAt = time.Now().Add(time.Hour)
	named, err := s.putBlob(ctx, blobOwner("released"), []byte("named"))
	require.NoError(t, err)
	chunk, err := s.putBlob(ctx, blobOwner("released"), []byte("[]"))
	require.NoError(t, err)
	unnamed, err := s.putBlob(ctx, blobOwner("released"), []byte("unnamed"))
	require.NoError(t, err)
	rec.Inflight = []storedInflight{{Blob: named}}
	rec.Spill = []string{chunk}
	_, err = s.save(ctx, rec, rev)
	require.NoError(t, err)

	other, _, _, err := s.claim(ctx, "attached", "", "", false)
	require.NoError(t, err)
	_ = other
	attached, err := s.putBlob(ctx, blobOwner("attached"), []byte("attached"))
	require.NoError(t, err)
	time.Sleep(10 * time.Millisecond)

	require.NoError(t, s.sweep(ctx))

	kv, err := s.blobBucket(ctx, false)
	require.NoError(t, err)
	for key, want := range map[string]bool{named: true, chunk: true, unnamed: false, attached: true} {
		_, err := kv.Get(ctx, key)
		if want {
			assert.NoError(t, err, key)
		} else {
			assert.ErrorIs(t, err, jetstream.ErrKeyNotFound, key)
		}
	}
}
