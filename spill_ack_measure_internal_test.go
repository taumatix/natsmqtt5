package natsmqtt5

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fullWindow is the in-flight list of a session with a full Receive Maximum of QoS 1 messages in
// flight, each carrying a 100 byte stored PUBLISH, as a client that stopped acknowledging leaves it.
func fullWindow(n int) []storedInflight {
	pub := make([]byte, 100)
	es := make([]storedInflight, n)
	for i := range es {
		es[i] = storedInflight{ID: uint16(i + 1), QoS: 1, Seq: uint64(i + 1), Pub: pub}
	}
	return es
}

// TestSpillAcknowledgementCost measures what a checkpoint costs after the client acknowledged the message
// at the front of a full window (65535 entries, default max_payload): spillInto and commitSpill on a real
// JetStream, with the payload bucket bytes they wrote next to the size of the session record, which a
// checkpoint writes whatever changed. It logs the numbers (run with -v) and asserts the bound the
// ROADMAP records: an acknowledgement writes one chunk, a fraction of the record. SPILL_ACKS sets how many
// acknowledgements to run (default 50).
func TestSpillAcknowledgementCost(t *testing.T) {
	s, _ := storeFixture(t)
	sess := &session{clientID: "measure-ack"}
	all := fullWindow(65535)
	rec := &sessionRecord{ClientID: "measure-ack", Inflight: all}
	_, err := sess.spillInto(s, rec)
	require.NoError(t, err)
	sess.commitSpill(s, rec, time.Now())
	require.Greater(t, len(rec.Spill), 10)
	kv, err := s.blobBucket(context.Background(), false)
	require.NoError(t, err)

	acks := 50
	if os.Getenv("SPILL_ACKS") != "" {
		acks = 0
		for _, c := range os.Getenv("SPILL_ACKS") {
			acks = acks*10 + int(c-'0')
		}
	}
	var chunks, chunkBytes, recBytes, maxChunks int
	var spill time.Duration
	for i := 1; i <= acks; i++ {
		prev := map[string]bool{}
		for _, k := range sess.spillKeys {
			prev[k] = true
		}
		next := &sessionRecord{ClientID: "measure-ack", Inflight: all[i:]}
		start := time.Now()
		_, err := sess.spillInto(s, next)
		require.NoError(t, err)
		sess.commitSpill(s, next, time.Now())
		spill += time.Since(start)
		recBytes += len(encodeRecord(next))
		n := 0
		for _, k := range next.Spill {
			if !prev[k] {
				n++
				e, err := kv.Get(context.Background(), k)
				require.NoError(t, err)
				chunkBytes += len(e.Value())
			}
		}
		chunks += n
		maxChunks = max(maxChunks, n)
	}
	t.Logf("%d chunks per record; per acknowledgement: %.2f chunks written (max %d), %d chunk bytes, %d record bytes, %v spillInto+commitSpill",
		len(rec.Spill), float64(chunks)/float64(acks), maxChunks, chunkBytes/acks, recBytes/acks, spill/time.Duration(acks))
	// Sizing the record by encoding it at each step of the search took 247 ms per acknowledgement here, now about 75.
	if !raceEnabled {
		require.Less(t, spill/time.Duration(acks), 160*time.Millisecond, "sizing the record must not encode it at every step")
	}
	require.LessOrEqual(t, maxChunks, 2, "an acknowledgement at the front writes at most the chunk the inline part ends in")
	require.Less(t, chunkBytes, recBytes, "the chunk an acknowledgement rewrites is smaller than the record a checkpoint writes anyway")
}
