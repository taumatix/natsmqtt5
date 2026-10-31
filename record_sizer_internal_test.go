package natsmqtt5

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRecordSizerMatchesEncodeRecord holds the estimate spillRecordReusing searches with to the bytes
// encodeRecord writes, for every split of a record's entries and several chunk counts.
func TestRecordSizerMatchesEncodeRecord(t *testing.T) {
	all := fullWindow(40)
	all[3].Pub = []byte(`<"&> quotes and \ slashes`)
	all[7].Pub = nil
	rec := &sessionRecord{ClientID: "sizer", Inflight: all}
	sizes := make([]int, len(all))
	for i, e := range all {
		raw, _ := json.Marshal(e)
		sizes[i] = len(raw) + 1
	}
	size := newRecordSizer(rec, sizes)
	require.Len(t, rec.Inflight, len(all), "sizing leaves the record's entries as they were")
	for keep := 0; keep <= len(all); keep++ {
		for _, chunks := range []int{0, 1, 2, 9} {
			rec.Inflight, rec.Spill = all[:keep], nil
			for range chunks {
				rec.Spill = append(rec.Spill, strings.Repeat("x", spillKeyLen))
			}
			require.Equal(t, len(encodeRecord(rec)), size.of(keep, chunks), "keep %d chunks %d", keep, chunks)
		}
	}
}
