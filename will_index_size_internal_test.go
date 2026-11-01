package natsmqtt5

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/taumatix/natsmqtt5/v2/packet"
)

func heapAfterGC() uint64 {
	var m runtime.MemStats
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// The Will index holds a willRef per Will-carrying connection in every broker, whatever the
// size of the Will. This builds the same map the watcher builds, from the same encoded bytes,
// and reports the heap per record so the cost at a given cluster size is a number rather than
// a guess. Before the index held only references (2026-10-30) a record cost about 330 B plus
// its Will: 16 MiB at 50,000 records of a 7-byte payload, and 3 GiB at 50,000 of 64 KiB.
// The ceiling is loose: it is there to notice a record growing with its payload again.
func TestWillIndexHeapPerRecord(t *testing.T) {
	for _, tc := range []struct{ n, payload int }{{5000, 7}, {20000, 7}, {500, 64 * 1024}} {
		encoded := make([][]byte, tc.n)
		for i := range encoded {
			id := fmt.Sprintf("client-%d", i)
			encoded[i] = encodeWillRecord(&willRecord{
				Version: willRecordVersion, ClientID: id, Owner: "broker-0123456789abcdef",
				Will:  &packet.Will{Topic: "status/" + id, Payload: make([]byte, tc.payload), QoS: 1},
				Delay: time.Second,
			})
		}
		before := heapAfterGC()
		index := make(map[string]willRef)
		for i, b := range encoded {
			key := willKeyPrefix(fmt.Sprintf("client-%d", i)) + fmt.Sprintf("nuid%018d", i)
			ref, err := refOf(key, uint64(i), b)
			if err != nil {
				t.Fatal(err)
			}
			index[key] = ref
		}
		after := heapAfterGC()
		// Signed: a collection of other goroutines' garbage between the two readings can make it negative.
		grew := int64(after) - int64(before)
		per := float64(grew) / float64(tc.n)
		t.Logf("%d records of a %d B payload: %.0f B each, %.1f MiB in all (record JSON is %d B)",
			tc.n, tc.payload, per, float64(grew)/(1<<20), len(encoded[0]))
		if per > 400 {
			t.Errorf("%.0f B per indexed record, want under 400 whatever the payload", per)
		}
		runtime.KeepAlive(index)
	}
}
