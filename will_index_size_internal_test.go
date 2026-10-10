package natsmqtt5

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/taumatix/natsmqtt5/packet"
)

func heapAfterGC() uint64 {
	var m runtime.MemStats
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// The Will index holds a decoded record per Will-carrying connection in every broker.
// This builds the same map the watcher builds, from the same encoded bytes, and reports
// the heap per record so the cost at a given cluster size is a number rather than a guess.
// The ceiling is loose: it is there to notice a record becoming several times heavier.
func TestWillIndexHeapPerRecord(t *testing.T) {
	for _, n := range []int{5000, 50000} {
		encoded := make([][]byte, n)
		for i := range encoded {
			id := fmt.Sprintf("client-%d", i)
			encoded[i] = encodeWillRecord(&willRecord{
				Version: willRecordVersion, ClientID: id, Owner: "broker-0123456789abcdef",
				Will:  &packet.Will{Topic: "status/" + id, Payload: []byte("offline"), QoS: 1},
				Delay: time.Second,
			})
		}
		before := heapAfterGC()
		index := make(map[string]*willLease, 0)
		for i, b := range encoded {
			rec, err := decodeWillRecord(b)
			if err != nil {
				t.Fatal(err)
			}
			key := willKeyPrefix(rec.ClientID) + fmt.Sprintf("nuid%018d", i)
			index[key] = &willLease{key: key, rec: *rec, rev: uint64(i)}
		}
		after := heapAfterGC()
		per := float64(after-before) / float64(n)
		t.Logf("%d records: %.0f B each, %.1f MiB in all (record JSON is %d B)", n, per, float64(after-before)/(1<<20), len(encoded[0]))
		if per > 1500 {
			t.Errorf("%.0f B per indexed record, want under 1500", per)
		}
		runtime.KeepAlive(index)
	}
}
