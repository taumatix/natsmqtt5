package natsmqtt5_test

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// requestTally counts what reaches a three-server cluster's JetStream API and its KV buckets,
// by the first tokens of the subject, through a plain core subscription on another server.
type requestTally struct {
	mu     sync.Mutex
	counts map[string]int
}

func tallyRequests(t *testing.T, natsURL string) *requestTally {
	t.Helper()
	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	r := &requestTally{counts: map[string]int{}}
	for _, subject := range []string{"$JS.API.>", "$KV.>"} {
		_, err = nc.Subscribe(subject, func(m *nats.Msg) {
			parts := strings.Split(m.Subject, ".")
			keep := 4 // $JS.API.STREAM.INFO, $JS.API.DIRECT.GET, $JS.API.CONSUMER.CREATE
			if parts[0] == "$KV" {
				keep = 2 // $KV.<bucket>
			}
			r.mu.Lock()
			r.counts[strings.Join(parts[:min(keep, len(parts))], ".")]++
			r.mu.Unlock()
		})
		require.NoError(t, err)
	}
	require.NoError(t, nc.Flush())
	return r
}

// since returns what was counted after the previous call, and resets.
func (r *requestTally) since() map[string]int {
	time.Sleep(200 * time.Millisecond) // the subscription is on another server
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.counts
	r.counts = map[string]int{}
	return out
}

func tallyString(m map[string]int) string {
	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&b, "\n\t%-50s %d", k, m[k])
	}
	return b.String()
}

// The cost the replay cursor adds on a three-way replicated stream across three real NATS
// servers, which an embedded server cannot show: every SUBSCRIBE reads the queue's last
// sequence (a request to the stream's leader), every 1024 deliveries a scan reads the session's
// subjects, and ids past the record's bound are written as chunks that wait for a majority.
// It logs the requests and writes counted at another server; what it asserts is the bound a
// deployment can rely on, and that the session still gets the message it was owed
// [MQTT-4.4.0-1] after the work.
//
// Measured on 2026-11-01 (three nats:2-alpine servers in Apple containers on one Mac, R3 queue):
// a SUBSCRIBE is one stream info request (3-7 ms); 4496 deliveries behind a hole take 2.5 s with
// the scans on (33 consumer creates, 25 deletes, 8 stream infos: about 8 creates per 1024
// deliveries) and 3.0 s with them off, so the scans are not measurable in the delivery rate; the
// 400 ids past the record's bound are written as 2 chunks, at the checkpoint and the release.
func TestClusterReplayCursorCostPerSubscribeAndPerDelivery_MQTT_4_4_0_1(t *testing.T) {
	c := startNATSCluster(t)
	tally := tallyRequests(t, c.URL(2))

	t.Run("a SUBSCRIBE costs one stream info request", func(t *testing.T) {
		r := newCursorRig(t, c.URL(0), "cur-cost-sub", packet.QoS1, clusterReplicas)
		awaitCursor(t, r)
		tally.since()
		const subscribes = 30
		start := time.Now()
		for i := range subscribes {
			r.sub.subscribe(fmt.Sprintf("cur-cost-sub/extra/%d", i), packet.QoS1)
		}
		per := time.Since(start) / subscribes
		got := tally.since()
		t.Logf("%d SUBSCRIBEs on a replicated queue: %s each; requests:%s", subscribes, per.Round(time.Millisecond), tallyString(got))
		require.Equal(t, subscribes, got["$JS.API.STREAM.INFO"], "one stream info request per SUBSCRIBE")
	})

	// Deliveries behind a message whose live copy never came. With the scans on they move the
	// cursor up and the record stays small; off, the ids pile up above it and the record
	// carries chunks.
	for _, tc := range []struct {
		name      string
		scanAfter int
		chunks    bool
	}{
		{"deliveries with the scans on", 1024, false},
		{"deliveries with the scans off write chunks", 1 << 30, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			natsmqtt5.SetCursorScanAfter(t, tc.scanAfter)
			id := fmt.Sprintf("cur-cost-%d", tc.scanAfter)
			r := newCursorRig(t, c.URL(0), id, packet.QoS1, clusterReplicas)
			awaitCursor(t, r)
			r.sendWithheld("lost")
			tally.since()
			const delivered = 4096 + 400
			start := time.Now()
			deliverAboveTheCursor(t, r, delivered)
			took := time.Since(start)
			got := tally.since()
			t.Logf("%d deliveries behind a hole: %s (%s each); requests and writes:%s", delivered, took.Round(time.Millisecond),
				(took / delivered).Round(time.Microsecond), tallyString(got))

			// The record and its chunks are written at a checkpoint and when the connection ends. A
			// chunk has the same key as its content, so one a checkpoint already wrote during the
			// deliveries is not written again at the end: count both windows.
			duringDeliveries := got["$KV."+natsmqtt5.DefaultStreamPrefix+"_inflight"]
			r.dropPastTheRewind()
			r.stop()
			got = tally.since()
			chunkWrites := duringDeliveries + got["$KV."+natsmqtt5.DefaultStreamPrefix+"_inflight"]
			t.Logf("deliveries and the connection ending: payload bucket writes (chunks): %d for %d ids; requests and writes at the end:%s",
				chunkWrites, delivered, tallyString(got))
			if tc.chunks {
				require.Positive(t, chunkWrites)
			}
			require.Less(t, chunkWrites, delivered/10, "ids are written in chunks, not one write per delivery")

			addrB := startBroker(t, c.URL(1), persistentWithQueue, rewindOf(cursorRewind), clusterReplicas)
			require.Equal(t, []string{"lost"}, r.resume(addrB),
				"[MQTT-4.4.0-1] the session is sent what it was owed, and none of the %d it completed", delivered)
		})
	}
}
