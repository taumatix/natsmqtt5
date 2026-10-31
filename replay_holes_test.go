package natsmqtt5_test

import (
	"encoding/json"
	"strconv"
	"sync"
	"testing"

	"github.com/eclipse/paho.golang/paho"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// queueReadStarts records the start sequence of every consumer the NATS server at natsURL is
// asked to create on the offline queue stream (0 for one that does not start at a sequence).
type queueReadStarts struct {
	mu     sync.Mutex
	starts []uint64
}

func watchQueueReadStarts(t *testing.T, natsURL string) *queueReadStarts {
	t.Helper()
	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	w := &queueReadStarts{}
	_, err = nc.Subscribe("$JS.API.CONSUMER.CREATE."+natsmqtt5.DefaultStreamPrefix+"_queue.>", func(m *nats.Msg) {
		var req struct {
			Config struct {
				OptStartSeq uint64 `json:"opt_start_seq"`
			} `json:"config"`
		}
		_ = json.Unmarshal(m.Data, &req)
		w.mu.Lock()
		w.starts = append(w.starts, req.Config.OptStartSeq)
		w.mu.Unlock()
	})
	require.NoError(t, err)
	require.NoError(t, nc.Flush())
	return w
}

func (w *queueReadStarts) since(n int) []uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]uint64(nil), w.starts[n:]...)
}

func (w *queueReadStarts) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.starts)
}

// A hole is read by its sequence, not by reading the stream from it: the resume sends the hole
// [MQTT-4.4.0-1] ("When a Client reconnects with Clean Start set to 0 and a session is present,
// both the Client and Server MUST resend any unacknowledged PUBLISH packets (where QoS > 0) ...")
// and nothing the client completed [MQTT-4.3.3-2], and no read it makes of the queue starts at
// or below the hole, so a hole kept for hours does not cost the span above it on every resume.
func TestAResumeReadsAHoleBySequenceAndTheRestFromTheCursor_MQTT_4_4_0_1_MQTT_4_3_3_2(t *testing.T) {
	natsmqtt5.SetCursorScanAfter(t, 1)
	natsURL := startNATS(t)
	r := newCursorRig(t, natsURL, "cur-holeseq", packet.QoS2)
	reads := watchQueueReadStarts(t, natsURL)

	r.send("before")
	lost := seqOf(t, r.sendWithheld("lost"))
	for i := range 20 {
		r.send("after" + strconv.Itoa(i))
	}
	awaitHole(t, r.b, r.id, lost)
	r.dropPastTheRewind()

	before := reads.count()
	assert.Equal(t, []string{"lost"}, r.resume(r.addr),
		"[MQTT-4.4.0-1] the hole is replayed; [MQTT-4.3.3-2] nothing completed comes back as new")
	for _, start := range reads.since(before) {
		assert.Greater(t, start, lost, "the resume read the queue from the hole (seq %d) on: %v", lost, reads.since(before))
	}
}

// A fetch that comes back empty is not the end of the stream: on a loaded cluster an ordered
// consumer, which nats.go makes afresh for every fetch, can answer a fetch with nothing although
// messages it matches are stored (a session lost the one message it was owed in CI's cluster job).
// The replay must keep reading until the consumer has nothing pending, and send what it was owed
// [MQTT-4.4.0-1]. SetEmptyQueueFetches stands in for the loaded server: the next fetches of the
// broker's queue reads come back empty.
func TestAResumeWhoseFirstReadsComeBackEmptyStillSendsWhatItWasOwed_MQTT_4_4_0_1(t *testing.T) {
	natsURL := startNATS(t)
	r := newCursorRig(t, natsURL, "cur-empty-fetch", packet.QoS1)
	r.send("before")
	r.sub.drop()
	waitDetached(t, r.logs, r.id)
	r.pub.publish(&paho.Publish{Topic: r.topic, QoS: 1, Payload: []byte("owed")})

	natsmqtt5.SetEmptyQueueFetches(r.b, 2)
	assert.Equal(t, []string{"owed"}, r.resume(r.addr),
		"[MQTT-4.4.0-1] the message stored while the session was away is sent on its resume")
}

// Nor is a consumer's "nothing pending" the end when its server is a replica of the stream that is
// still catching up after a restart: it has stored less than the leader, so it reports nothing
// pending at a position short of the stream's end. The read asks the leader first; taking the
// replica's word, ReconnectCatchUp sent a session none of what was stored while its NATS server
// was down (CI's cluster job). SetStaleQueueConsumerInfos stands in for the replica.
func TestAResumeWhoseConsumerServerIsBehindTheStreamStillSendsWhatItWasOwed_MQTT_4_4_0_1(t *testing.T) {
	natsURL := startNATS(t)
	r := newCursorRig(t, natsURL, "cur-stale-info", packet.QoS1)
	r.send("before")
	r.sub.drop()
	waitDetached(t, r.logs, r.id)
	r.pub.publish(&paho.Publish{Topic: r.topic, QoS: 1, Payload: []byte("owed")})

	natsmqtt5.SetEmptyQueueFetches(r.b, 1)
	natsmqtt5.SetStaleQueueConsumerInfos(r.b, 1)
	assert.Equal(t, []string{"owed"}, r.resume(r.addr),
		"[MQTT-4.4.0-1] the message stored while the session was away is sent on its resume")
}

// A fetch the NATS server answers with no responders is not a failed read either. Every fetch of an
// ordered consumer makes a new consumer, which a cluster can place on another server; a pull that
// reaches the broker's server before the new consumer's interest does is answered 503. Taken for a
// failure, it ended the resume's read with nothing sent, and the session waited for a message it was
// owed [MQTT-4.4.0-1] until its next resume (CI's cluster job, one resume in a few hundred).
// SetUnansweredQueueFetches stands in for the cluster.
func TestAResumeWhoseFirstReadsGoUnansweredStillSendsWhatItWasOwed_MQTT_4_4_0_1(t *testing.T) {
	natsURL := startNATS(t)
	r := newCursorRig(t, natsURL, "cur-no-responders", packet.QoS1)
	r.send("before")
	r.sub.drop()
	waitDetached(t, r.logs, r.id)
	r.pub.publish(&paho.Publish{Topic: r.topic, QoS: 1, Payload: []byte("owed")})

	natsmqtt5.SetUnansweredQueueFetches(r.b, 2)
	assert.Equal(t, []string{"owed"}, r.resume(r.addr),
		"[MQTT-4.4.0-1] the message stored while the session was away is sent on its resume")
}
