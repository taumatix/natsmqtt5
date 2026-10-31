package natsmqtt5_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// The sweep of the Will bucket is an extension, not a numbered MUST: no [MQTT-x.y.z-n] statement
// says how long a server keeps a Will it has failed to publish. What it must not disturb are
// [MQTT-3.1.2-8] and [MQTT-3.1.2-10] (a Will is published after the Network Connection closes and
// once the Will Delay Interval has elapsed; its stored form is removed once published), so the
// tests assert that a Will still waiting out its delay, and the Will of a connection still open,
// are left alone, and that only a record overdue by more than the sweep age goes.

func willBucket(t *testing.T, natsURL string) jetstream.KeyValue {
	t.Helper()
	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	kv, err := js.KeyValue(context.Background(), "MQTT5_wills")
	require.NoError(t, err)
	return kv
}

// putWillRecord writes a Will record the way a broker's would be encoded. The test writes it
// because the faults that leave one behind past its due time cannot be driven through the
// broker's interface; it stands in for a record a fault leaked.
func putWillRecord(t *testing.T, kv jetstream.KeyValue, key string, rec map[string]any) {
	t.Helper()
	rec["Version"] = 1
	rec["ClientID"] = "leaked"
	rec["Will"] = &packet.Will{Topic: "status/leaked", Payload: []byte("late"), QoS: packet.QoS1}
	b, err := json.Marshal(rec)
	require.NoError(t, err)
	_, err = kv.Create(context.Background(), key, b)
	require.NoError(t, err)
}

func hasKey(kv jetstream.KeyValue, key string) bool {
	_, err := kv.Get(context.Background(), key)
	return err == nil
}

// A record that ended and was due longer ago than the sweep age is deleted without being
// published. One still waiting out its delay, and one of a connection that is open, stay,
// however short the sweep age is: the bucket has no time-to-live because an open connection's
// record may be years old.
func TestWillBucketSweepDeletesOnlyARecordOverdueByMoreThanTheSweepAge(t *testing.T) {
	url := startNATS(t)
	b, addr, _ := startBrokerHandle(t, url, fastWills)
	natsmqtt5.SetWillSweepAfter(b, time.Minute)

	watcher, _ := connectClient(t, addr, connectOpts("watcher"))
	watcher.subscribe(paho.SubscribeOptions{Topic: "status/#", QoS: 1})

	// An open connection with a Will, and one that dropped and is waiting out a long delay.
	connectClient(t, addr, durableWillConnect("open", "status/open", "gone", 0, 0))
	waiting, _ := connectClient(t, addr, durableWillConnect("waiting", "status/waiting", "gone", 3600, 7200))
	waiting.dropConnection()

	kv := willBucket(t, url)
	// The owner does not answer, so a survivor would adopt this and publish it late; the sweep
	// runs on every check, long before the silence has lasted long enough to adopt.
	putWillRecord(t, kv, "leaked.overdue", map[string]any{
		"Owner": "ghost", "Ended": true, "DueAt": time.Now().Add(-2 * time.Hour)})
	// Due a moment ago: overdue, but not by more than the sweep age.
	putWillRecord(t, kv, "leaked.recent", map[string]any{
		"Owner": "ghost2", "Ended": true, "DueAt": time.Now().Add(-10 * time.Second)})
	// Not ended: the connection is open as far as anyone can tell, however old the record.
	putWillRecord(t, kv, "leaked.open", map[string]any{
		"Owner": "ghost3", "Ended": false, "DueAt": time.Now().Add(-2 * time.Hour)})

	require.Eventually(t, func() bool { return !hasKey(kv, "leaked.overdue") }, 10*time.Second, 50*time.Millisecond,
		"the overdue record was never swept")
	// The sweep deletes; it does not publish. Within the time a survivor waits before adopting
	// a silent owner's records, which the other two ghosts below would otherwise run into.
	watcher.expectNoMessageFor(300 * time.Millisecond)

	assert.True(t, hasKey(kv, "leaked.recent"), "a record overdue by less than the sweep age is kept")
	assert.True(t, hasKey(kv, "leaked.open"), "a record not marked ended is never swept")
	// The two real connections' records are untouched.
	assert.Equal(t, 4, willKeys(t, url), "open, waiting, recent and open-looking records remain")
}

// The README says the DurablePublish flush confirms that the NATS server received the live
// publish and says nothing about JetStream: the live copy is core NATS and the only stored
// copy is the queue's. This shows it on the wire: an acknowledged QoS 1 message appears on two
// subjects, and no stream captures the live one, while the queue stream captures the other.
// (An extension; it touches [MQTT-4.3.2-2], at-least-once delivery, only in that the stored
// copy is what a session that was away is given.)
func TestDurablePublishFlushCoversTheLiveCopyAndOnlyTheQueueStoresIt(t *testing.T) {
	url := startNATS(t)
	addr := durableBroker(t, url)

	nc, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	seen := make(chan string, 8)
	_, err = nc.Subscribe(natsmqtt5.DefaultSubjectPrefix+".>", func(m *nats.Msg) { seen <- m.Subject })
	require.NoError(t, err)
	require.NoError(t, nc.Flush())

	pub := dialRaw(t, addr)
	pub.connect(rawConnect("flush-pub", 0))
	rawPublishQoS(pub, packet.QoS1, 3, "dp/cover", "x")
	ack, ok := pub.read().(*packet.Puback)
	require.True(t, ok)
	require.Equal(t, packet.Success, ack.ReasonCode)

	// The flush came back, so the NATS server has the live publish: it reached a core subscriber.
	require.NoError(t, nc.Flush())
	live := natsmqtt5.DefaultSubjectPrefix + ".dp.cover"
	queued := natsmqtt5.DefaultSubjectPrefix + ".$queue.dp.cover"
	got := map[string]bool{}
	require.Eventually(t, func() bool {
		for len(seen) > 0 {
			got[<-seen] = true
		}
		return got[live] && got[queued]
	}, 5*time.Second, 10*time.Millisecond, "saw only %v", got)
	require.True(t, got[live], "the live copy reached the NATS server; saw %v", got)
	require.True(t, got[queued], "the queue copy was published; saw %v", got)

	js, err := jetstream.New(nc)
	require.NoError(t, err)
	name, err := js.StreamNameBySubject(context.Background(), queued)
	require.NoError(t, err)
	assert.Equal(t, natsmqtt5.DefaultStreamPrefix+"_queue", name, "the stored copy is the queue's")
	_, err = js.StreamNameBySubject(context.Background(), live)
	assert.ErrorIs(t, err, jetstream.ErrStreamNotFound, "no stream stores the live copy, so the flush confirms nothing about JetStream")
}
