package natsmqtt5_test

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// The chunks of delivered ids beside a session record (replay_cursor_spill_test.go) fail in two ways a
// unit test can only simulate: the payload bucket cannot be written when the session is released, and a
// chunk the record names is gone when it is restored. Both are real faults here: the bucket is deleted
// from the JetStream server, and a chunk is purged from it, while a real broker serves a real client over
// a socket. Either way the record must not promise ids it does not hold [MQTT-4.4.0-1], so it carries no
// cursor and the replay starts at the time rewind, as for a record written before the cursor existed.
// The rest of the session survives.

// inflightBucket opens the payload bucket the chunks are written to.
func inflightBucket(t *testing.T, natsURL string) jetstream.KeyValue {
	t.Helper()
	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	kv, err := js.KeyValue(context.Background(), natsmqtt5.DefaultStreamPrefix+"_inflight")
	require.NoError(t, err)
	return kv
}

// A release whose chunks cannot be written (the payload bucket is gone) writes the record without the
// cursor and without chunk names, and says so in the log; the next broker resumes the session from the
// time rewind.
func TestARecordWhoseChunksCannotBeWrittenCarriesNoCursor_MQTT_4_4_0_1(t *testing.T) {
	natsmqtt5.SetCursorScanAfter(t, 1<<30)
	natsmqtt5.SetInlineDeliveredBound(t, 100)
	natsURL := startNATS(t)
	r := newCursorRig(t, natsURL, "cur-nowrite", packet.QoS1)

	// A first absence writes chunks, which creates the payload bucket the broker keeps open.
	r.sendWithheld("lost")
	deliverAboveTheCursor(t, r, 300)
	r.dropPastTheRewind()
	require.NotEmpty(t, blobKeys(t, natsURL), "the first release wrote chunks")

	// The session comes back to the same broker and is sent a different set of ids, whose chunks
	// differ from those already stored; then the bucket is deleted under the broker.
	r.sub = dialRaw(t, r.addr)
	require.True(t, r.sub.connect(rawConnect(r.id, 300)).SessionPresent)
	assert.Equal(t, []string{"lost"}, r.sub.completeFor(time.Second))
	r.sendWithheld("lost-again")
	deliverAboveTheCursor(t, r, 300)
	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	require.NoError(t, js.DeleteKeyValue(context.Background(), natsmqtt5.DefaultStreamPrefix+"_inflight"))

	r.dropPastTheRewind()
	r.stop()

	rec := storedSession(t, natsURL, r.id)
	assert.NotContains(t, rec, "ReplayCursor", "ids that were not written must not be promised by a cursor")
	assert.NotContains(t, rec, "DeliveredSpill")
	assert.Contains(t, r.logs.String(), "delivered ids could not be kept beside the session record")

	addrB := startBroker(t, natsURL, persistentWithQueue, rewindOf(cursorRewind))
	back := dialRaw(t, addrB)
	require.True(t, back.connect(rawConnect(r.id, 300)).SessionPresent, "the session itself survives")
	// Without the cursor the replay reaches back only one rewind, which is how a record without one
	// always behaved: the message older than the rewind is not found.
	assert.NotContains(t, back.completeFor(time.Second), "lost-again")
}

// A chunk the record names that cannot be read when the session is restored drops the restored cursor
// and says so; the session is restored and the replay uses the time rewind.
func TestARecordWithAnUnreadableChunkIsRestoredWithoutItsCursor_MQTT_4_4_0_1(t *testing.T) {
	natsmqtt5.SetCursorScanAfter(t, 1<<30)
	natsmqtt5.SetInlineDeliveredBound(t, 100)
	natsURL := startNATS(t)
	r := newCursorRig(t, natsURL, "cur-unreadable", packet.QoS1)

	r.sendWithheld("lost")
	deliverAboveTheCursor(t, r, 600)
	r.dropPastTheRewind()
	r.stop()

	rec := storedSession(t, natsURL, r.id)
	require.NotEmpty(t, rec["DeliveredSpill"], "the released record has chunks")
	chunk := rec["DeliveredSpill"].([]any)[0].(string)
	require.NoError(t, inflightBucket(t, natsURL).Purge(context.Background(), chunk))

	var logsB syncBuffer
	addrB := startBroker(t, natsURL, persistentWithQueue, rewindOf(cursorRewind), loggingBroker(t, &logsB))
	back := dialRaw(t, addrB)
	require.True(t, back.connect(rawConnect(r.id, 300)).SessionPresent, "the session is restored")
	got := back.completeFor(time.Second)
	assert.Contains(t, logsB.String(), "some delivered ids of a restored session could not be read")
	assert.NotContains(t, got, "lost", "with the cursor dropped the replay reaches back only one rewind")
}
