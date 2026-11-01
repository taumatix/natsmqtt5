package natsmqtt5_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5/v2"
	"github.com/taumatix/natsmqtt5/v2/packet"
)

// A session record carries at most 4096 delivered ids in its value
// (maxStoredDelivered). The replay cursor promises every id above it, so a
// record that could not hold them all used to be written without the cursor,
// and the broker that restored it replayed from the time rewind and lost a
// message older than that [MQTT-4.4.0-1]. The ids past the bound now go to
// chunks in the payload bucket, beside the record, and come back with it.
//
//	[MQTT-4.4.0-1] "When a Client reconnects with Clean Start set to 0 and a
//	  session is present, both the Client and Server MUST resend any
//	  unacknowledged PUBLISH packets ..." — what the session was owed.
//	[MQTT-3.1.2-4] Clean Start discards the Session State, chunks with it.

// deliverAboveTheCursor has the rig's subscriber receive and complete n QoS 1
// messages published in batches, after the one whose live copy never came.
func deliverAboveTheCursor(t *testing.T, r *cursorRig, n int) {
	t.Helper()
	pub := dialRaw(t, r.addr)
	pub.connect(rawConnect(r.id+"-batch", 0))
	for sent := 0; sent < n; {
		batch := min(250, n-sent)
		for i := range batch {
			pub.send(&packet.Publish{Topic: r.topic, QoS: packet.QoS1, PacketID: uint16(i + 1),
				Payload: []byte(fmt.Sprintf("after-%05d", sent+i))})
		}
		for range batch {
			_, ok := pub.read().(*packet.Puback)
			require.True(t, ok)
		}
		for i := range batch {
			completeOne(t, r.sub, fmt.Sprintf("after-%05d", sent+i))
		}
		sent += batch
	}
}

func TestMoreThan4096IDsAboveTheCursorSurviveARestartWithTheCursor_MQTT_4_4_0_1(t *testing.T) {
	// No scan moves the cursor up: all the ids stay above it, as they do for a
	// client that is behind a message that never arrived.
	natsmqtt5.SetCursorScanAfter(t, 1<<30)
	natsURL := startNATS(t)
	r := newCursorRig(t, natsURL, "cur-big", packet.QoS1)

	r.send("before")
	r.sendWithheld("lost")
	const above = 4096 + 300
	deliverAboveTheCursor(t, r, above)
	r.dropPastTheRewind()
	r.stop()

	rec := storedSession(t, natsURL, r.id)
	assert.NotZero(t, rec["ReplayCursor"], "the record carries the cursor although the ids outgrew its value")
	assert.Len(t, rec["Delivered"], 4096, "the value holds the bound")
	assert.NotEmpty(t, rec["DeliveredSpill"], "and the rest are in chunks beside it")
	chunks := len(blobKeys(t, natsURL))
	assert.Equal(t, len(rec["DeliveredSpill"].([]any)), chunks, "the chunks are all the bucket holds")

	addrB := startBroker(t, natsURL, persistentWithQueue, rewindOf(cursorRewind))
	assert.Equal(t, []string{"lost"}, r.resume(addrB),
		"[MQTT-4.4.0-1] the cursor reached the broker that restored the session, so the message older than the "+
			"rewind is sent, and none of the %d it completed comes back", above)
}

// The chunks go with the session: Clean Start discards the Session State
// [MQTT-3.1.2-4], and a record that stops naming a chunk has it deleted.
func TestTheChunksOfDeliveredIDsAreDeletedWithTheSession_MQTT_3_1_2_4(t *testing.T) {
	natsmqtt5.SetCursorScanAfter(t, 1<<30)
	natsmqtt5.SetInlineDeliveredBound(t, 100)
	natsURL := startNATS(t)
	r := newCursorRig(t, natsURL, "cur-gone", packet.QoS1)

	r.sendWithheld("lost")
	deliverAboveTheCursor(t, r, 600)
	r.dropPastTheRewind()
	r.stop()
	require.NotEmpty(t, blobKeys(t, natsURL), "the released record has chunks")

	addrB := startBroker(t, natsURL, persistentWithQueue, rewindOf(cursorRewind))
	fresh := dialRaw(t, addrB)
	ack := fresh.connect(&packet.Connect{ClientID: r.id, CleanStart: true, KeepAlive: 30})
	assert.False(t, ack.SessionPresent)
	assert.Empty(t, blobKeys(t, natsURL), "Clean Start discards the session and its chunks")
}

// A record with fewer ids than the bound is the format it always was: no
// chunk, no new key.
func TestARecordWithFewIDsHasNoChunks_MQTT_4_4_0_1(t *testing.T) {
	natsmqtt5.SetCursorScanAfter(t, 1<<30)
	natsURL := startNATS(t)
	r := newCursorRig(t, natsURL, "cur-small", packet.QoS1)

	r.sendWithheld("lost")
	deliverAboveTheCursor(t, r, 50)
	r.dropPastTheRewind()
	r.stop()

	rec := storedSession(t, natsURL, r.id)
	assert.NotZero(t, rec["ReplayCursor"])
	assert.NotContains(t, rec, "DeliveredSpill")
	assert.Empty(t, blobKeys(t, natsURL))

	addrB := startBroker(t, natsURL, persistentWithQueue, rewindOf(cursorRewind))
	assert.Equal(t, []string{"lost"}, r.resume(addrB))
}

// blobKeys lists the keys in the payload bucket, none when it does not exist.
func blobKeys(t *testing.T, natsURL string) []string {
	t.Helper()
	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	kv, err := js.KeyValue(context.Background(), natsmqtt5.DefaultStreamPrefix+"_inflight")
	if err != nil {
		return nil
	}
	keys, err := kv.Keys(context.Background())
	if err != nil {
		return nil
	}
	return keys
}
