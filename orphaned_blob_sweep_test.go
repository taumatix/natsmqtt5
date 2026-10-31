package natsmqtt5_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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

// A payload stashed for a session that has no record (the session was discarded
// around the write, or its cleanup was lost) is deleted by the store sweep once
// it is older than the grace period, while the payloads of a session that still
// has a record, and any too young to judge, stay.
//
//	[MQTT-4.4.0-1] a live session's unacknowledged message is still resent whole.
func TestTheSweepDeletesAnOrphanedPayloadAndKeepsALiveSessions(t *testing.T) {
	natsURL := startNATS(t)
	fast := func(o *natsmqtt5.Options) {
		o.PersistentSessions, o.DisableOfflineQueue = true, true
		o.SessionCheckpointInterval = 100 * time.Millisecond
	}
	b, addr, _ := startBrokerHandle(t, natsURL, fast)

	big := bytes.Repeat([]byte("k"), 100<<10)
	pub, _ := connectClient(t, addr, connectOpts("pub-orphan"))
	pub.publish(&paho.Publish{Topic: "or/a", QoS: 1, Retain: true, Payload: big})
	time.Sleep(200 * time.Millisecond)

	c := dialRaw(t, addr)
	c.connect(rawConnect("orphan-live", 300))
	c.subscribe("or/#", packet.QoS1)
	first := c.expectPublish()
	require.Eventually(t, func() bool { return len(inflightKeys(t, natsURL)) == 1 },
		5*time.Second, 50*time.Millisecond, "the live session keeps its payload")
	c.drop()

	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	kv, err := js.KeyValue(context.Background(), natsmqtt5.DefaultStreamPrefix+"_inflight")
	require.NoError(t, err)
	sum := sha256.Sum256([]byte("lost"))
	orphan := natsmqtt5.BlobOwner("ghost") + "." + hex.EncodeToString(sum[:])
	_, err = kv.Put(context.Background(), orphan, []byte("lost"))
	require.NoError(t, err)

	require.NoError(t, natsmqtt5.SweepStore(context.Background(), b))
	assert.Len(t, inflightKeys(t, natsURL), 2, "a payload younger than the grace period is not judged")

	natsmqtt5.SetBlobGrace(b, time.Millisecond)
	time.Sleep(10 * time.Millisecond)
	require.NoError(t, natsmqtt5.SweepStore(context.Background(), b))
	keys := inflightKeys(t, natsURL)
	require.Len(t, keys, 1, "the orphan is deleted")
	assert.NotEqual(t, orphan, keys[0])

	back := dialRaw(t, addr)
	require.True(t, back.connect(rawConnect("orphan-live", 300)).SessionPresent)
	got := back.expectPublish()
	assert.Equal(t, first.PacketID, got.PacketID)
	assert.Equal(t, big, got.Payload, "[MQTT-4.4.0-1] the live session's payload survived the sweep")
}

// A session attached to this broker: its record can be a step behind the payloads the broker holds,
// so the sweep judges that owner's payloads by what the broker holds in memory as well as by the
// record. A payload under the owner that neither names, and older than the grace period, is deleted
// (before, it waited for the bucket's TTL); the payload of the unacknowledged message stays, and
// the message is resent whole. A session attached to another broker is that broker's to judge: its
// payloads stay.
//
//	[MQTT-4.4.0-1] "When a Client reconnects with Clean Start set to 0 and a session is present,
//	  both the Client and Server MUST resend any unacknowledged PUBLISH packets (where QoS > 0) and
//	  PUBREL packets using their original Packet Identifiers."
func TestTheSweepDeletesAnAttachedSessionsUnnamedPayloadAndKeepsTheOneItHolds_MQTT_4_4_0_1(t *testing.T) {
	natsURL := startNATS(t)
	fast := func(o *natsmqtt5.Options) {
		o.PersistentSessions, o.DisableOfflineQueue = true, true
		o.SessionCheckpointInterval = 100 * time.Millisecond
	}
	b, addr, _ := startBrokerHandle(t, natsURL, fast)
	_, addrOther, _ := startBrokerHandle(t, natsURL, fast)

	big := bytes.Repeat([]byte("k"), 100<<10)
	pub, _ := connectClient(t, addr, connectOpts("pub-attached"))
	pub.publish(&paho.Publish{Topic: "at/a", QoS: 1, Retain: true, Payload: big})

	here := dialRaw(t, addr)
	here.connect(rawConnect("attached-here", 300))
	here.subscribe("at/#", packet.QoS1)
	first := here.expectPublish()
	there := dialRaw(t, addrOther)
	there.connect(rawConnect("attached-there", 300))
	there.subscribe("at/#", packet.QoS1)
	there.expectPublish()
	require.Eventually(t, func() bool { return len(inflightKeys(t, natsURL)) == 2 },
		5*time.Second, 50*time.Millisecond, "both sessions hold the same payload, under their own owner")

	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	kv, err := js.KeyValue(context.Background(), natsmqtt5.DefaultStreamPrefix+"_inflight")
	require.NoError(t, err)
	plant := func(clientID, payload string) string {
		sum := sha256.Sum256([]byte(payload))
		key := natsmqtt5.BlobOwner(clientID) + "." + hex.EncodeToString(sum[:])
		_, err := kv.Put(context.Background(), key, []byte(payload))
		require.NoError(t, err)
		return key
	}
	unnamedHere := plant("attached-here", "unnamed")
	unnamedThere := plant("attached-there", "unnamed")

	natsmqtt5.SetBlobGrace(b, time.Millisecond)
	time.Sleep(10 * time.Millisecond)
	require.NoError(t, natsmqtt5.SweepStore(context.Background(), b))
	keys := inflightKeys(t, natsURL)
	assert.NotContains(t, keys, unnamedHere, "the payload neither the record nor the broker names is deleted")
	assert.Contains(t, keys, unnamedThere, "another broker's attached session is left to that broker")
	assert.Len(t, keys, 3, "the payloads both sessions hold stay: %v", keys)

	here.drop()
	back := dialRaw(t, addr)
	require.True(t, back.connect(rawConnect("attached-here", 300)).SessionPresent)
	got := back.expectPublish()
	assert.Equal(t, first.PacketID, got.PacketID)
	assert.Equal(t, big, got.Payload, "[MQTT-4.4.0-1] the attached session's payload survived the sweep")
}
