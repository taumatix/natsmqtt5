package natsmqtt5_test

import (
	"bytes"
	"context"
	"encoding/base64"
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

// A message in flight that has no copy in the offline queue (a retained message
// is the common case) and a payload over what a session record keeps inline.
// The record names the payload and the successor reads it back.
//
//	[MQTT-4.4.0-1] "When a Client reconnects with Clean Start set to 0 and a
//	  session is present, both the Client and Server MUST resend any
//	  unacknowledged PUBLISH packets (where QoS > 0) and PUBREL packets using
//	  their original Packet Identifiers."

// [MQTT-4.4.0-1]: a 100 KiB retained message sent at QoS 1 to a client of a broker
// killed with SIGKILL is resent by the broker that takes the session over, whole,
// with its Packet Identifier, DUP and RETAIN as first sent.
func TestAKilledBrokersOversizeRetainedMessageInFlightIsResent(t *testing.T) {
	natsURL := startNATS(t)
	child := startBrokerBinary(t, natsURL, "-persistent-sessions", "-session-checkpoint-interval", killedBrokerCheckpoint)
	survivor := startBroker(t, natsURL, persistentWithQueue)

	big := bytes.Repeat([]byte("0123456789abcdef"), 100<<6) // 100 KiB
	pub, _ := connectClient(t, survivor, connectOpts("pub-killed-big"))
	pub.publish(&paho.Publish{Topic: "kb/a", QoS: 1, Retain: true, Payload: big})
	// Older than the checkpoint's skew, so the successor's replay of the queue
	// does not also hold this message as one published while the broker died.
	time.Sleep(time.Second)

	c := dialRaw(t, child.addr)
	c.connect(rawConnect("killed-big", 300))
	c.subscribe("kb/#", packet.QoS1)
	first := c.expectPublish()
	require.Equal(t, big, first.Payload)
	require.True(t, first.Retain)
	time.Sleep(700 * time.Millisecond)
	child.kill9(t)

	back := dialRaw(t, survivor)
	require.True(t, back.connect(rawConnect("killed-big", 300)).SessionPresent)
	got := back.expectPublish()
	assert.Equal(t, first.PacketID, got.PacketID, "[MQTT-4.4.0-1] the original Packet Identifier")
	assert.True(t, got.Dup)
	assert.True(t, got.Retain)
	assert.Equal(t, big, got.Payload, "the whole payload, not a truncated or missing one")
	back.send(&packet.Puback{Ack: packet.Ack{PacketID: got.PacketID}})
	back.expectNothing()
}

// [MQTT-4.4.0-1]: the same for QoS 2, a PUBLISH not yet answered.
func TestAKilledBrokersOversizeQoS2RetainedMessageInFlightIsResent(t *testing.T) {
	natsURL := startNATS(t)
	child := startBrokerBinary(t, natsURL, "-persistent-sessions", "-session-checkpoint-interval", "1h")
	survivor := startBroker(t, natsURL, persistentWithQueue)

	big := bytes.Repeat([]byte("fedcba9876543210"), 100<<6)
	pub, _ := connectClient(t, survivor, connectOpts("pub-killed-big2"))
	pub.publish(&paho.Publish{Topic: "kb2/a", QoS: 2, Retain: true, Payload: big})
	time.Sleep(time.Second)

	c := dialRaw(t, child.addr)
	c.connect(rawConnect("killed-big2", 300))
	c.subscribe("kb2/#", packet.QoS2)
	first := c.expectPublish()
	require.Equal(t, big, first.Payload)
	child.kill9(t)

	back, got := afterTheKill(t, survivor, "killed-big2")
	pubs := p3Publishes(got)
	require.Len(t, pubs, 1, "[MQTT-4.4.0-1] once, with the checkpoint interval at an hour: the write before the PUBLISH")
	assert.Equal(t, first.PacketID, pubs[0].PacketID)
	assert.True(t, pubs[0].Dup)
	assert.Equal(t, big, pubs[0].Payload)
	back.send(&packet.Pubrec{Ack: packet.Ack{PacketID: first.PacketID}})
	assert.Equal(t, first.PacketID, back.expectPubrel().PacketID)
}

// A payload over what a record keeps inline stays out of the record, and a
// payload that is gone when the session is restored costs that one message, not
// the ones after it [MQTT-4.4.0-1].
func TestAnOversizePayloadStaysOutOfTheRecordAndItsLossSpoilsOnlyThatMessage(t *testing.T) {
	natsURL := startNATS(t)
	var logs syncBuffer
	noQueue := func(o *natsmqtt5.Options) { o.PersistentSessions, o.DisableOfflineQueue = true, true }
	addrA, stopA := startStoppableBroker(t, natsURL, noQueue, loggingBroker(t, &logs))
	addrB := startBroker(t, natsURL, noQueue)

	c := dialRaw(t, addrA)
	c.connect(rawConnect("big-gone", 300))
	c.subscribe("bg/#", packet.QoS1)
	pub, _ := connectClient(t, addrA, connectOpts("pub-big-gone"))
	pub.publish(&paho.Publish{Topic: "bg/large", QoS: 1, Payload: bytes.Repeat([]byte("y"), 200<<10)})
	pub.publish(&paho.Publish{Topic: "bg/small", QoS: 1, Payload: []byte("small")})
	require.Equal(t, "bg/large", c.expectPublish().Topic)
	small := c.expectPublish()
	c.drop()
	waitDetached(t, &logs, "big-gone")
	stopA()

	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	ctx := context.Background()
	sessions, err := js.KeyValue(ctx, natsmqtt5.DefaultStreamPrefix+"_sessions")
	require.NoError(t, err)
	entry, err := sessions.Get(ctx, base64.RawURLEncoding.EncodeToString([]byte("big-gone")))
	require.NoError(t, err)
	assert.Less(t, len(entry.Value()), 4<<10, "the record names the payload and does not hold it")

	blobs, err := js.KeyValue(ctx, natsmqtt5.DefaultStreamPrefix+"_inflight")
	require.NoError(t, err)
	keys, err := blobs.Keys(ctx)
	require.NoError(t, err)
	require.Len(t, keys, 1, "the payload is in the bucket, once")
	require.NoError(t, blobs.Purge(ctx, keys[0]))

	back := dialRaw(t, addrB)
	require.True(t, back.connect(rawConnect("big-gone", 300)).SessionPresent)
	got := back.expectPublish()
	assert.Equal(t, small.PacketID, got.PacketID)
	assert.Equal(t, "small", string(got.Payload))
	back.expectNothing()
}

// A payload the bucket holds for an unacknowledged message is kept for as long
// as the session is served, however long the client leaves the message
// unacknowledged: the bucket ages values out after twice the maximum Session
// Expiry Interval (4 s here) and the session writes it again before then
// [MQTT-4.4.0-1].
func TestAnOversizePayloadOfAMessageLeftUnacknowledgedIsKeptAlive(t *testing.T) {
	natsURL := startNATS(t)
	short := func(o *natsmqtt5.Options) {
		o.PersistentSessions, o.DisableOfflineQueue = true, true
		o.MaxSessionExpiry = 4 * time.Second
		o.SessionCheckpointInterval = 100 * time.Millisecond
	}
	addr := startBroker(t, natsURL, short)

	c := dialRaw(t, addr)
	c.connect(rawConnect("big-idle", 300))
	c.subscribe("bi/#", packet.QoS1)
	pub, _ := connectClient(t, addr, connectOpts("pub-big-idle"))
	pub.publish(&paho.Publish{Topic: "bi/large", QoS: 1, Payload: bytes.Repeat([]byte("z"), 64<<10)})
	require.Equal(t, "bi/large", c.expectPublish().Topic)

	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	ctx := context.Background()
	var kv jetstream.KeyValue
	// The bucket is created before the payload is put in it, so a bucket that
	// exists is not yet a payload that is stored.
	var keys []string
	require.Eventually(t, func() bool {
		kv, err = js.KeyValue(ctx, natsmqtt5.DefaultStreamPrefix+"_inflight")
		if err != nil {
			return false
		}
		keys, err = kv.Keys(ctx)
		return err == nil && len(keys) == 1
	}, 5*time.Second, 50*time.Millisecond, "the payload is stored while the client is connected")

	time.Sleep(11 * time.Second) // more than the bucket's 8 s
	keys, err = kv.Keys(ctx)
	require.NoError(t, err, "the payload is still there after the bucket's time to live")
	assert.Len(t, keys, 1)
}

// [MQTT-4.4.0-1]: a retained message too large for a session record but within
// what the NATS server accepts is still resent after the broker serving it
// stops. The payload bucket's limit is the server's max_payload less the room a
// key-value write needs, not the 7/8 a session record is held to.
func TestAnInflightRetainedPublishNearMaxPayloadIsRestored(t *testing.T) {
	var logs syncBuffer
	const maxPayload = 64 << 10
	natsURL := startNATSWithMaxPayload(t, maxPayload)
	noQueue := func(o *natsmqtt5.Options) { o.PersistentSessions, o.DisableOfflineQueue = true, true }
	addrA, stopA := startStoppableBroker(t, natsURL, noQueue, loggingBroker(t, &logs))
	addrB := startBroker(t, natsURL, noQueue)

	// Over seven eighths of max_payload (57344), so over what a session record
	// holds, and under max_payload less the publish's own headers.
	big := bytes.Repeat([]byte("z"), maxPayload-maxPayload/8+1024)
	pub, _ := connectClient(t, addrA, connectOpts("pub-near-max"))
	pub.publish(&paho.Publish{Topic: "nm/a", QoS: 1, Retain: true, Payload: big})
	time.Sleep(200 * time.Millisecond)

	c := dialRaw(t, addrA)
	c.connect(rawConnect("near-max", 300))
	c.subscribe("nm/#", packet.QoS1)
	first := c.expectPublish()
	require.Equal(t, big, first.Payload)
	c.drop()
	waitDetached(t, &logs, "near-max")
	stopA()

	back := dialRaw(t, addrB)
	require.True(t, back.connect(rawConnect("near-max", 300)).SessionPresent)
	got := back.expectPublish()
	assert.Equal(t, first.PacketID, got.PacketID)
	assert.True(t, got.Dup)
	assert.Equal(t, big, got.Payload)
}

// inflightKeys lists the keys of the payload bucket, none while it holds nothing.
func inflightKeys(t *testing.T, natsURL string) []string {
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

// A payload kept for an unacknowledged message is deleted once the client
// acknowledges it, and one session finishing does not delete the payload of
// another that was sent the same message under the same Packet Identifier
// [MQTT-4.4.0-1].
func TestAnAcknowledgedMessagesPayloadIsDeletedAndAnotherSessionsIsKept(t *testing.T) {
	natsURL := startNATS(t)
	fast := func(o *natsmqtt5.Options) {
		o.PersistentSessions, o.DisableOfflineQueue = true, true
		o.SessionCheckpointInterval = 100 * time.Millisecond
	}
	addrA, stopA := startStoppableBroker(t, natsURL, fast)
	addrB := startBroker(t, natsURL, fast)

	big := bytes.Repeat([]byte("q"), 100<<10)
	pub, _ := connectClient(t, addrA, connectOpts("pub-share"))
	pub.publish(&paho.Publish{Topic: "sh/a", QoS: 1, Retain: true, Payload: big})
	time.Sleep(200 * time.Millisecond)

	one, two := dialRaw(t, addrA), dialRaw(t, addrA)
	one.connect(rawConnect("share-one", 300))
	one.subscribe("sh/#", packet.QoS1)
	two.connect(rawConnect("share-two", 300))
	two.subscribe("sh/#", packet.QoS1)
	first, second := one.expectPublish(), two.expectPublish()
	require.Equal(t, first.PacketID, second.PacketID, "the same message under the same identifier")

	require.Eventually(t, func() bool { return len(inflightKeys(t, natsURL)) == 2 },
		5*time.Second, 50*time.Millisecond, "each session keeps its own payload")

	one.send(&packet.Puback{Ack: packet.Ack{PacketID: first.PacketID}})
	require.Eventually(t, func() bool { return len(inflightKeys(t, natsURL)) == 1 },
		5*time.Second, 50*time.Millisecond, "the acknowledged message's payload is deleted")

	two.drop()
	time.Sleep(200 * time.Millisecond)
	stopA()
	back := dialRaw(t, addrB)
	require.True(t, back.connect(rawConnect("share-two", 300)).SessionPresent)
	got := back.expectPublish()
	assert.Equal(t, second.PacketID, got.PacketID)
	assert.Equal(t, big, got.Payload, "the other session's payload survived the first one's acknowledgement")
	back.send(&packet.Puback{Ack: packet.Ack{PacketID: got.PacketID}})
	require.Eventually(t, func() bool { return len(inflightKeys(t, natsURL)) == 0 },
		5*time.Second, 50*time.Millisecond, "and is deleted when it is acknowledged on the broker that restored it")
}

// A session that is discarded takes its stored payloads with it: a client that
// reconnects with Clean Start has no use for the ones the old session held, and
// nothing else would remove them before the bucket's TTL.
func TestACleanStartDeletesThePayloadsOfTheSessionItReplaces(t *testing.T) {
	natsURL := startNATS(t)
	fast := func(o *natsmqtt5.Options) {
		o.PersistentSessions, o.DisableOfflineQueue = true, true
		o.SessionCheckpointInterval = 100 * time.Millisecond
	}
	addr := startBroker(t, natsURL, fast)

	pub, _ := connectClient(t, addr, connectOpts("pub-cs"))
	pub.publish(&paho.Publish{Topic: "cs/a", QoS: 1, Retain: true, Payload: bytes.Repeat([]byte("z"), 100<<10)})
	time.Sleep(200 * time.Millisecond)

	old := dialRaw(t, addr)
	old.connect(rawConnect("cs-client", 300))
	old.subscribe("cs/#", packet.QoS1)
	old.expectPublish()
	require.Eventually(t, func() bool { return len(inflightKeys(t, natsURL)) == 1 },
		5*time.Second, 50*time.Millisecond, "the session keeps its payload")
	old.drop()
	time.Sleep(300 * time.Millisecond)
	require.Len(t, inflightKeys(t, natsURL), 1, "a session that is only away keeps it")

	fresh := dialRaw(t, addr)
	cp := rawConnect("cs-client", 300)
	cp.CleanStart = true
	require.False(t, fresh.connect(cp).SessionPresent)

	require.Eventually(t, func() bool { return len(inflightKeys(t, natsURL)) == 0 },
		5*time.Second, 50*time.Millisecond, "the replaced session's payload is deleted")
}

// A session whose Session Expiry Interval is zero ends with its connection, so
// the payloads it held are deleted with its record.
func TestASessionThatEndsWithItsConnectionDeletesItsPayloads(t *testing.T) {
	natsURL := startNATS(t)
	fast := func(o *natsmqtt5.Options) {
		o.PersistentSessions, o.DisableOfflineQueue = true, true
		o.SessionCheckpointInterval = 100 * time.Millisecond
	}
	addr := startBroker(t, natsURL, fast)

	pub, _ := connectClient(t, addr, connectOpts("pub-ex0"))
	pub.publish(&paho.Publish{Topic: "ex0/a", QoS: 1, Retain: true, Payload: bytes.Repeat([]byte("y"), 100<<10)})
	time.Sleep(200 * time.Millisecond)

	c := dialRaw(t, addr)
	c.connect(rawConnect("ex0-client", 0))
	c.subscribe("ex0/#", packet.QoS1)
	c.expectPublish()
	c.drop()

	time.Sleep(time.Second)
	require.Eventually(t, func() bool { return len(inflightKeys(t, natsURL)) == 0 },
		5*time.Second, 50*time.Millisecond, "ending the session leaves no payload")
}

// [MQTT-4.4.0-1]: with a small max_payload, a PUBLISH that would fit the inline limit but not a
// record value goes to the payload bucket and is resent, rather than being cut from the record.
func TestAnInflightPublishThatFitsTheInlineLimitButNotARecordIsStillResent(t *testing.T) {
	var logs syncBuffer
	natsURL := startNATSWithMaxPayload(t, 4096)
	noQueue := func(o *natsmqtt5.Options) { o.PersistentSessions, o.DisableOfflineQueue = true, true }
	addrA, stopA := startStoppableBroker(t, natsURL, noQueue, loggingBroker(t, &logs))
	addrB := startBroker(t, natsURL, noQueue)

	c := dialRaw(t, addrA)
	c.connect(rawConnect("small-max", 300))
	c.subscribe("sm/#", packet.QoS1)
	pub, _ := connectClient(t, addrA, connectOpts("pub-small-max"))
	body := bytes.Repeat([]byte("z"), 3<<10)
	pub.publish(&paho.Publish{Topic: "sm/a", QoS: 1, Payload: body})
	first := c.expectPublish()
	require.Equal(t, body, first.Payload)
	c.drop()
	waitDetached(t, &logs, "small-max")
	stopA()

	back := dialRaw(t, addrB)
	require.True(t, back.connect(rawConnect("small-max", 300)).SessionPresent)
	got := back.expectPublish()
	assert.Equal(t, first.PacketID, got.PacketID, "[MQTT-4.4.0-1] the original Packet Identifier")
	assert.True(t, got.Dup)
	assert.Equal(t, body, got.Payload)
}

// Many in-flight messages that each fit the inline limit must not, together, push the record
// past the value: the ones beyond the inline budget go to the payload bucket, so none is lost.
func TestManySmallInflightPublishesUnderASmallMaxPayloadAreAllResent(t *testing.T) {
	var logs syncBuffer
	natsURL := startNATSWithMaxPayload(t, 4096)
	noQueue := func(o *natsmqtt5.Options) { o.PersistentSessions, o.DisableOfflineQueue = true, true }
	addrA, stopA := startStoppableBroker(t, natsURL, noQueue, loggingBroker(t, &logs))
	addrB := startBroker(t, natsURL, noQueue)

	const n = 12
	c := dialRaw(t, addrA)
	c.connect(rawConnect("many-small", 300))
	c.subscribe("ms/#", packet.QoS1)
	pub, _ := connectClient(t, addrA, connectOpts("pub-many-small"))
	ids := map[uint16]bool{}
	for i := 0; i < n; i++ {
		body := bytes.Repeat([]byte{byte('a' + i)}, 600)
		pub.publish(&paho.Publish{Topic: "ms/x", QoS: 1, Payload: body})
	}
	for i := 0; i < n; i++ {
		ids[c.expectPublish().PacketID] = true
	}
	c.drop()
	waitDetached(t, &logs, "many-small")
	stopA()

	back := dialRaw(t, addrB)
	require.True(t, back.connect(rawConnect("many-small", 300)).SessionPresent)
	for i := 0; i < n; i++ {
		got := back.expectPublish()
		assert.True(t, ids[got.PacketID], "[MQTT-4.4.0-1] resend %d carries an identifier the client was sent", i)
		assert.True(t, got.Dup)
		delete(ids, got.PacketID)
	}
	assert.Empty(t, ids, "every unacknowledged message is resent")
}

// [MQTT-4.4.0-1]: more unacknowledged messages than a record value holds under a small
// max_payload are all resent, in the order they were sent, by the broker that restores the
// session, because the entries that do not fit the record are kept in the payload bucket
// beside it. Once they are acknowledged nothing is left in the bucket.
func TestUnacknowledgedMessagesBeyondOneRecordValueAreAllResentInOrder(t *testing.T) {
	var logs syncBuffer
	natsURL := startNATSWithMaxPayload(t, 4096)
	noQueue := func(o *natsmqtt5.Options) { o.PersistentSessions, o.DisableOfflineQueue = true, true }
	addrA, stopA := startStoppableBroker(t, natsURL, noQueue, loggingBroker(t, &logs))
	addrB := startBroker(t, natsURL, noQueue)

	const n = 60
	c := dialRaw(t, addrA)
	c.connect(rawConnect("many-spill", 300))
	c.subscribe("sp/#", packet.QoS1)
	pub, _ := connectClient(t, addrA, connectOpts("pub-many-spill"))
	for i := 0; i < n; i++ {
		pub.publish(&paho.Publish{Topic: "sp/x", QoS: 1, Payload: bytes.Repeat([]byte{byte('a' + i%26)}, 600+i)})
	}
	sent := make([]uint16, 0, n)
	for i := 0; i < n; i++ {
		sent = append(sent, c.expectPublish().PacketID)
	}
	c.drop()
	waitDetached(t, &logs, "many-spill")
	stopA()

	back := dialRaw(t, addrB)
	require.True(t, back.connect(rawConnect("many-spill", 300)).SessionPresent)
	for i := 0; i < n; i++ {
		got := back.expectPublish()
		assert.Equal(t, sent[i], got.PacketID, "[MQTT-4.4.0-1] resend %d, in the order sent", i)
		assert.Equal(t, bytes.Repeat([]byte{byte('a' + i%26)}, 600+i), got.Payload)
		back.send(&packet.Puback{Ack: packet.Ack{PacketID: got.PacketID}})
	}
	back.expectNothing()
	require.Eventually(t, func() bool { return len(inflightKeys(t, natsURL)) == 0 },
		10*time.Second, 100*time.Millisecond, "acknowledged messages leave nothing in the bucket")
}
