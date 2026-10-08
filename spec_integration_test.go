package natsmqtt5_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
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

// Integration tests for the session, offline-queue and replay statements
// (CONTRIBUTING.md, "Every statement has an integration test"). Each runs a real
// NATS server, one or two real brokers and a real TCP socket, and cites the
// [MQTT-x.y.z-n] statement it proves. Statement wording was read from the
// OASIS MQTT 5.0 specification on 2026-10-08.
//
// Where a test writes into the session record, it does so because the broker
// cannot be driven into that state through its interface: the record is the
// interface between two brokers, and what a test writes is what a broker writes.
// Each such test says what it stands in for.

// connectPacketBytes is a CONNECT written byte by byte from MQTT-5.0 §3.1, not
// through this module's encoder, so the CONNACK it provokes is judged against
// the specification and not against our own codec.
func connectPacketBytes(clientID string, cleanStart bool, expiry uint32) []byte {
	flags := byte(0x00)
	if cleanStart {
		flags = 0x02 // Clean Start, bit 1 (§3.1.2.4)
	}
	body := []byte{
		0x00, 0x04, 'M', 'Q', 'T', 'T', // protocol name
		0x05,       // protocol version 5
		flags,      // connect flags
		0x00, 0x1E, // keep alive 30 s
		0x05, 0x11, // properties: length 5, Session Expiry Interval (0x11)
		byte(expiry >> 24), byte(expiry >> 16), byte(expiry >> 8), byte(expiry),
		0x00, byte(len(clientID)), // client identifier
	}
	body = append(body, clientID...)
	return append([]byte{0x10, byte(len(body))}, body...)
}

// rawDisconnect is a DISCONNECT with Reason Code 0x00 and no properties
// (§3.14): the remaining length may be 0.
var rawDisconnect = []byte{0xE0, 0x00}

// handshake sends raw CONNECT bytes and returns the CONNACK's three fixed
// bytes: packet type, Connect Acknowledge Flags, Connect Reason Code
// (§3.2.2.1, §3.2.2.2). The properties after them are read and discarded.
func (c *rawClient) handshake(connect []byte) (typ, ackFlags, reason byte) {
	c.t.Helper()
	_, err := c.nc.Write(connect)
	require.NoError(c.t, err)
	require.NoError(c.t, c.nc.SetReadDeadline(time.Now().Add(5*time.Second)))
	head := make([]byte, 2)
	_, err = io.ReadFull(c.r, head)
	require.NoError(c.t, err)
	rest := make([]byte, head[1])
	_, err = io.ReadFull(c.r, rest)
	require.NoError(c.t, err)
	require.GreaterOrEqual(c.t, len(rest), 2, "a CONNACK holds at least the flags and the reason code")
	return head[0], rest[0], rest[1]
}

// sessionPresentOnConnect returns the Session Present bit of the CONNACK to a
// raw CONNECT, requiring a Success CONNACK.
func sessionPresentOnConnect(t *testing.T, addr, clientID string, cleanStart bool, expiry uint32) (c *rawClient, present bool) {
	t.Helper()
	c = dialRaw(t, addr)
	typ, flags, reason := c.handshake(connectPacketBytes(clientID, cleanStart, expiry))
	require.Equal(t, byte(0x20), typ, "a CONNECT is answered with a CONNACK")
	require.Equal(t, byte(0x00), reason, "Success reason code [MQTT-3.2.2-3]")
	require.Zero(t, flags&0xFE, "the reserved bits of the acknowledge flags are 0 [MQTT-3.2.2-1]")
	return c, flags&0x01 == 1
}

// ---- A broker without JetStream ------------------------------------------

func withoutJetStream(o *natsmqtt5.Options) {
	o.DisableRetained = true
	o.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
}

// A broker against a NATS server with no JetStream keeps sessions in its own
// memory, and Session Present has to say so truthfully: 0 for Clean Start 1
// [MQTT-3.2.2-2], 0 when it holds no state for the Client Identifier, 1 when it
// does [MQTT-3.2.2-3], and 0 again once the process that held the state is gone
// [MQTT-3.2.2-3, "otherwise"].
func TestWithoutJetStreamSessionPresentIsTruthful(t *testing.T) {
	natsURL := startNATSWithoutJetStream(t)
	addrA, stopA := startStoppableBroker(t, natsURL, withoutJetStream)

	_, present := sessionPresentOnConnect(t, addrA, "nojs-1", true, 300)
	assert.False(t, present, "Clean Start 1 gives Session Present 0 [MQTT-3.2.2-2]")

	first, present := sessionPresentOnConnect(t, addrA, "nojs-2", false, 300)
	assert.False(t, present, "no state for the Client Identifier: Session Present 0 [MQTT-3.2.2-3]")
	first.subscribe("n/#", packet.QoS1)
	first.send(&packet.Disconnect{})
	time.Sleep(100 * time.Millisecond)

	back, present := sessionPresentOnConnect(t, addrA, "nojs-2", false, 300)
	assert.True(t, present, "the broker holds the session in memory: Session Present 1 [MQTT-3.2.2-3, MQTT-3.1.2-5]")
	// The subscription came back with the session, not by being made again.
	pub, _ := connectClient(t, addrA, connectOpts("pub-nojs"))
	pub.publish(&paho.Publish{Topic: "n/x", QoS: 1, Payload: []byte("live")})
	assert.Equal(t, "n/x", back.expectPublish().Topic)

	back.send(&packet.Disconnect{})
	time.Sleep(100 * time.Millisecond)
	stopA()

	addrB := startBroker(t, natsURL, withoutJetStream)
	_, present = sessionPresentOnConnect(t, addrB, "nojs-2", false, 300)
	assert.False(t, present,
		"a broker that was restarted holds nothing without JetStream: Session Present 0 [MQTT-3.2.2-3]")
}

// A session whose Session Expiry Interval is 0 ends with its connection
// [MQTT-3.1.2-23, contrapositive], and the broker says so.
func TestWithoutJetStreamAZeroExpirySessionEndsWithItsConnection(t *testing.T) {
	addr := startBroker(t, startNATSWithoutJetStream(t), withoutJetStream)
	c, present := sessionPresentOnConnect(t, addr, "nojs-zero", false, 0)
	require.False(t, present)
	c.subscribe("z/#", packet.QoS1)
	c.send(&packet.Disconnect{})
	time.Sleep(100 * time.Millisecond)

	_, present = sessionPresentOnConnect(t, addr, "nojs-zero", false, 0)
	assert.False(t, present, "Session Present 0 for a session that ended with its connection [MQTT-3.2.2-3]")
}

// The options that need JetStream refuse to start without it rather than
// start and claim sessions they cannot keep.
func TestWithoutJetStreamPersistentSessionsAreRefused(t *testing.T) {
	_, err := natsmqtt5.New(natsmqtt5.Options{
		NATSURL:            startNATSWithoutJetStream(t),
		Listen:             "127.0.0.1:0",
		DisableRetained:    true,
		PersistentSessions: true,
	})
	require.Error(t, err, "PersistentSessions against a server without JetStream must not start")
}

// ---- Expiry with the offline queue ---------------------------------------

// A session past its Session Expiry Interval is discarded [MQTT-4.1.0-2]; what
// was published while it was away must not be replayed into the session that
// takes its place, and the CONNACK says there is no session [MQTT-3.2.2-3].
func expiredSessionIsDiscarded(t *testing.T, customise ...func(*natsmqtt5.Options)) {
	t.Helper()
	addr := startBroker(t, startNATS(t), customise...)

	away, present := sessionPresentOnConnect(t, addr, "expired-1", false, 1)
	require.False(t, present)
	away.subscribe("e/#", packet.QoS1)
	away.send(&packet.Disconnect{})
	time.Sleep(100 * time.Millisecond)

	pub, _ := connectClient(t, addr, connectOpts("pub-expired"))
	pub.publish(&paho.Publish{Topic: "e/1", QoS: 1, Payload: []byte("1")})
	time.Sleep(1500 * time.Millisecond)

	back, present := sessionPresentOnConnect(t, addr, "expired-1", false, 300)
	assert.False(t, present, "the expired session is gone [MQTT-4.1.0-2, MQTT-3.2.2-3]")
	back.expectNothing()
	back.subscribe("e/#", packet.QoS1)
	back.expectNothing()
}

func TestAnExpiredSessionIsDiscardedAndItsQueuedMessagesWithIt(t *testing.T) {
	expiredSessionIsDiscarded(t)
}

func TestAnExpiredPersistentSessionIsDiscardedAndItsQueuedMessagesWithIt(t *testing.T) {
	expiredSessionIsDiscarded(t, persistentWithQueue)
}

// A shared-subscription member whose session expired is a new session when it
// comes back [MQTT-4.1.0-2]: Session Present 0, and no work handed to it until
// it subscribes again.
func TestAnExpiredSharedMemberComesBackWithoutASession(t *testing.T) {
	addr := startBroker(t, startNATS(t))
	member, present := sessionPresentOnConnect(t, addr, "share-expired", false, 1)
	require.False(t, present)
	member.subscribe("$share/g/jobs/#", packet.QoS1)
	member.send(&packet.Disconnect{})
	time.Sleep(100 * time.Millisecond)

	pub, _ := connectClient(t, addr, connectOpts("pub-share-expired"))
	pub.publish(&paho.Publish{Topic: "jobs/1", QoS: 1, Payload: []byte("1")})
	time.Sleep(1500 * time.Millisecond)

	back, present := sessionPresentOnConnect(t, addr, "share-expired", false, 300)
	assert.False(t, present, "the expired member's session is gone [MQTT-4.1.0-2, MQTT-3.2.2-3]")
	back.expectNothing()
}

// ---- QoS 2 through the offline queue --------------------------------------

// receiveQoS2UntilPubrel takes one QoS 2 PUBLISH, answers it with a PUBREC and
// reads the PUBREL, leaving the exchange open at the point where the sender must
// not send the PUBLISH again [MQTT-4.3.3-6].
func (c *rawClient) receiveQoS2UntilPubrel(topic, payload string) uint16 {
	c.t.Helper()
	p := c.expectPublish()
	require.Equal(c.t, topic, p.Topic)
	require.Equal(c.t, payload, string(p.Payload))
	require.Equal(c.t, packet.QoS2, p.QoS)
	require.False(c.t, p.Dup, "the first send of a message has DUP 0 [MQTT-4.3.3-2]")
	c.send(&packet.Pubrec{Ack: packet.Ack{PacketID: p.PacketID}})
	rel := c.expectPubrel()
	require.Equal(c.t, p.PacketID, rel.PacketID, "PUBREL carries the PUBLISH's Packet Identifier [MQTT-4.3.3-4]")
	return p.PacketID
}

// A QoS 2 message published while its subscriber was away arrives as QoS 2 with
// DUP 0, and the exchange completes in the order the specification gives.
func TestAQoS2MessageFromTheOfflineQueueCompletesTheHandshake(t *testing.T) {
	addr := startBroker(t, startNATS(t))
	away, _ := sessionPresentOnConnect(t, addr, "q2-offline", false, 300)
	away.subscribe("g/#", packet.QoS2)
	away.send(&packet.Disconnect{})
	time.Sleep(100 * time.Millisecond)

	pub, _ := connectClient(t, addr, connectOpts("pub-q2-offline"))
	pub.publish(&paho.Publish{Topic: "g/1", QoS: 2, Payload: []byte("once")})

	back, present := sessionPresentOnConnect(t, addr, "q2-offline", false, 300)
	require.True(t, present)
	id := back.receiveQoS2UntilPubrel("g/1", "once")
	back.send(&packet.Pubcomp{Ack: packet.Ack{PacketID: id}})
	back.expectNothing()
}

// A QoS 2 message whose PUBLISH has been acknowledged with PUBREC is resumed
// with a PUBREL and never with the PUBLISH again [MQTT-4.3.3-6, MQTT-4.4.0-1];
// and the offline queue's replay, which starts before the connection dropped,
// must not deliver it a second time as new.
func TestAReplayDoesNotRepeatAQoS2MessageThatWasPastPubrec(t *testing.T) {
	addr := startBroker(t, startNATS(t))
	c, _ := sessionPresentOnConnect(t, addr, "q2-resume", false, 300)
	c.subscribe("g/#", packet.QoS2)

	pub, _ := connectClient(t, addr, connectOpts("pub-q2-resume"))
	pub.publish(&paho.Publish{Topic: "g/1", QoS: 2, Payload: []byte("once")})
	id := c.receiveQoS2UntilPubrel("g/1", "once")
	c.drop()
	time.Sleep(100 * time.Millisecond)

	back, present := sessionPresentOnConnect(t, addr, "q2-resume", false, 300)
	require.True(t, present)
	rel := back.expectPubrel()
	assert.Equal(t, id, rel.PacketID, "the PUBREL is resent with its original Packet Identifier [MQTT-4.4.0-1]")
	back.send(&packet.Pubcomp{Ack: packet.Ack{PacketID: id}})
	back.expectNothing()
}

// ---- The session record ---------------------------------------------------

// rewriteRecord reads the session record of clientID from the key-value bucket,
// lets change edit it as a generic JSON object, and writes it back. The record
// is what one broker leaves for the next, so a test that edits it is playing the
// part of the broker that wrote it.
func rewriteRecord(t *testing.T, natsURL, clientID string, change func(rec map[string]any)) {
	t.Helper()
	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	ctx := context.Background()
	kv, err := js.KeyValue(ctx, natsmqtt5.DefaultStreamPrefix+"_sessions")
	require.NoError(t, err)
	key := base64.RawURLEncoding.EncodeToString([]byte(clientID))
	entry, err := kv.Get(ctx, key)
	require.NoError(t, err)
	var rec map[string]any
	require.NoError(t, json.Unmarshal(entry.Value(), &rec))
	change(rec)
	b, err := json.Marshal(rec)
	require.NoError(t, err)
	_, err = kv.Update(ctx, key, b, entry.Revision())
	require.NoError(t, err)
}

// queuedCopy reads message seq of the offline queue's stream as the broker
// stored it: its Mqtt5-Msg-Id header is the id a session record keeps.
func queuedCopy(t *testing.T, natsURL string, seq uint64) (id, payload string) {
	t.Helper()
	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	stream, err := js.Stream(context.Background(), natsmqtt5.DefaultStreamPrefix+"_queue")
	require.NoError(t, err)
	raw, err := stream.GetMsg(context.Background(), seq)
	require.NoError(t, err)
	id = raw.Header.Get("Mqtt5-Msg-Id")
	require.NotEmpty(t, id, "a queued copy carries the id the record refers to it by")
	return id, string(raw.Data)
}

// The record a broker that predates AwayFromSeq and Delivered wrote has neither
// field; one written today for a client that was caught up has neither either,
// because they are omitted when empty. A session resumed from it replays from
// the time it was released [MQTT-3.1.2-23, MQTT-4.5.0-1]. The record here is
// cut down to exactly the fields v0.10.0 wrote.
func TestASessionRecordWithoutTheReplayPositionIsResumedFromItsReleaseTime(t *testing.T) {
	natsURL := startNATS(t)
	addrA, stopA := startStoppableBroker(t, natsURL, persistentWithQueue)

	away, _ := sessionPresentOnConnect(t, addrA, "old-record", false, 300)
	away.subscribe("o/#", packet.QoS2)
	away.send(&packet.Disconnect{})
	time.Sleep(100 * time.Millisecond)

	pub, _ := connectClient(t, addrA, connectOpts("pub-old-record"))
	pub.publish(&paho.Publish{Topic: "o/1", QoS: 1, Payload: []byte("1")})
	pub.publish(&paho.Publish{Topic: "o/2", QoS: 2, Payload: []byte("2")})
	stopA()

	oldFields := map[string]bool{
		"Version": true, "ClientID": true, "Owner": true, "Attached": true, "Identity": true,
		"Username": true, "Subscriptions": true, "ExpirySeconds": true, "ExpiresAt": true, "AwayAt": true,
	}
	rewriteRecord(t, natsURL, "old-record", func(rec map[string]any) {
		require.Contains(t, rec, "AwayAt", "the record being cut down must be a released session's")
		for k := range rec {
			if !oldFields[k] {
				delete(rec, k)
			}
		}
	})

	addrB := startBroker(t, natsURL, persistentWithQueue)
	back, present := sessionPresentOnConnect(t, addrB, "old-record", false, 300)
	require.True(t, present, "an older record is a session [MQTT-3.2.2-3]")

	one := back.expectPublish()
	assert.Equal(t, "o/1", one.Topic)
	assert.False(t, one.Dup)
	back.send(&packet.Puback{Ack: packet.Ack{PacketID: one.PacketID}})
	back.send(&packet.Pubcomp{Ack: packet.Ack{PacketID: back.receiveQoS2UntilPubrel("o/2", "2")}})
	back.expectNothing()
}

// A QoS 2 message the last connection delivered above the replay's starting
// sequence must not be delivered again as a new message when the session is
// restored on another broker [MQTT-4.3.3-2: a first send is the only send with
// DUP 0; MQTT-4.3.3-6: no PUBLISH after the PUBREL]. The record lists the ids
// delivered above the start for that reason.
//
// The broker cannot be driven into "delivered above the start" through its
// interface in a repeatable way, since that needs two publishers' copies to
// reach the queue and the connection in opposite orders. So the record is
// written the way the releasing broker writes it, from the real queue sequences
// and ids of two real messages: owed is below the start, delivered is above it.
func restoredRewind(t *testing.T, listDelivered bool) (second *packet.Publish) {
	t.Helper()
	natsURL := startNATS(t)
	addrA, stopA := startStoppableBroker(t, natsURL, persistentWithQueue)

	c, _ := sessionPresentOnConnect(t, addrA, "q2-rewind", false, 300)
	c.subscribe("g/#", packet.QoS2)
	pub, _ := connectClient(t, addrA, connectOpts("pub-q2-rewind"))
	pub.publish(&paho.Publish{Topic: "g/0", QoS: 1, Payload: []byte("owed")})
	pub.publish(&paho.Publish{Topic: "g/1", QoS: 2, Payload: []byte("delivered")})

	zero := c.expectPublish()
	require.Equal(t, "g/0", zero.Topic)
	c.send(&packet.Puback{Ack: packet.Ack{PacketID: zero.PacketID}})
	c.receiveQoS2UntilPubrel("g/1", "delivered")
	c.drop()
	time.Sleep(100 * time.Millisecond)
	stopA()

	_, owedPayload := queuedCopy(t, natsURL, 1)
	require.Equal(t, "owed", owedPayload)
	deliveredID, deliveredPayload := queuedCopy(t, natsURL, 2)
	require.Equal(t, "delivered", deliveredPayload)
	rewriteRecord(t, natsURL, "q2-rewind", func(rec map[string]any) {
		rec["AwayFromSeq"] = 1
		if listDelivered {
			rec["Delivered"] = []map[string]any{{"ID": deliveredID, "Seq": 2}}
		}
	})

	addrB := startBroker(t, natsURL, persistentWithQueue)
	back, present := sessionPresentOnConnect(t, addrB, "q2-rewind", false, 300)
	require.True(t, present)
	first := back.expectPublish()
	require.Equal(t, "g/0", first.Topic, "the rewind reaches what the record says was owed")
	back.send(&packet.Puback{Ack: packet.Ack{PacketID: first.PacketID}})
	if !listDelivered {
		second = back.expectPublish()
		return second
	}
	back.expectNothing()
	return nil
}

func TestARestoredRewindDoesNotRedeliverAQoS2MessageItAlreadyDelivered(t *testing.T) {
	assert.Nil(t, restoredRewind(t, true),
		"g/1 was delivered and PUBREC'd before the detach; it must not come again as new")
}

// The control for the test above: the same rewind with the id left out of the
// record does reach the message, so it is the id and not the rewind's position
// that keeps it from being sent twice.
func TestARestoredRewindWithoutTheDeliveredIDWouldRedeliverIt(t *testing.T) {
	again := restoredRewind(t, false)
	require.NotNil(t, again)
	assert.Equal(t, "g/1", again.Topic)
	assert.Equal(t, packet.QoS2, again.QoS)
}
