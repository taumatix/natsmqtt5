package natsmqtt5_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// In-flight messages that have no copy in the offline queue to be read back:
// a retained message sent on subscribing, one from a shared subscription's
// backlog, and any with the queue off. The session record carries what is needed
// to resend them [MQTT-4.4.0-1]: the stream sequence for a shared message, the
// PUBLISH itself (when it is small) for the rest.
//
//	[MQTT-4.4.0-1] "When a Client reconnects with Clean Start set to 0 and a
//	  session is present, both the Client and Server MUST resend any
//	  unacknowledged PUBLISH packets (where QoS > 0) and PUBREL packets using
//	  their original Packet Identifiers."
//	[MQTT-3.3.1-1] "The DUP flag MUST be set to 1 by the Client or Server when it
//	  attempts to re-deliver a PUBLISH packet."
//	[MQTT-3.3.1-3] (the DUP flag is set by whether this send is a retransmission)

// [MQTT-4.4.0-1], [MQTT-3.3.1-1]: a retained message the client was sent on
// subscribing and had not acknowledged is resent by the broker that restores the
// session, with its original Packet Identifier, DUP 1, and the RETAIN flag it
// went out with, though the retained store may have moved on since.
func TestARetainedMessageInFlightIsResentByAnotherBroker(t *testing.T) {
	natsURL := startNATS(t)
	addrA, stopA := startStoppableBroker(t, natsURL, persistent)
	addrB := startBroker(t, natsURL, persistent)

	pub, _ := connectClient(t, addrA, connectOpts("pub-restore-ret"))
	pub.publish(&paho.Publish{Topic: "rs/ret/a", QoS: 1, Retain: true, Payload: []byte("as it was")})

	c := dialRaw(t, addrA)
	c.connect(rawConnect("restore-ret", 300))
	c.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{
		{Filter: "rs/ret/#", QoS: packet.QoS1, RetainAsPublished: true}}})
	require.IsType(t, &packet.Suback{}, c.read())
	first := c.expectPublish()
	require.Equal(t, "as it was", string(first.Payload))
	require.True(t, first.Retain)
	c.drop()
	time.Sleep(100 * time.Millisecond)
	stopA()

	// The retained message changes while nobody holds the session.
	pub2, _ := connectClient(t, addrB, connectOpts("pub-restore-ret2"))
	pub2.publish(&paho.Publish{Topic: "rs/ret/a", QoS: 1, Retain: true, Payload: []byte("changed")})

	back := dialRaw(t, addrB)
	require.True(t, back.connect(rawConnect("restore-ret", 300)).SessionPresent)
	got := back.expectPublish()
	assert.Equal(t, first.PacketID, got.PacketID, "the original Packet Identifier [MQTT-4.4.0-1]")
	assert.Equal(t, "as it was", string(got.Payload), "the message that was sent, not the retained one now")
	assert.True(t, got.Dup, "[MQTT-3.3.1-1]")
	assert.True(t, got.Retain)
	back.send(&packet.Puback{Ack: packet.Ack{PacketID: got.PacketID}})
}

// [MQTT-4.4.0-1]: the same for a broker killed with SIGKILL, whose last
// checkpoint is what the successor has.
func TestAKilledBrokersRetainedMessageInFlightIsResent(t *testing.T) {
	natsURL := startNATS(t)
	child := startBrokerBinary(t, natsURL, "-persistent-sessions", "-session-checkpoint-interval", killedBrokerCheckpoint)
	survivor := startBroker(t, natsURL, persistentWithQueue)

	pub, _ := connectClient(t, survivor, connectOpts("pub-killed-ret"))
	pub.publish(&paho.Publish{Topic: "kr/a", QoS: 1, Retain: true, Payload: []byte("retained")})
	// Older than the checkpoint's skew, so the successor's replay of the queue does
	// not also hold this message as one published while the broker was going down.
	time.Sleep(time.Second)

	c := dialRaw(t, child.addr)
	c.connect(rawConnect("killed-ret", 300))
	c.subscribe("kr/#", packet.QoS1)
	first := c.expectPublish()
	require.Equal(t, "retained", string(first.Payload))
	time.Sleep(700 * time.Millisecond)
	child.kill9(t)

	back := dialRaw(t, survivor)
	require.True(t, back.connect(rawConnect("killed-ret", 300)).SessionPresent)
	got := back.expectPublish()
	assert.Equal(t, first.PacketID, got.PacketID)
	assert.Equal(t, "retained", string(got.Payload))
	assert.True(t, got.Dup)
	back.send(&packet.Puback{Ack: packet.Ack{PacketID: got.PacketID}})
	back.expectNothing()
}

// [MQTT-4.4.0-1]: with the offline queue off a message in flight is resent from
// the record, QoS 1 and QoS 2 (a PUBLISH not yet taken by the client) alike.
func TestWithoutTheOfflineQueueARestoredSessionResendsFromTheRecord(t *testing.T) {
	natsURL := startNATS(t)
	noQueue := func(o *natsmqtt5.Options) { o.PersistentSessions, o.DisableOfflineQueue = true, true }
	addrA, stopA := startStoppableBroker(t, natsURL, noQueue)
	addrB := startBroker(t, natsURL, noQueue)

	c := dialRaw(t, addrA)
	c.connect(rawConnect("restore-noqueue", 300))
	c.subscribe("rs/nq/#", packet.QoS2)
	pub, _ := connectClient(t, addrA, connectOpts("pub-restore-noqueue"))
	pub.publish(&paho.Publish{Topic: "rs/nq/one", QoS: 1, Payload: []byte("one")})
	pub.publish(&paho.Publish{Topic: "rs/nq/two", QoS: 2, Payload: []byte("two")})
	sent := []*packet.Publish{c.expectPublish(), c.expectPublish()}
	c.drop()
	time.Sleep(100 * time.Millisecond)
	stopA()

	back := dialRaw(t, addrB)
	require.True(t, back.connect(rawConnect("restore-noqueue", 300)).SessionPresent)
	for _, want := range sent {
		got := back.expectPublish()
		assert.Equal(t, want.PacketID, got.PacketID)
		assert.Equal(t, string(want.Payload), string(got.Payload))
		assert.Equal(t, want.QoS, got.QoS)
		assert.True(t, got.Dup)
	}
}

// [MQTT-4.4.0-1]: a payload too large to keep in a session record is kept beside
// it, so with the offline queue off the broker that restores the session still
// resends the message, with the small one after it in the order they were sent
// [MQTT-4.6.0-5].
func TestWithoutTheOfflineQueueAPayloadTooLargeForTheRecordIsStillResent(t *testing.T) {
	natsURL := startNATS(t)
	noQueue := func(o *natsmqtt5.Options) { o.PersistentSessions, o.DisableOfflineQueue = true, true }
	addrA, stopA := startStoppableBroker(t, natsURL, noQueue)
	addrB := startBroker(t, natsURL, noQueue)

	c := dialRaw(t, addrA)
	c.connect(rawConnect("restore-big", 300))
	c.subscribe("rs/big/#", packet.QoS1)
	pub, _ := connectClient(t, addrA, connectOpts("pub-restore-big"))
	large := bytes.Repeat([]byte("x"), 32<<10)
	pub.publish(&paho.Publish{Topic: "rs/big/large", QoS: 1, Payload: large})
	pub.publish(&paho.Publish{Topic: "rs/big/small", QoS: 1, Payload: []byte("small")})
	first := c.expectPublish()
	require.Equal(t, "rs/big/large", first.Topic)
	small := c.expectPublish()
	c.drop()
	time.Sleep(100 * time.Millisecond)
	stopA()

	back := dialRaw(t, addrB)
	require.True(t, back.connect(rawConnect("restore-big", 300)).SessionPresent)
	got := back.expectPublish()
	assert.Equal(t, first.PacketID, got.PacketID)
	assert.Equal(t, large, got.Payload)
	assert.True(t, got.Dup)
	got = back.expectPublish()
	assert.Equal(t, small.PacketID, got.PacketID)
	assert.Equal(t, "small", string(got.Payload))
	back.expectNothing()
}

// [MQTT-4.4.0-1]: a shared subscription's QoS 1 message its member was sent and
// had not acknowledged is resent by the broker that restores the member's
// session, from the queue copy the record names.
func TestASharedSubscriptionsMessageInFlightIsResentByAnotherBroker(t *testing.T) {
	natsURL := startNATS(t)
	addrA, stopA := startStoppableBroker(t, natsURL, persistent)
	addrB := startBroker(t, natsURL, persistent)

	c := dialRaw(t, addrA)
	c.connect(rawConnect("restore-shared", 300))
	c.subscribe("$share/g/rs/sh/#", packet.QoS1)
	pub, _ := connectClient(t, addrA, connectOpts("pub-restore-shared"))
	pub.publish(&paho.Publish{Topic: "rs/sh/a", QoS: 1, Payload: []byte("job")})
	first := c.expectPublish()
	require.Equal(t, "job", string(first.Payload))
	c.drop()
	time.Sleep(100 * time.Millisecond)
	stopA()

	back := dialRaw(t, addrB)
	require.True(t, back.connect(rawConnect("restore-shared", 300)).SessionPresent)
	got := back.expectPublish()
	assert.Equal(t, first.PacketID, got.PacketID, "[MQTT-4.4.0-1]")
	assert.Equal(t, "job", string(got.Payload))
	assert.True(t, got.Dup)
	back.send(&packet.Puback{Ack: packet.Ack{PacketID: got.PacketID}})
	back.expectNothing()
}
