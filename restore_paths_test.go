package natsmqtt5_test

import (
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// Two restore paths of a session record, driven over TCP against a real
// embedded NATS server with JetStream: the queue copy of an unacknowledged
// message that has aged out between the release and the resume, and a filter
// that was unsubscribed while its message was still unacknowledged.
//
//	[MQTT-4.4.0-1] "When a Client reconnects with Clean Start set to 0 and a
//	  session is present, both the Client and Server MUST resend any
//	  unacknowledged PUBLISH packets (where QoS > 0) and PUBREL packets using
//	  their original Packet Identifiers."

// [MQTT-4.4.0-1]: a message whose queue copy left the stream (OfflineQueueMaxAge)
// while the session was away cannot be resent, because the broker no longer has
// it; it is dropped, the rest of the session resumes, and a message published
// after the resume is delivered.
func TestAnInflightMessageWhoseQueueCopyAgedOutIsDroppedOnResume(t *testing.T) {
	natsURL := startNATS(t)
	shortQueue := func(o *natsmqtt5.Options) { o.OfflineQueueMaxAge = time.Second }
	addrA, stopA := startStoppableBroker(t, natsURL, persistentWithQueue, shortQueue)
	addrB := startBroker(t, natsURL, persistentWithQueue, shortQueue)

	c := dialRaw(t, addrA)
	c.connect(rawConnect("restore-aged", 300))
	c.subscribe("rs/aged/#", packet.QoS1)
	pub, _ := connectClient(t, addrA, connectOpts("pub-restore-aged"))
	pub.publish(&paho.Publish{Topic: "rs/aged/old", QoS: 1, Payload: []byte("old")})
	require.Equal(t, "old", string(c.expectPublish().Payload))
	c.drop()
	time.Sleep(100 * time.Millisecond)
	stopA()
	time.Sleep(3 * time.Second) // past the queue's MaxAge

	back := dialRaw(t, addrB)
	require.True(t, back.connect(rawConnect("restore-aged", 300)).SessionPresent)
	back.expectNothing()

	pub2, _ := connectClient(t, addrB, connectOpts("pub-restore-aged2"))
	pub2.publish(&paho.Publish{Topic: "rs/aged/new", QoS: 1, Payload: []byte("new")})
	got := back.expectPublish()
	assert.Equal(t, "new", string(got.Payload), "the session carries on")
	assert.False(t, got.Dup)
}

// [MQTT-4.4.0-1]: unsubscribing does not withdraw a message already sent and not
// acknowledged: it is the client's to settle, and the restored session resends
// it with its original identifier. Nothing new is delivered on the filter.
func TestAnInflightMessageOfAnUnsubscribedFilterIsStillResentOnResume(t *testing.T) {
	natsURL := startNATS(t)
	addrA, stopA := startStoppableBroker(t, natsURL, persistentWithQueue)
	addrB := startBroker(t, natsURL, persistentWithQueue)

	c := dialRaw(t, addrA)
	c.connect(rawConnect("restore-unsub", 300))
	c.subscribe("rs/unsub/#", packet.QoS1)
	pub, _ := connectClient(t, addrA, connectOpts("pub-restore-unsub"))
	pub.publish(&paho.Publish{Topic: "rs/unsub/a", QoS: 1, Payload: []byte("owed")})
	sent := c.expectPublish()
	c.unsubscribe("rs/unsub/#")
	c.drop()
	time.Sleep(100 * time.Millisecond)
	stopA()

	back := dialRaw(t, addrB)
	require.True(t, back.connect(rawConnect("restore-unsub", 300)).SessionPresent)
	got := back.expectPublish()
	assert.Equal(t, sent.PacketID, got.PacketID, "[MQTT-4.4.0-1]")
	assert.Equal(t, "owed", string(got.Payload))
	assert.True(t, got.Dup)
	back.send(&packet.Puback{Ack: packet.Ack{PacketID: got.PacketID}})

	pub2, _ := connectClient(t, addrB, connectOpts("pub-restore-unsub2"))
	pub2.publish(&paho.Publish{Topic: "rs/unsub/b", QoS: 1, Payload: []byte("after")})
	back.expectNothing()
}
