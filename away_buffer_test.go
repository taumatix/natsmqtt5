package natsmqtt5_test

import (
	"fmt"
	"testing"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// A broker with no offline queue (no JetStream here) holds the QoS 1 and 2 messages
// published while a persistent session is away, in memory and bounded, and a client
// told Session Present 1 gets all of them, in order, before anything newer
// [MQTT-3.1.2-23], [MQTT-4.5.0-1].
func TestWithoutJetStreamWhatWasPublishedWhileAwayIsDeliveredOnResume(t *testing.T) {
	var logs syncBuffer
	b, addr, _ := startBrokerHandle(t, startNATSWithoutJetStream(t), withoutJetStream, loggingBroker(t, &logs))

	away, _ := sessionPresentOnConnect(t, addr, "held-1", false, 300)
	away.subscribe("h/#", packet.QoS1)
	away.send(&packet.Disconnect{})
	waitDetached(t, &logs, "held-1")

	pub, _ := connectClient(t, addr, connectOpts("pub-held"))
	seen := natsmqtt5.LiveCopies(b)
	for i := 0; i < 5; i++ {
		pub.publish(&paho.Publish{Topic: "h/x", QoS: 1, Payload: []byte(fmt.Sprint(i))})
	}
	pub.publish(&paho.Publish{Topic: "h/x", QoS: 0, Payload: []byte("qos0")})
	awaitLiveCopies(t, b, seen, 6)

	back, present := sessionPresentOnConnect(t, addr, "held-1", false, 300)
	require.True(t, present)
	for i := 0; i < 5; i++ {
		p := back.expectPublish()
		assert.Equal(t, fmt.Sprint(i), string(p.Payload), "in publish order")
		back.send(&packet.Puback{Ack: packet.Ack{PacketID: p.PacketID}})
	}
	pub.publish(&paho.Publish{Topic: "h/x", QoS: 1, Payload: []byte("after")})
	assert.Equal(t, "after", string(back.expectPublish().Payload), "live delivery follows the held messages")
}

// When more is published than the bound holds, the broker cannot keep its promise, so it does
// not make it: the session ends, Session Present is 0 and the client subscribes again
// [MQTT-3.2.2-3], rather than 1 with a gap [MQTT-3.1.2-23], [MQTT-4.5.0-1].
func TestWithoutJetStreamASessionThatOverflowedItsBufferIsNotPresent(t *testing.T) {
	var logs syncBuffer
	b, addr, _ := startBrokerHandle(t, startNATSWithoutJetStream(t), withoutJetStream, loggingBroker(t, &logs),
		func(o *natsmqtt5.Options) { o.AwayBufferMessages = 3 })

	away, _ := sessionPresentOnConnect(t, addr, "held-2", false, 300)
	away.subscribe("o/#", packet.QoS1)
	away.send(&packet.Disconnect{})
	waitDetached(t, &logs, "held-2")

	pub, _ := connectClient(t, addr, connectOpts("pub-over"))
	seen := natsmqtt5.LiveCopies(b)
	for i := 0; i < 4; i++ {
		pub.publish(&paho.Publish{Topic: "o/x", QoS: 1, Payload: []byte(fmt.Sprint(i))})
	}
	awaitLiveCopies(t, b, seen, 4)

	back, present := sessionPresentOnConnect(t, addr, "held-2", false, 300)
	assert.False(t, present, "messages were lost, so the session is not kept [MQTT-3.2.2-3]")
	back.expectNothing()
	assert.Contains(t, logs.String(), "buffer")
}

// A session that was away and filled the bound exactly lost nothing and is still present.
func TestWithoutJetStreamABufferExactlyFullKeepsTheSession(t *testing.T) {
	var logs syncBuffer
	b, addr, _ := startBrokerHandle(t, startNATSWithoutJetStream(t), withoutJetStream, loggingBroker(t, &logs),
		func(o *natsmqtt5.Options) { o.AwayBufferMessages = 3 })

	away, _ := sessionPresentOnConnect(t, addr, "held-3", false, 300)
	away.subscribe("e/#", packet.QoS1)
	away.send(&packet.Disconnect{})
	waitDetached(t, &logs, "held-3")

	pub, _ := connectClient(t, addr, connectOpts("pub-exact"))
	seen := natsmqtt5.LiveCopies(b)
	for i := 0; i < 3; i++ {
		pub.publish(&paho.Publish{Topic: "e/x", QoS: 1, Payload: []byte(fmt.Sprint(i))})
	}
	awaitLiveCopies(t, b, seen, 3)

	back, present := sessionPresentOnConnect(t, addr, "held-3", false, 300)
	require.True(t, present)
	for i := 0; i < 3; i++ {
		assert.Equal(t, fmt.Sprint(i), string(back.expectPublish().Payload))
	}
}

// With the buffer turned off a broker without a queue cannot hold anything, and so
// answers Session Present 0 for a session that missed a QoS 1 message
// [MQTT-3.2.2-3], [MQTT-3.1.2-23], [MQTT-4.5.0-1].
func TestWithoutJetStreamWithTheBufferOffAMissedMessageEndsTheSession(t *testing.T) {
	var logs syncBuffer
	b, addr, _ := startBrokerHandle(t, startNATSWithoutJetStream(t), withoutJetStream, loggingBroker(t, &logs),
		func(o *natsmqtt5.Options) { o.AwayBufferMessages = -1 })

	away, _ := sessionPresentOnConnect(t, addr, "held-4", false, 300)
	away.subscribe("f/#", packet.QoS1)
	away.send(&packet.Disconnect{})
	waitDetached(t, &logs, "held-4")

	pub, _ := connectClient(t, addr, connectOpts("pub-off"))
	seen := natsmqtt5.LiveCopies(b)
	pub.publish(&paho.Publish{Topic: "f/x", QoS: 1, Payload: []byte("1")})
	awaitLiveCopies(t, b, seen, 1)

	_, present := sessionPresentOnConnect(t, addr, "held-4", false, 300)
	assert.False(t, present)
}

// A session that missed only QoS 0 messages lost nothing it was owed and stays present.
func TestWithoutJetStreamMissedQoS0KeepsTheSession(t *testing.T) {
	var logs syncBuffer
	b, addr, _ := startBrokerHandle(t, startNATSWithoutJetStream(t), withoutJetStream, loggingBroker(t, &logs))

	away, _ := sessionPresentOnConnect(t, addr, "held-5", false, 300)
	away.subscribe("z/#", packet.QoS1)
	away.send(&packet.Disconnect{})
	waitDetached(t, &logs, "held-5")

	pub, _ := connectClient(t, addr, connectOpts("pub-q0"))
	seen := natsmqtt5.LiveCopies(b)
	pub.publish(&paho.Publish{Topic: "z/x", QoS: 0, Payload: []byte("0")})
	awaitLiveCopies(t, b, seen, 1)

	back, present := sessionPresentOnConnect(t, addr, "held-5", false, 300)
	assert.True(t, present)
	back.expectNothing()
}
