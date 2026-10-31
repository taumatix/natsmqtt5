package natsmqtt5_test

import (
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// refuseThenRetry drives the refused flush of durable_flush_fail_test.go (the queue copy is stored, NATS then
// stays away, the PUBACK is 0x83) and returns the queue stream and subject after the client's retry has been
// acknowledged. retry sends the PUBLISH again, under the packet identifier of the refused one.
func refuseThenRetry(t *testing.T, clientID, topic string, retry func(pub *rawClient)) (jetstream.Stream, string) {
	t.Helper()
	natsURL := startNATS(t)
	proxy := newCutProxy(t, natsURL)
	proxy.stayDown.Store(true)
	durable := startBroker(t, proxy.url(), func(o *natsmqtt5.Options) {
		o.DurablePublish = true
		o.DurablePublishDedupe = true
	})

	pub := dialRaw(t, durable)
	pub.connect(rawConnect(clientID, 0))
	proxy.armed.Store(true)
	rawPublishQoS(pub, packet.QoS1, 1, topic, "first")
	ack, ok := pub.readWithin(20 * time.Second).(*packet.Puback)
	require.True(t, ok)
	require.Equal(t, packet.ImplementationSpecificError, ack.ReasonCode)

	proxy.refuse.Store(false)
	require.Eventually(t, func() bool {
		retry(pub)
		a, ok := pub.readWithin(20 * time.Second).(*packet.Puback)
		return ok && a.ReasonCode == packet.Success
	}, 60*time.Second, time.Second)
	return queueStream(t, natsURL), strings.ReplaceAll(topic, "/", ".")
}

// With DurablePublishDedupe the retry of a refused publish (same packet identifier, DUP set, same message)
// carries the first attempt's Nats-Msg-Id, so the queue stream drops it inside its duplicate window and a
// session that was away is offered the message once.
func TestDurablePublishDedupeDropsTheRetryOfARefusedFlushFromTheQueue(t *testing.T) {
	t.Parallel()
	stream, subject := refuseThenRetry(t, "dd-pub", "dd/retry", func(pub *rawClient) {
		pub.send(&packet.Publish{Topic: "dd/retry", QoS: packet.QoS1, PacketID: 1, Dup: true, Payload: []byte("first")})
	})
	assert.Equal(t, 1, queuedCopies(t, stream, subject))
}

// A retransmission that is a different message under the same packet identifier is a new message: dropping
// its queue copy as the duplicate would lose it for a session that is away.
func TestDurablePublishDedupeKeepsADifferentMessageSentUnderTheRefusedPacketID(t *testing.T) {
	t.Parallel()
	stream, subject := refuseThenRetry(t, "dd-other-pub", "dd/other", func(pub *rawClient) {
		pub.send(&packet.Publish{Topic: "dd/other", QoS: packet.QoS1, PacketID: 1, Dup: true, Payload: []byte("second")})
	})
	assert.Equal(t, 2, queuedCopies(t, stream, subject))
}

// Without DUP the same packet identifier, topic and payload is a new publish by the client's own account, so
// it is queued again.
func TestDurablePublishDedupeKeepsAnIdenticalPublishThatIsNotMarkedAsARetransmission(t *testing.T) {
	t.Parallel()
	stream, subject := refuseThenRetry(t, "dd-nodup-pub", "dd/nodup", func(pub *rawClient) {
		pub.send(&packet.Publish{Topic: "dd/nodup", QoS: packet.QoS1, PacketID: 1, Payload: []byte("first")})
	})
	assert.Equal(t, 2, queuedCopies(t, stream, subject))
}

// Once a publish is acknowledged its identifier is forgotten: the same packet identifier reused later for the
// same topic and payload, even marked DUP, is a new message inside the duplicate window and is queued.
func TestDurablePublishDedupeDoesNotDropALaterMessageThatReusesAnAcknowledgedPacketID(t *testing.T) {
	t.Parallel()
	natsURL := startNATS(t)
	b := startBroker(t, natsURL, func(o *natsmqtt5.Options) {
		o.DurablePublish = true
		o.DurablePublishDedupe = true
	})
	pub := dialRaw(t, b)
	pub.connect(rawConnect("dd-reuse-pub", 0))
	for i := range 2 {
		pub.send(&packet.Publish{Topic: "dd/reuse", QoS: packet.QoS1, PacketID: 1, Dup: i == 1, Payload: []byte("same")})
		a, ok := pub.readWithin(10 * time.Second).(*packet.Puback)
		require.True(t, ok)
		require.Equal(t, packet.Success, a.ReasonCode)
	}
	assert.Equal(t, 2, queuedCopies(t, queueStream(t, natsURL), "dd.reuse"))
}

func TestDurablePublishDedupeNeedsDurablePublish(t *testing.T) {
	t.Parallel()
	_, err := natsmqtt5.New(natsmqtt5.Options{DurablePublishDedupe: true})
	require.ErrorIs(t, err, natsmqtt5.ErrInvalidOptions)
}
