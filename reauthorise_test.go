package natsmqtt5_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// A session is keyed by its Client Identifier alone, so whoever the
// Authenticator lets use that identifier inherits the session and everything it
// is subscribed to. That is why a resumed session's filters are put back past
// the Authorizer rather than trusted.
//
// TestPersistentSessionReauthorisesRestoredSubscriptions covers the session
// rebuilt from a durable record. These cover the session this broker still
// holds in memory, where the NATS subscriptions were never torn down and so
// there is nothing to "restore" — the check has to reach the live set instead.

// narrowingAuthorizer denies ActionSubscribe on "secret/#" once deny is set. The
// flag is atomic because the broker reads it on its own goroutines while the
// test writes it.
func narrowingAuthorizer(deny *atomic.Bool) func(*natsmqtt5.Options) {
	return func(o *natsmqtt5.Options) {
		o.Authorizer = natsmqtt5.AuthorizerFunc(func(_ context.Context, req *natsmqtt5.AuthzRequest) error {
			if req.Action == natsmqtt5.ActionSubscribe && req.Topic == "secret/#" && deny.Load() {
				return errors.New("no longer permitted")
			}
			return nil
		})
	}
}

// A permission narrowed between two connections must take effect on the second
// one, not when the session eventually expires.
func TestInMemorySessionReauthorisesSubscriptionsOnResume(t *testing.T) {
	var deny atomic.Bool
	addr := startBroker(t, startNATS(t), narrowingAuthorizer(&deny))

	first, _ := connectClient(t, addr, durableConnect("narrowed-memory", 300))
	first.subscribe(
		paho.SubscribeOptions{Topic: "secret/#", QoS: 1},
		paho.SubscribeOptions{Topic: "public/#", QoS: 1},
	)
	require.NoError(t, first.Client.Disconnect(&paho.Disconnect{ReasonCode: 0}))

	// The client's permissions are narrowed while it is away.
	deny.Store(true)

	second, connack := connectClient(t, addr, durableConnect("narrowed-memory", 300))
	require.True(t, connack.SessionPresent,
		"the same broker still holds the session, so this is the in-memory branch")

	pub, _ := connectClient(t, addr, connectOpts("pub"))
	pub.publish(&paho.Publish{Topic: "secret/a", QoS: 1, Payload: []byte("classified")})
	second.expectNoMessage()

	// The filter that is still permitted has to survive: dropping the whole
	// subscription set would pass the assertion above for the wrong reason.
	pub.publish(&paho.Publish{Topic: "public/a", QoS: 1, Payload: []byte("fine")})
	assert.Equal(t, "fine", second.expectMessage().Payload)
}

// The discriminating half: with the permission left alone, the same reconnect
// must keep delivering. Without this, a broker that silently dropped every
// resumed subscription would pass the test above.
func TestInMemorySessionKeepsSubscriptionsThatAreStillPermitted(t *testing.T) {
	var deny atomic.Bool
	addr := startBroker(t, startNATS(t), narrowingAuthorizer(&deny))

	first, _ := connectClient(t, addr, durableConnect("unchanged-memory", 300))
	first.subscribe(paho.SubscribeOptions{Topic: "secret/#", QoS: 1})
	require.NoError(t, first.Client.Disconnect(&paho.Disconnect{ReasonCode: 0}))

	second, connack := connectClient(t, addr, durableConnect("unchanged-memory", 300))
	require.True(t, connack.SessionPresent)

	pub, _ := connectClient(t, addr, connectOpts("pub"))
	pub.publish(&paho.Publish{Topic: "secret/a", QoS: 1, Payload: []byte("still mine")})
	assert.Equal(t, "still mine", second.expectMessage().Payload)
}

// Retransmission on resume hands the previous connection's undelivered payloads
// over at CONNACK time, before the client has sent anything. A payload on a
// filter this connection may no longer have must not be among them — and one on
// a filter it still has must.
func TestWithdrawnFilterTakesItsUnacknowledgedMessagesWithIt(t *testing.T) {
	var deny atomic.Bool
	addr := startBroker(t, startNATS(t), narrowingAuthorizer(&deny))

	raw := dialRaw(t, addr)
	require.False(t, raw.connect(rawConnect("withdrawn-q1", 300)).SessionPresent)
	raw.subscribe("secret/#", packet.QoS1)
	raw.subscribe("public/#", packet.QoS1)

	pub, _ := connectClient(t, addr, connectOpts("pub"))
	pub.publish(&paho.Publish{Topic: "secret/a", QoS: 1, Payload: []byte("classified")})
	pub.publish(&paho.Publish{Topic: "public/a", QoS: 1, Payload: []byte("fine")})

	// Received and deliberately left unacknowledged, in the order they were
	// sent — which is the order a resend has to use [MQTT-4.6.0-5], so a broker
	// that resent the withdrawn one would be caught by the first read below.
	classified := raw.expectPublish()
	require.Equal(t, "secret/a", classified.Topic)
	require.Equal(t, "public/a", raw.expectPublish().Topic)
	raw.drop()

	deny.Store(true)

	resumed := dialRaw(t, addr)
	require.True(t, resumed.connect(rawConnect("withdrawn-q1", 300)).SessionPresent)

	again := resumed.expectPublish()
	assert.Equal(t, "public/a", again.Topic, "the permitted filter's arrears must still come back")
	assert.True(t, again.Dup)
	resumed.expectNothing()

	// The client flushes the acknowledgement it still owed from the previous
	// connection. The broker sent it that message, so answering with
	// 0x82 Protocol Error and disconnecting would punish the client for the
	// broker's own withdrawal.
	resumed.send(&packet.Puback{Ack: packet.Ack{PacketID: classified.PacketID}})
	resumed.expectNothing()
}

// The exception, and the reason the withdrawal is not simply "drop everything
// that no longer matches": past its PUBREC the client owns the message
// [MQTT-4.3.3-8], so what is outstanding is a PUBREL carrying no payload.
// Withholding it would leave the client waiting for a PUBCOMP forever and its
// Packet Identifier unusable, to prevent a disclosure that already happened.
func TestAWithdrawnFilterStillOwesItsPubrel(t *testing.T) {
	var deny atomic.Bool
	addr := startBroker(t, startNATS(t), narrowingAuthorizer(&deny))

	raw := dialRaw(t, addr)
	require.False(t, raw.connect(rawConnect("withdrawn-q2", 300)).SessionPresent)
	raw.subscribe("secret/#", packet.QoS2)

	pub, _ := connectClient(t, addr, connectOpts("pub"))
	pub.publish(&paho.Publish{Topic: "secret/a", QoS: 2, Payload: []byte("exactly once")})

	first := raw.expectPublish()
	require.Equal(t, packet.QoS2, first.QoS)
	raw.send(&packet.Pubrec{Ack: packet.Ack{PacketID: first.PacketID}})
	require.Equal(t, first.PacketID, raw.expectPubrel().PacketID)
	raw.drop()

	deny.Store(true)

	resumed := dialRaw(t, addr)
	require.True(t, resumed.connect(rawConnect("withdrawn-q2", 300)).SessionPresent)
	assert.Equal(t, first.PacketID, resumed.expectPubrel().PacketID)

	resumed.send(&packet.Pubcomp{Ack: packet.Ack{PacketID: first.PacketID}})
	resumed.expectNothing()
}
