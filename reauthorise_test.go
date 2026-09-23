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
