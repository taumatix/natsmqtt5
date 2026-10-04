package natsmqtt5_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// Brokers that share a NATS cluster serve one MQTT namespace, and a deployment
// that learns of a revocation has no easy way to know which broker holds the
// client. Reauthorize on any of them reaches the one that does.
func TestReauthorizeOnAnotherBrokerReachesTheOneHoldingTheSession(t *testing.T) {
	var revoked atomic.Bool
	natsURL := startNATS(t)
	_, addrA, _ := startBrokerHandle(t, natsURL, revocable(&revoked))
	b, _, _ := startBrokerHandle(t, natsURL, revocable(&revoked))

	r := dialRaw(t, addrA)
	r.connect(rawConnect("reauth-cluster", 300))
	r.subscribe("secret/#", packet.QoS0)
	r.subscribe("open/#", packet.QoS0)
	pub, _ := connectClient(t, addrA, connectOpts("pub-reauth-cluster"))

	revoked.Store(true)
	require.NoError(t, b.Reauthorize(context.Background(), "reauth-cluster"))

	pub.publish(&paho.Publish{Topic: "secret/1", Payload: []byte("1")})
	pub.publish(&paho.Publish{Topic: "open/1", Payload: []byte("1")})
	got := r.expectPublish()
	assert.Equal(t, "open/1", got.Topic, "the broker holding the session was not reached")
	r.expectNothing()
}

// A broker with another SubjectPrefix serves another namespace, whose Client
// Identifiers are its own: its Reauthorize must not reach this one's sessions.
func TestReauthorizeStaysWithinItsSubjectPrefix(t *testing.T) {
	var revoked atomic.Bool
	natsURL := startNATS(t)
	_, addrA, _ := startBrokerHandle(t, natsURL, revocable(&revoked))
	other, _, _ := startBrokerHandle(t, natsURL, revocable(&revoked), func(o *natsmqtt5.Options) {
		o.SubjectPrefix = "other"
		o.DisableRetained = true
	})

	r := dialRaw(t, addrA)
	r.connect(rawConnect("reauth-prefix", 300))
	r.subscribe("secret/#", packet.QoS0)
	pub, _ := connectClient(t, addrA, connectOpts("pub-reauth-prefix"))

	revoked.Store(true)
	require.NoError(t, other.Reauthorize(context.Background(), "reauth-prefix"))

	pub.publish(&paho.Publish{Topic: "secret/1", Payload: []byte("1")})
	assert.Equal(t, "secret/1", r.expectPublish().Topic, "a broker in another namespace swept this one's session")
}

// Nobody holding the session is the ordinary case for a client that is offline:
// it is re-checked when it next connects. The answer is nil, after a short wait
// for a broker that might hold it.
func TestReauthorizeOfAClientNoBrokerHoldsReturnsPromptly(t *testing.T) {
	var revoked atomic.Bool
	natsURL := startNATS(t)
	b, _, _ := startBrokerHandle(t, natsURL, revocable(&revoked))
	startBrokerHandle(t, natsURL, revocable(&revoked))

	start := time.Now()
	require.NoError(t, b.Reauthorize(context.Background(), "nobody"))
	assert.Less(t, time.Since(start), 5*time.Second)
}

// The caller's context bounds the wait for a broker holding the session.
func TestReauthorizeHonoursItsContextWhileAsking(t *testing.T) {
	var revoked atomic.Bool
	b, _, _ := startBrokerHandle(t, startNATS(t), revocable(&revoked))
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	_ = b.Reauthorize(ctx, "nobody")
	assert.Less(t, time.Since(start), 500*time.Millisecond)
}
