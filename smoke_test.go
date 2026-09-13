package natsmqtt5_test

import (
	"os"
	"testing"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSmoke drives a broker that is already running somewhere else — the
// container image, in CI — over a real socket. Everything else in this suite
// builds its broker in-process, which proves the code works but says nothing
// about whether the image starts, reaches NATS, and accepts connections.
//
// It is skipped unless NATSMQTT5_SMOKE_ADDR names that broker:
//
//	NATSMQTT5_SMOKE_ADDR=127.0.0.1:1883 go test -run TestSmoke -count=1 .
func TestSmoke(t *testing.T) {
	addr := os.Getenv("NATSMQTT5_SMOKE_ADDR")
	if addr == "" {
		t.Skip("set NATSMQTT5_SMOKE_ADDR to the host:port of a running broker")
	}

	sub, connack := connectClient(t, addr, connectOpts("smoke-sub"))
	assert.False(t, connack.SessionPresent, "a clean start must not resume a session")
	sub.subscribe(paho.SubscribeOptions{Topic: "smoke/#", QoS: 2})

	// QoS 2 exercises the full four-packet exchange in both directions.
	pub, _ := connectClient(t, addr, connectOpts("smoke-pub"))
	pub.publish(&paho.Publish{Topic: "smoke/hello", QoS: 2, Payload: []byte("hello")})

	got := sub.expectMessage()
	assert.Equal(t, "smoke/hello", got.Topic)
	assert.Equal(t, "hello", got.Payload)

	// A retained message goes through JetStream, so this is also the check
	// that the broker reached a NATS server with JetStream enabled.
	pub.publish(&paho.Publish{Topic: "smoke/state", QoS: 1, Retain: true, Payload: []byte("v1")})

	late, _ := connectClient(t, addr, connectOpts("smoke-late"))
	late.subscribe(paho.SubscribeOptions{Topic: "smoke/state", QoS: 1})

	retained := late.expectMessage()
	assert.Equal(t, "v1", retained.Payload)
	assert.True(t, retained.Retain, "a retained message must arrive with the flag set")
}

// TestSmokeSessionMovesBetweenBrokers is the same claim as
// TestPersistentSessionSurvivesABrokerRestart, made against two brokers in
// separate containers rather than two in one process. In-process the session
// store shares a Go heap with everything it coordinates; here the only thing
// the two brokers have in common is the NATS server, which is the arrangement a
// deployment actually has.
//
//	NATSMQTT5_SMOKE_ADDR=127.0.0.1:1883 NATSMQTT5_SMOKE_ADDR_B=127.0.0.1:1884 \
//	  go test -run TestSmokeSessionMovesBetweenBrokers -count=1 .
func TestSmokeSessionMovesBetweenBrokers(t *testing.T) {
	addrA, addrB := os.Getenv("NATSMQTT5_SMOKE_ADDR"), os.Getenv("NATSMQTT5_SMOKE_ADDR_B")
	if addrA == "" || addrB == "" {
		t.Skip("set NATSMQTT5_SMOKE_ADDR and NATSMQTT5_SMOKE_ADDR_B to two brokers sharing a NATS server")
	}

	const clientID = "smoke-roamer"

	// The JetStream volume outlives a `docker compose down`, so start by
	// clearing anything a previous run left: a Clean Start deletes the stored
	// record, and a Session Expiry Interval of zero means this connection
	// leaves none of its own behind.
	wipe, _ := connectClient(t, addrA, connectOpts(clientID))
	require.NoError(t, wipe.Client.Disconnect(&paho.Disconnect{ReasonCode: 0}))

	onA, connack := connectClient(t, addrA, durableConnect(clientID, 300))
	require.False(t, connack.SessionPresent, "the run started from a clean session")
	onA.subscribe(paho.SubscribeOptions{Topic: "smoke/roam/#", QoS: 1})
	require.NoError(t, onA.Client.Disconnect(&paho.Disconnect{ReasonCode: 0}))

	onB, connack := connectClient(t, addrB, durableConnect(clientID, 300))
	assert.True(t, connack.SessionPresent,
		"the second broker must resume a session it has never served")

	// Published through the first broker, received on the second, on a
	// subscription that was never made there.
	pub, _ := connectClient(t, addrA, connectOpts("smoke-roam-pub"))
	pub.publish(&paho.Publish{Topic: "smoke/roam/x", QoS: 1, Payload: []byte("crossed")})

	got := onB.expectMessage()
	assert.Equal(t, "smoke/roam/x", got.Topic)
	assert.Equal(t, "crossed", got.Payload)
}
