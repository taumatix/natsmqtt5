package natsmqtt5_test

import (
	"os"
	"testing"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
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
