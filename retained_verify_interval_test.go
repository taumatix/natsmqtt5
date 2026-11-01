package natsmqtt5_test

import (
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5/v2"
)

// A partition shorter than the NATS client's ping interval triggers no reconnect, so the check after
// one never runs, and a stale entry that one undelivered message hides from the counts is served until
// the counts disagree. Options.RetainedVerifyInterval makes the sweep check every entry on a timer
// as well [MQTT-3.3.1-6]. The stale state is planted by hand, as in retained_reconnect_test.go: the
// consumer would deliver the message that cancels it within milliseconds. The counts agree here ("gone"
// leaves a tombstone, "fresh" is missing from the view), so only the full check finds the ghost.
func TestARetainedViewIsVerifiedOnATimerWhenAskedTo_MQTT_3_3_1_6(t *testing.T) {
	const interval = 400 * time.Millisecond

	natsURL := startNATS(t)
	broker, addr, _ := startBrokerHandle(t, natsURL, func(o *natsmqtt5.Options) {
		o.RetainedVerifyInterval = interval
	})
	pub, _ := connectClient(t, addr, connectOpts("rvi-pub"))
	pub.publish(&paho.Publish{Topic: "rvi/gone", QoS: 1, Retain: true, Payload: []byte("x")})
	pub.publish(&paho.Publish{Topic: "rvi/gone", QoS: 1, Retain: true})
	pub.publish(&paho.Publish{Topic: "rvi/fresh", QoS: 1, Retain: true, Payload: []byte("fresh")})
	natsmqtt5.RetainedPlant(broker, "rvi/ghost", "ghost", 9999)
	natsmqtt5.RetainedForget(broker, "rvi/fresh")

	natsmqtt5.RetainedSweep(broker, 0)
	_, has := natsmqtt5.RetainedPayload(broker, "rvi/ghost")
	assert.True(t, has, "the interval has not passed, and the counts agree")

	time.Sleep(interval + 100*time.Millisecond)
	natsmqtt5.RetainedSweep(broker, 0)
	_, has = natsmqtt5.RetainedPayload(broker, "rvi/ghost")
	assert.False(t, has, "the interval has passed: every entry is checked against the stream")
}

func TestARetainedViewIsNotVerifiedOnATimerByDefault(t *testing.T) {
	natsURL := startNATS(t)
	broker, addr, _ := startBrokerHandle(t, natsURL)
	pub, _ := connectClient(t, addr, connectOpts("rvd-pub"))
	pub.publish(&paho.Publish{Topic: "rvd/gone", QoS: 1, Retain: true, Payload: []byte("x")})
	pub.publish(&paho.Publish{Topic: "rvd/gone", QoS: 1, Retain: true})
	pub.publish(&paho.Publish{Topic: "rvd/fresh", QoS: 1, Retain: true, Payload: []byte("fresh")})
	natsmqtt5.RetainedPlant(broker, "rvd/ghost", "ghost", 9999)
	natsmqtt5.RetainedForget(broker, "rvd/fresh")

	time.Sleep(500 * time.Millisecond)
	natsmqtt5.RetainedSweep(broker, 0)
	_, has := natsmqtt5.RetainedPayload(broker, "rvd/ghost")
	require.True(t, has, "off by default: only a count mismatch or a reconnect triggers the full check")
}
