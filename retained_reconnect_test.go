package natsmqtt5_test

import (
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5/v2"
)

// The retained view is compared with the stream by count, and a count cannot see a
// stale entry that one message the consumer has not delivered yet cancels out. After
// the NATS client reconnects, the sweep checks every entry instead [MQTT-3.3.1-6].
// The view is put in that state by hand (the consumer would deliver the missing
// message within milliseconds, which is why the state cannot be waited for): "ghost"
// is an entry for a topic the stream cleared, "swap" holds an older message than the
// stream's, and "fresh" is in the stream and not yet in the view, so the counts match.
func TestARetainedViewIsVerifiedAfterANATSReconnectEvenWhenTheCountsAgree(t *testing.T) {
	natsURL := startNATS(t)
	broker, addr, _ := startBrokerHandle(t, natsURL)
	pub, _ := connectClient(t, addr, connectOpts("rv-pub"))
	pub.publish(&paho.Publish{Topic: "rv/gone", QoS: 1, Retain: true, Payload: []byte("x")})
	pub.publish(&paho.Publish{Topic: "rv/gone", QoS: 1, Retain: true}) // cleared: a tombstone
	pub.publish(&paho.Publish{Topic: "rv/swap", QoS: 1, Retain: true, Payload: []byte("new")})
	pub.publish(&paho.Publish{Topic: "rv/fresh", QoS: 1, Retain: true, Payload: []byte("fresh")})

	// The view's entries: ghost (stale) and swap (stale), not fresh. The tombstone
	// is in the view too, so it holds as many entries as the stream has messages.
	natsmqtt5.RetainedPlant(broker, "rv/ghost", "ghost", 9999)
	natsmqtt5.RetainedPlant(broker, "rv/swap", "old", 9998)
	natsmqtt5.RetainedForget(broker, "rv/fresh")

	natsmqtt5.RetainedSweep(broker, 0)
	_, has := natsmqtt5.RetainedPayload(broker, "rv/ghost")
	assert.True(t, has, "a count that agrees hides the stale entry; the check after a reconnect is what finds it")

	natsmqtt5.RetainedSweep(broker, 1)
	_, has = natsmqtt5.RetainedPayload(broker, "rv/ghost")
	assert.False(t, has, "[MQTT-3.3.1-6] the stream cleared this topic")
	got, has := natsmqtt5.RetainedPayload(broker, "rv/swap")
	require.True(t, has)
	assert.Equal(t, "new", got, "the stream's message replaces the older one the view held")
}

// The check after a reconnect runs when the NATS client reconnects, not at the next session sweep,
// which is up to SessionSweepInterval later (an hour here): until it runs, a subscriber is sent the
// stale entry of a topic the stream cleared. [MQTT-3.3.1-6]: "If the Payload contains zero bytes it
// is processed normally by the Server but any retained message with the same topic name MUST be
// removed and any future subscribers for the topic will not receive a retained message." The stale
// entry is planted by hand, as above; the reconnect is real (the proxy between the broker and NATS
// closes its connections).
func TestARetainedViewIsVerifiedAsSoonAsTheNATSClientReconnects_MQTT_3_3_1_6(t *testing.T) {
	natsURL := startNATS(t)
	proxy := newCutProxy(t, natsURL)
	broker, addr, _ := startBrokerHandle(t, proxy.url(), func(o *natsmqtt5.Options) { o.SessionSweepInterval = time.Hour })
	pub, _ := connectClient(t, addr, connectOpts("rv-now-pub"))
	pub.publish(&paho.Publish{Topic: "rv/kept", QoS: 1, Retain: true, Payload: []byte("kept")})
	pub.publish(&paho.Publish{Topic: "rv/fresh", QoS: 1, Retain: true, Payload: []byte("fresh")})
	natsmqtt5.RetainedPlant(broker, "rv/ghost", "ghost", 9999)
	natsmqtt5.RetainedForget(broker, "rv/fresh")

	proxy.sever()
	require.True(t, natsmqtt5.AwaitNATSReconnect(broker, 10*time.Second))
	assert.Eventually(t, func() bool {
		_, has := natsmqtt5.RetainedPayload(broker, "rv/ghost")
		return !has
	}, 5*time.Second, 20*time.Millisecond, "[MQTT-3.3.1-6] the stream holds no message for this topic")
	got, has := natsmqtt5.RetainedPayload(broker, "rv/kept")
	assert.True(t, has && got == "kept", "an entry the stream holds is kept")
}
