package natsmqtt5_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/taumatix/natsmqtt5/v2"
	"github.com/taumatix/natsmqtt5/v2/packet"
)

// A message published after a SUBACK reaches the subscriber once, as the live copy the
// subscription asked for [MQTT-3.3.1-12], and not a second time as a retained message
// [MQTT-3.3.1-6]. The retained set a subscription is owed is the one that stood when its
// interest reached NATS; the retained set was once read after the SUBACK was written, so a
// publisher that waited for the SUBACK could store its message first and the subscriber got
// it again with RETAIN set (seen in CI as TestPublish_MQTT_3_3_1_12_and_13 on a slow
// runner). The gate holds the SUBSCRIBE between its SUBACK and its retained messages for as
// long as the publisher takes.
func TestARetainedMessageStoredAfterTheSubackIsNotSentAgainToTheSubscriber_MQTT_3_3_1_12(t *testing.T) {
	natsURL := startNATS(t)
	b, addr, _ := startBrokerHandle(t, natsURL)
	pubAddr := startBroker(t, natsURL)

	stored := make(chan struct{})
	natsmqtt5.SetSubackGate(b, func() {
		select {
		case <-stored:
		case <-time.After(10 * time.Second):
		}
	})

	sub, _ := p2Connect(t, addr, "late-sub")
	p2Sub(t, sub.rawClient, packet.Subscription{Filter: "late/#", QoS: 0})

	pub, _ := p2Connect(t, pubAddr, "late-pub")
	pub.send(&packet.Publish{Retain: true, Topic: "late/x", Payload: []byte("m")})
	live := p2Next[*packet.Publish](t, sub.rawClient)
	assert.False(t, live.Retain, "the live copy, RETAIN cleared as Retain As Published is 0")
	close(stored)
	sub.expectNothing()
}
