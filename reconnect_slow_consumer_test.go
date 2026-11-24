package natsmqtt5_test

import (
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"

	"github.com/taumatix/natsmqtt5"
)

// Options.ReconnectCatchUp when nats.go drops a live copy on the subscriber's broker because the
// subscription's handler fell behind its pending limits (a slow consumer). The connection stays
// up and nothing but the connection's error handler hears of it; the queue holds the copy, so
// the broker treats the report as a short drop and its connected sessions catch up from the
// queue: every message, in order, once (MQTT-5.0 §4.3.2, [MQTT-4.6.0-5]).
//
// The handler is held on a gate and the subscription capped at two pending messages, so the
// copies behind the held one overflow deterministically.
func TestReconnectCatchUpSendsWhatNATSDroppedAsASlowConsumer(t *testing.T) {
	natsURL := startNATS(t)
	subBroker, subAddr, _ := startBrokerHandle(t, natsURL, withReconnectCatchUp)
	pubAddr := startBroker(t, natsURL)

	release := make(chan struct{})
	var once sync.Once
	open := func() { once.Do(func() { close(release) }) }
	t.Cleanup(open)
	natsmqtt5.SetLiveSlowConsumer(subBroker, 2, func() { <-release })

	sub, _ := connectClient(t, subAddr, connectOpts("slow-sub"))
	sub.subscribe(paho.SubscribeOptions{Topic: "slow/one", QoS: 1})
	pub, _ := connectClient(t, pubAddr, connectOpts("slow-pub"))

	want := []string{"m1", "m2", "m3", "m4", "m5", "m6", "m7", "m8"}
	for _, p := range want {
		pub.publish(&paho.Publish{Topic: "slow/one", QoS: 1, Payload: []byte(p)})
	}
	// The first copy is held in the handler, two wait behind it, the rest are dropped.
	time.Sleep(500 * time.Millisecond)
	natsmqtt5.SetLiveSlowConsumer(subBroker, 0, nil)
	open()

	sub.expectPayloads(want...)
}
