package natsmqtt5_test

import (
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
)

// Options.ReconnectCatchUp when the route between two NATS servers goes, with both brokers
// connected to their own server throughout: the publisher's broker stores each message in the
// replicated queue (a majority of the stream's replicas is still reachable) and publishes its live
// copy, which the cut route cannot carry. Neither broker's connection drops, so nats.go reports
// nothing; the subscriber's server tells its clients the cluster's servers changed, and the broker
// catches up from the queue. The subscriber gets the messages published while the route was down,
// once and in order, then the one published after (MQTT-5.0 §4.3.2, [MQTT-4.6.0-5]).
//
// The servers ping every 2 s so that a route cut in both directions is noticed in seconds, and the
// isolation is shorter than the broker's own check of its Will Messages' bucket, which closes
// clients that stay cut off longer.
func TestClusterReconnectCatchUpSendsWhatARouteLostWhileBothBrokersStayConnected(t *testing.T) {
	c := startNATSClusterWith(t, "ping_interval: \"2s\"\nping_max: 1")
	subBroker, subAddr, _ := startBrokerHandle(t, c.URL(1), clusterReplicas, quickReconnect, withReconnectCatchUp)
	pubAddr := startBroker(t, c.URL(0), clusterReplicas)
	// The stream's leader is on the publisher's side of the cut, so its writes are acknowledged
	// without an election.
	c.LeadStream(natsmqtt5.DefaultStreamPrefix+"_queue", 0)

	sub, _ := connectClient(t, subAddr, connectOpts("rtc-sub"))
	sub.subscribe(paho.SubscribeOptions{Topic: "rtc/one", QoS: 1})
	pub, _ := connectClient(t, pubAddr, connectOpts("rtc-pub"))
	pub.publish(&paho.Publish{Topic: "rtc/one", QoS: 1, Payload: []byte("before")})
	sub.expectPayloads("before")

	servers := len(subBroker.NATS().DiscoveredServers())
	require.NotZero(t, servers, "the server advertises its cluster to its clients")
	c.Isolate(1)
	require.Eventually(t, func() bool { return len(subBroker.NATS().DiscoveredServers()) < servers }, 30*time.Second,
		50*time.Millisecond, "the subscriber's server never told its clients a route was gone")
	require.True(t, subBroker.NATS().IsConnected(), "the broker's own connection stays up: that is the case")
	for _, p := range []string{"during-1", "during-2"} {
		pub.publish(&paho.Publish{Topic: "rtc/one", QoS: 1, Payload: []byte(p)})
	}
	c.Heal(1)
	require.Eventually(t, func() bool { return len(subBroker.NATS().DiscoveredServers()) >= servers }, 60*time.Second,
		50*time.Millisecond, "the routes never came back")
	time.Sleep(3 * time.Second) // the catch-up starts once the servers have stayed the same
	pub.publish(&paho.Publish{Topic: "rtc/one", QoS: 1, Payload: []byte("after")})

	sub.expectPayloads("during-1", "during-2", "after")
}
