package natsmqtt5_test

import (
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
)

// Options.ReconnectCatchUp across servers: the subscriber's broker is on one NATS server of a
// three-server cluster and the publisher's on another. The subscriber's server is killed, so
// its broker has no connection and no interest anywhere in the cluster; the publisher's
// messages are stored by the stream's surviving replicas and published live to nobody on that
// side. When the server is back and the broker reconnects, its connected QoS 1 subscriber gets
// them, once and in order, before what was published afterwards (MQTT-5.0 §4.3.2,
// [MQTT-4.6.0-5]).
func TestClusterReconnectCatchUpSendsWhatWasPublishedWhileTheSubscribersServerWasDown(t *testing.T) {
	c := startNATSCluster(t)
	// Only server 2: a broker that failed over to a server the cluster advertised would be back
	// before the publishes, and the test would prove nothing.
	onlyServer2 := func(o *natsmqtt5.Options) {
		o.NATSOptions = append(o.NATSOptions, nats.IgnoreDiscoveredServers())
	}
	subBroker, subAddr, _ := startBrokerHandle(t, c.URL(2), clusterReplicas, quickReconnect, onlyServer2,
		withReconnectCatchUp)
	pubAddr := startBroker(t, c.URL(0)+","+c.URL(1), clusterReplicas)
	// The stream's leader is not on the server about to be killed, so the publishes during the
	// outage are stored at once, without an election (a refused publish retried would be a
	// second copy, which QoS 1 allows but this test would count).
	c.LeadStream(natsmqtt5.DefaultStreamPrefix+"_queue", 0)

	sub, _ := connectClient(t, subAddr, connectOpts("rcc-sub"))
	sub.subscribe(paho.SubscribeOptions{Topic: "rcc/one", QoS: 1})
	pub, _ := connectClient(t, pubAddr, connectOpts("rcc-pub"))

	pub.publish(&paho.Publish{Topic: "rcc/one", QoS: 1, Payload: []byte("before")})
	sub.expectPayloads("before")

	c.StopServer(2, false)
	require.Eventually(t, func() bool { return !subBroker.NATS().IsConnected() }, 30*time.Second,
		10*time.Millisecond, "the subscriber's broker never noticed its server was gone")
	for _, p := range []string{"during-1", "during-2", "during-3"} {
		pub.publish(&paho.Publish{Topic: "rcc/one", QoS: 1, Payload: []byte(p)})
	}
	c.StartServer(2)
	require.Eventually(t, func() bool { return subBroker.NATS().IsConnected() }, 60*time.Second,
		50*time.Millisecond, "the subscriber's broker never reconnected")
	pub.publish(&paho.Publish{Topic: "rcc/one", QoS: 1, Payload: []byte("after")})

	sub.expectPayloads("during-1", "during-2", "during-3", "after")
}
