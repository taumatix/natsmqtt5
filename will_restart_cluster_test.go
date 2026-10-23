package natsmqtt5_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
)

// A rolling restart of a replicated NATS cluster is the routine operation a deployment
// performs most. Each server goes down and comes back while the brokers stay up and fail
// over to the others. With the default WillCheckInterval no broker may close a client for it
// and no Will may be published: the client's connection never ended [MQTT-3.1.2-8].
func TestClusterARollingRestartOfNATSNeitherClosesAClientNorPublishesItsWill(t *testing.T) {
	c := startNATSCluster(t)
	defaults := func(o *natsmqtt5.Options) {
		clusterReplicas(o)
		o.DurableWills = true // WillCheckInterval stays at its default
	}
	_, hostAddr, _ := startBrokerHandle(t, c.URLs(), defaults)
	watchAddr := startBroker(t, c.URLs(), defaults)
	watcher := newWillWatcher(t, watchAddr)

	client, _ := connectClient(t, hostAddr, durableWillConnect("rolling", "status/rolling", "gone", 0, 0))
	require.Equal(t, 1, willKeys(t, c.URL(0)))

	for i := range 3 {
		c.StopServer(i, true)
		time.Sleep(3 * time.Second)
		c.StartServer(i)
		// The next server must not go down before this one has rejoined its streams,
		// as a rolling restart waits for health between steps.
		time.Sleep(12 * time.Second)
	}

	watcher.expectNoMessageFor(2 * time.Second)
	select {
	case d := <-client.disconnects:
		t.Fatalf("a rolling restart of NATS made the broker disconnect the client (reason 0x%02X)", d.ReasonCode)
	case <-client.Done():
		t.Fatal("a rolling restart of NATS closed the client's connection")
	default:
	}
	assert.Equal(t, 1, willKeys(t, c.URL(0)), "the Will record must survive the restart")
}

// A broker pointed at one NATS server loses its only link when that server restarts. A
// restart that ends inside twice WillCheckInterval costs it nothing: the client stays
// connected, nobody adopts its Will, and the record is still there.
func TestClusterARestartOfTheBrokersOnlyNATSServerWithinTwiceTheCheckIntervalClosesNothing(t *testing.T) {
	c := startNATSCluster(t)
	defaults := func(o *natsmqtt5.Options) {
		clusterReplicas(o)
		o.DurableWills = true
	}
	_, hostAddr, _ := startBrokerHandle(t, c.URL(0), defaults)
	watchAddr := startBroker(t, c.URL(1)+","+c.URL(2), defaults)
	watcher := newWillWatcher(t, watchAddr)
	client, _ := connectClient(t, hostAddr, durableWillConnect("lone", "status/lone", "gone", 0, 0))
	require.Equal(t, 1, willKeys(t, c.URL(1)))

	c.StopServer(0, false)
	time.Sleep(2 * time.Second)
	c.StartServer(0)

	watcher.expectNoMessageFor(20 * time.Second) // past the survivors' adoption wait
	select {
	case d := <-client.disconnects:
		t.Fatalf("the broker disconnected its client for a 2-second NATS restart (reason 0x%02X)", d.ReasonCode)
	case <-client.Done():
		t.Fatal("the client's connection closed for a 2-second NATS restart")
	default:
	}
	assert.Equal(t, 1, willKeys(t, c.URL(1)))
}
