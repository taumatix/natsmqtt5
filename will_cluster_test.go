package natsmqtt5_test

import (
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
)

// clusterWills is fastWills with every bucket and stream replicated three ways, as a
// deployment on a three-server cluster would have it.
func clusterWills(o *natsmqtt5.Options) {
	fastWills(o)
	clusterReplicas(o)
	// Patient enough to ride out a Raft election: isolating a server that leads the Will
	// bucket leaves the survivors unable to write to it until they elect another leader, which
	// NATS starts 4 to 9 s after the last heartbeat (measured: writes back within 9 s), and a
	// broker that cannot write for twice this interval closes its clients, as designed. The
	// 150 ms of fastWills would fence the survivors too, and 4 s did now and then.
	o.WillCheckInterval = 6 * time.Second
}

// clusterReplicas replicates every bucket and stream three ways and, with
// NATSMQTT5_CLUSTER_LOG set, prints the broker's log.
func clusterReplicas(o *natsmqtt5.Options) {
	o.SessionReplicas = 3
	o.RetainedReplicas = 3
	o.OfflineQueueReplicas = 3
	if os.Getenv("NATSMQTT5_CLUSTER_LOG") != "" { // the brokers' own log, for a failing run
		o.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
}

// The partition test in nats_partition_test.go cuts the broker's one link with a proxy. A
// real cluster has a harder case: the NATS server the broker uses is alive, accepts the
// broker's connection and answers on its own subjects, and is cut off from the other two
// servers. Only the Will bucket, which lives on the majority, tells the broker anything.
// [MQTT-3.1.2-8]: the Will must not be published while the client's connection is open, and
// [MQTT-3.1.3-9]-style duplication must not follow once the partition heals.
//
// Whether the isolated server leads the Will bucket decides what the survivors go through: if it does they
// must elect a leader before they can read the bucket at all, if it does not they read it throughout. The
// cluster picks the leader, so each shape is made, not left to chance.
func TestClusterWillOfAClientOnAnIsolatedNATSServerIsPublishedOnceAfterItsConnectionCloses(t *testing.T) {
	for _, shape := range []struct {
		name   string
		leader int
	}{{"the isolated server leads the Will bucket", 0}, {"a survivor leads the Will bucket", 1}} {
		t.Run(shape.name, func(t *testing.T) { isolatedServerWill(t, shape.leader) })
	}
}

func isolatedServerWill(t *testing.T, leader int) {
	c := startNATSCluster(t)

	cut, cutAddr, _ := startBrokerHandle(t, c.URL(0), clusterWills)
	survivor := startBroker(t, c.URL(1)+","+c.URL(2), clusterWills)
	watcher := newWillWatcher(t, survivor)
	c.LeadStream("KV_MQTT5_wills", leader)

	client := dialRaw(t, cutAddr)
	client.connect(rawConnectWithWillMessage("isolated", "status/isolated", "gone"))
	closed := client.closedAt()
	require.Equal(t, 1, willKeys(t, c.URL(1)))

	c.Isolate(0)
	t.Cleanup(func() { c.Heal(0) })

	var got received
	select {
	case got = <-watcher.messages:
	case <-time.After(90 * time.Second):
		t.Fatal("no Will after the broker's NATS server was isolated from its cluster")
	}
	willAt := time.Now()
	assert.Equal(t, "gone", got.Payload)

	select {
	case closedAt := <-closed:
		assert.False(t, closedAt.After(willAt),
			"[MQTT-3.1.2-8] the Will was published while the client's connection was still open")
	case <-time.After(time.Second):
		t.Fatal("[MQTT-3.1.2-8] the Will was published and the client's connection is still open")
	}

	// The partition heals; the fenced broker's own disconnect path must find the record gone
	// (or taken) and publish nothing.
	c.Heal(0)
	watcher.expectNoMessageFor(15 * time.Second)
	assert.Equal(t, 0, willKeys(t, c.URL(1)), "the Will record outlived its publication")
	natsmqtt5.Kill(cut)
}
