package natsmqtt5_test

import (
	"context"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
)

// A broker keeps its retained set from a consumer of the retained stream. A broker whose
// NATS server is cut off from the cluster misses a clear, and if the tombstone that records
// the clear is purged before the broker is back, the consumer's resume never delivers it.
// [MQTT-3.3.1-6]: a zero-byte retained PUBLISH removes the retained message, for every
// subscriber that subscribes afterwards, on every broker.
func TestClusterARetainedClearMissedDuringAnOutageIsAppliedAfterTheTombstoneIsPurged(t *testing.T) {
	c := startNATSCluster(t)
	quick := func(o *natsmqtt5.Options) {
		clusterReplicas(o)
		o.SessionSweepInterval = 500 * time.Millisecond
		o.RetainedTombstoneTTL = 2 * time.Second
	}
	_, addrCut, _ := startBrokerHandle(t, c.URL(0), quick)
	addrOther := startBroker(t, c.URL(1)+","+c.URL(2), quick)

	pub, _ := connectClient(t, addrOther, connectOpts("tomb-cluster-pub"))
	pub.publish(&paho.Publish{Topic: "tomb/cluster", QoS: 1, Retain: true, Payload: []byte("old")})
	pub.publish(&paho.Publish{Topic: "tomb/kept", QoS: 1, Retain: true, Payload: []byte("kept")})

	// The cut broker has the retained message once it arrives through the stream.
	require.Eventually(t, func() bool { return retainedCount(t, addrCut, "tomb/cluster") == 1 },
		30*time.Second, 200*time.Millisecond)

	c.Isolate(0)
	t.Cleanup(func() { c.Heal(0) })
	// The stream may have to elect a new leader first, so the clear is retried.
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := pub.Client.Publish(ctx, &paho.Publish{Topic: "tomb/cluster", QoS: 1, Retain: true})
		return err == nil
	}, 60*time.Second, time.Second, "the clear never succeeded")

	// Past the tombstone's time to live the surviving broker has purged it: only the kept
	// message is left in the stream, and the cut-off broker, which cannot hear, still has both.
	require.Eventually(t, func() bool { return retainedStreamMsgs(t, c.URL(1)) == 1 },
		30*time.Second, 500*time.Millisecond, "the tombstone was not purged")
	require.Equal(t, 1, retainedCount(t, addrCut, "tomb/cluster"), "the isolation did not take effect")

	c.Heal(0)
	assert.Eventually(t, func() bool { return retainedCount(t, addrCut, "tomb/cluster") == 0 },
		60*time.Second, time.Second,
		"[MQTT-3.3.1-6] the cut-off broker still serves a retained message that was cleared during its outage")
	assert.Equal(t, 1, retainedCount(t, addrCut, "tomb/kept"), "a live retained message must survive the resync")
}

// retainedCount subscribes a fresh client to topic on addr and counts the retained
// messages it is sent.
func retainedCount(t *testing.T, addr, topic string) int {
	t.Helper()
	cl, _ := connectClient(t, addr, connectOpts("rc-"+time.Now().Format("150405.000000")))
	defer func() { _ = cl.Client.Disconnect(&paho.Disconnect{ReasonCode: 0}) }()
	cl.subscribe(paho.SubscribeOptions{Topic: topic, QoS: 1})
	n := 0
	for {
		select {
		case <-cl.messages:
			n++
		case <-time.After(500 * time.Millisecond):
			return n
		}
	}
}
