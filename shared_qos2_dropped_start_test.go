package natsmqtt5_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5/v2"
	"github.com/taumatix/natsmqtt5/v2/packet"
)

// The same window as TestALateBacklogSkipsTheLiveCopyOfAMessageTheGroupAlreadyAcknowledged, with a
// broker of an earlier release in the fleet: it updates the group's consumer, which replaces the
// metadata the start is kept in. The start is also kept beside the consumer (in the members bucket),
// and the broker that joins next puts it back, so the late backlog still skips what the group has
// acknowledged. Before, the metadata stayed dropped and about half of the ten messages came twice.
func TestALateBacklogStillSkipsAnAcknowledgedMessageAfterAnEarlierReleaseDroppedTheConsumersStart_MQTT_4_3_3_2_MQTT_4_8_2_5(t *testing.T) {
	natsmqtt5.SetQueueRetryEvery(t, 50*time.Millisecond, 100*time.Millisecond)
	natsURL := startNATS(t)
	squatQueueSubjects(t, natsURL)
	bB, addrB, _ := startBrokerHandle(t, natsURL, sweeping, sharedQoS2)
	var retryOpen atomic.Bool
	natsmqtt5.SetQueueRetryGate(bB, retryOpen.Load)
	require.False(t, bB.OfflineQueueActive(), "the squatter holds the queue's subjects")
	deleteSquatter(t, natsURL)

	late := dialRaw(t, addrB)
	late.connect(rawConnect("late-member", 300))
	late.subscribe(group, packet.QoS2)

	bA, addrA, _ := startBrokerHandle(t, natsURL, sweeping, sharedQoS2)
	require.True(t, bA.OfflineQueueActive())
	early := dialRaw(t, addrA)
	early.connect(rawConnect("early-member", 300))
	require.Equal(t, []packet.ReasonCode{packet.GrantedQoS2}, early.subscribeCodes(group, packet.QoS2))
	dropConsumerMetadata(t, natsURL)

	const n = 10
	release := make(chan struct{})
	var held sync.WaitGroup
	held.Add(n)
	natsmqtt5.SetPublishQueuedGate(bA, func([]byte) { held.Done(); <-release })
	var published sync.WaitGroup
	for i := range n {
		pub, _ := connectClient(t, addrA, connectOpts(fmt.Sprintf("ack-pub-%d", i)))
		published.Add(1)
		go func() {
			defer published.Done()
			pub.publish(&paho.Publish{Topic: "jobs/x", QoS: 2, Payload: []byte(fmt.Sprintf("job-%d", i))})
		}()
	}
	held.Wait()

	got := map[string]int{}
	for _, p := range early.completeFor(2 * time.Second) {
		got[p]++
	}
	require.Len(t, got, n, "the early member takes every message from the backlog: %v", got)

	retryOpen.Store(true)
	require.Eventually(t, func() bool {
		return memberEntries(t, natsURL) == 2 && natsmqtt5.HasSharedBacklog(bB, "late-member")
	}, 10*time.Second, 20*time.Millisecond, "the late broker's member was not given the backlog")
	close(release)
	published.Wait()

	assert.Empty(t, late.completeFor(2*time.Second),
		"[MQTT-4.3.3-2] a message the group acknowledged is not delivered again from its live copy")
	assert.Empty(t, early.completeFor(time.Second), "nor is it sent to the member that took it a second time")
}

// dropConsumerMetadata does to the group's consumer what a broker of an earlier release does when it
// joins the group: updates it with its configuration, which carries no metadata.
func dropConsumerMetadata(t *testing.T, natsURL string) {
	t.Helper()
	names := sharedConsumers(t, natsURL)
	require.Len(t, names, 1)
	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cons, err := js.Consumer(ctx, "MQTT5_queue", names[0])
	require.NoError(t, err)
	cfg := cons.CachedInfo().Config
	require.NotEmpty(t, cfg.Metadata["natsmqtt5-start-seq"], "the consumer was made with its start")
	cfg.Metadata = nil
	_, err = js.UpdateConsumer(ctx, "MQTT5_queue", cfg)
	require.NoError(t, err)
	cons, err = js.Consumer(ctx, "MQTT5_queue", names[0])
	require.NoError(t, err)
	require.Empty(t, cons.CachedInfo().Config.Metadata["natsmqtt5-start-seq"], "the update dropped the start")
}
