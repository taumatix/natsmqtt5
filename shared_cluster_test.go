package natsmqtt5_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// The last leaver of a shared group and a joiner on another broker, over a replicated
// stream and bucket: the joiner's SUBACK is answered, a job it has not yet been sent is
// waiting in the group's backlog, and only then does the leaver, which had found the group
// empty before the joiner wrote its entry, go to delete the consumer. The group's lock makes
// the leaver find the joiner's entry, and the job is delivered. Before, the delete took the
// backlog with it [MQTT-5.0 §4.8.2: the group has a Session subscribed].
func TestClusterALastLeaverDoesNotDeleteTheBacklogOfAJoinerOnAnotherBroker_MQTT_5_0_4_8_2(t *testing.T) {
	c := startNATSCluster(t)
	_, addrA, _ := startBrokerHandle(t, c.URL(0), sweeping, clusterReplicas)
	_, addrB, _ := startBrokerHandle(t, c.URL(1), sweeping, clusterReplicas)

	leaver := dialRaw(t, addrA)
	leaver.connect(rawConnect("leaver", 300))
	leaver.subscribe(group, packet.QoS1)
	requireConsumers(t, c.URL(0), 1)

	reached, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	natsmqtt5.SetBeforeGroupDelete(t, func() {
		once.Do(func() { close(reached) })
		<-release
	})
	left := make(chan struct{})
	go func() {
		defer close(left)
		leaver.unsubscribe(group)
	}()
	select {
	case <-reached:
	case <-time.After(30 * time.Second):
		t.Fatal("the leaver never found the group empty")
	}

	joiner := dialRaw(t, addrB)
	joiner.connect(oneAtATime("joiner", 300))
	joiner.subscribe(group, packet.QoS1)
	publishJobs(t, addrB, 2)
	first := joiner.expectPublish() // the joiner's one slot is taken: job-1 waits in the backlog

	close(release)
	<-left
	requireConsumers(t, c.URL(0), 1, "the joiner's group was kept")
	joiner.send(&packet.Puback{Ack: packet.Ack{PacketID: first.PacketID}})
	got := joiner.expectPublish()
	assert.Equal(t, "job-1", string(got.Payload), "the job waiting in the backlog survived the leaver's delete")
	joiner.send(&packet.Puback{Ack: packet.Ack{PacketID: got.PacketID}})
}

// A broker of an earlier release updates the group's consumer and its metadata, with the
// consumer's start, goes; a broker of this release joining the group, on another server of
// the cluster, puts the start back from the copy kept beside the consumer (the late backlog
// uses it to skip what the group has acknowledged [MQTT-4.3.3-2]).
func TestClusterAJoiningBrokerRestoresTheConsumerStartAnEarlierReleaseDropped_MQTT_4_3_3_2(t *testing.T) {
	c := startNATSCluster(t)
	_, addrA, _ := startBrokerHandle(t, c.URL(0), sweeping, clusterReplicas, sharedQoS2)
	_, addrB, _ := startBrokerHandle(t, c.URL(2), sweeping, clusterReplicas, sharedQoS2)

	first := dialRaw(t, addrA)
	first.connect(rawConnect("first", 300))
	first.subscribe(group, packet.QoS2)
	requireConsumers(t, c.URL(0), 1)
	dropConsumerMetadata(t, c.URL(1))

	second := dialRaw(t, addrB)
	second.connect(rawConnect("second", 300))
	second.subscribe(group, packet.QoS2)

	nc, err := nats.Connect(c.URL(1))
	require.NoError(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cons, err := js.Consumer(ctx, "MQTT5_queue", sharedConsumers(t, c.URL(1))[0])
	require.NoError(t, err)
	assert.NotEmpty(t, cons.CachedInfo().Config.Metadata["natsmqtt5-start-seq"],
		"the joining broker put the consumer's start back")
}
