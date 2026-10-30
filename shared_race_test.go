package natsmqtt5_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// The last member of a group leaves, finds it empty, and a new member joins
// before the consumer is deleted. The joiner's puller finds the consumer gone and
// makes it again, so the group works for it: a message published afterwards is
// delivered, and the group is still its own.
//
// MQTT-5.0 §4.8.2 (quoted from the specification, the sentence carries no
// statement id): "A Shared Subscription ends, and any undelivered messages
// associated with it are deleted, when there are no longer any Sessions
// subscribed to it". The joiner is subscribed, so the group must not stay ended.
func TestAJoinerWhoArrivesAsTheLastMemberLeavesHasAGroup_MQTT_5_0_4_8_2(t *testing.T) {
	natsURL := startNATS(t)
	_, addr, _ := startBrokerHandle(t, natsURL, sweeping)

	leaver := dialRaw(t, addr)
	leaver.connect(rawConnect("leaver", 300))
	leaver.subscribe(group, packet.QoS1)
	requireConsumers(t, natsURL, 1)

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
	case <-time.After(5 * time.Second):
		t.Fatal("the leaver never found the group empty")
	}

	joiner := dialRaw(t, addr)
	joiner.connect(rawConnect("joiner", 300))
	joiner.subscribe(group, packet.QoS1)
	require.Equal(t, 1, memberEntries(t, natsURL), "the joiner is a member")

	close(release)
	<-left
	// The consumer is deleted now; the joiner's puller makes it again at its
	// first failed pull, and a job published after that reaches it.
	requireConsumers(t, natsURL, 1, "the joiner's group was made again")
	publishJobs(t, addr, 1)
	got := joiner.expectPublish()
	assert.Equal(t, "job-0", string(got.Payload))
	joiner.send(&packet.Puback{Ack: packet.Ack{PacketID: got.PacketID}})
}
