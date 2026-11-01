package natsmqtt5_test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// The last window of "A default queue that failed at startup": a message the group had already
// acknowledged from its backlog, whose live copy reaches a broker only after that broker's queue came
// up and its member was given the backlog. The acknowledged message is at or below the consumer's
// acknowledgement floor, which the late broker used to take for "stored before the consumer" and so
// delivered the live copy, a second delivery of a QoS 2 message to the group [MQTT-4.3.3-2],
// [MQTT-4.8.2-5]. The consumer's start, recorded when it is made, tells the two apart.
//
// Broker A's early member takes all ten messages from the backlog and completes them to PUBREL (the
// ownership point: the group's consumer acknowledges at PUBREC). Only then does broker B's queue come
// up and its member join; the publishers' live copies are still held, and are released after. Each
// goes through the NATS queue group to one of the two members, so before the fix each of the ten
// reached the late member too with probability one half.
func TestALateBacklogSkipsTheLiveCopyOfAMessageTheGroupAlreadyAcknowledged_MQTT_4_3_3_2_MQTT_4_8_2_5(t *testing.T) {
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
