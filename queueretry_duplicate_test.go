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

	"github.com/taumatix/natsmqtt5/v2"
	"github.com/taumatix/natsmqtt5/v2/packet"
)

// A shared subscription on a broker whose default queue came up late is given the group's backlog
// consumer, which other members, on a broker whose queue was up, had already made. Messages stored
// after that consumer was made are in its backlog; their live copies, reaching this broker after the
// backlog was given, must be skipped, or the group gets them twice: once live and once pulled.
//
//	[MQTT-4.3.3-2] the QoS 2 sender "MUST send a PUBLISH packet containing this Packet Identifier
//	  with QoS 2 and DUP flag set to 0" once per new Application Message: a member must not be sent
//	  the same QoS 2 message as a new one twice.
//
// The publishers are held between storing the queue copy and publishing the live one
// (SetPublishQueuedGate) while broker B's queue comes up (SetQueueRetryGate), which is the window:
// the live copies arrive after B read the queue's last sequence. Each live copy goes through the
// NATS queue group to one of the two members, so before the fix each of the ten was sent twice with
// probability one half.
func TestALateBacklogSkipsTheLiveCopyOfAMessageTheGroupsConsumerAlreadyHeld_MQTT_4_3_3_2(t *testing.T) {
	lateBacklogOnce(t, true)
}

// The other side of the bound: with no other member, the late broker makes the group's consumer
// itself, after the held messages were stored, so the backlog does not hold them and their live
// copies are the only ones. They must still be delivered, once each: the subscription "MUST add it
// to the Session State" of a matching subscription [MQTT-4.5.0-1], and [MQTT-4.3.3-2] as above.
func TestALateBacklogStillDeliversTheLiveCopyOfAMessageStoredBeforeItsConsumer_MQTT_4_5_0_1_MQTT_4_3_3_2(t *testing.T) {
	lateBacklogOnce(t, false)
}

// lateBacklogOnce runs the scenario above, with or without a member on the broker whose queue was
// up (which makes the group's consumer first), and asserts that the late member gets each held
// message exactly once.
func lateBacklogOnce(t *testing.T, earlyMember bool) {
	natsmqtt5.SetQueueRetryEvery(t, 50*time.Millisecond, 100*time.Millisecond)
	natsURL := startNATS(t)
	squatQueueSubjects(t, natsURL)
	var logsA syncBuffer
	bB, addrB, _ := startBrokerHandle(t, natsURL, sweeping, sharedQoS2)
	var retryOpen atomic.Bool
	natsmqtt5.SetQueueRetryGate(bB, retryOpen.Load)
	require.False(t, bB.OfflineQueueActive(), "the squatter holds the queue's subjects")
	deleteSquatter(t, natsURL)

	member := dialRaw(t, addrB)
	member.connect(rawConnect("late-member", 300))
	member.subscribe(group, packet.QoS2)

	bA, addrA, _ := startBrokerHandle(t, natsURL, sweeping, sharedQoS2, loggingBroker(t, &logsA))
	require.True(t, bA.OfflineQueueActive())
	members := 1
	if earlyMember {
		members = 2
		away := dialRaw(t, addrA)
		away.connect(rawConnect("early-member", 300))
		away.subscribe(group, packet.QoS2)
		away.drop()
		waitDetached(t, &logsA, "early-member")
	}

	const n = 10
	release := make(chan struct{})
	var held sync.WaitGroup
	held.Add(n)
	natsmqtt5.SetPublishQueuedGate(bA, func([]byte) { held.Done(); <-release })
	var published sync.WaitGroup
	for i := range n {
		pub, _ := connectClient(t, addrA, connectOpts(fmt.Sprintf("late-pub-%d", i)))
		published.Add(1)
		go func() {
			defer published.Done()
			pub.publish(&paho.Publish{Topic: "jobs/x", QoS: 2, Payload: []byte(fmt.Sprintf("job-%d", i))})
		}()
	}
	held.Wait()

	retryOpen.Store(true)
	// The member entry is written before the backlog is given (joinGroup), so it is the backlog
	// itself that is waited for: a live copy released in between reaches a subscription with none.
	require.Eventually(t, func() bool {
		return memberEntries(t, natsURL) == members && natsmqtt5.HasSharedBacklog(bB, "late-member")
	}, 10*time.Second, 20*time.Millisecond, "the late broker's member was not given the backlog")
	close(release)
	published.Wait()

	counts := map[string]int{}
	for _, p := range member.completeFor(2 * time.Second) {
		counts[p]++
	}
	for i := range n {
		assert.Equal(t, 1, counts[fmt.Sprintf("job-%d", i)],
			"[MQTT-4.3.3-2] job-%d reaches the group once: %v", i, counts)
	}
}
