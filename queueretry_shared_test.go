package natsmqtt5_test

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// lateQueueBroker starts a broker whose default queue cannot be created (a stream holds its
// subjects), then frees the subjects and waits for the broker's retry to bring the queue up.
// What depends on the queue (the hold-back loop and the membership refresh, startQueueLoops) is
// started by that retry and not by Open, so these tests are the only ones that run it that way.
func lateQueueBroker(t *testing.T, natsURL string, customise ...func(*natsmqtt5.Options)) (b *natsmqtt5.Broker, addr string, release func()) {
	t.Helper()
	natsmqtt5.SetQueueRetryEvery(t, 50*time.Millisecond, 100*time.Millisecond)
	squatQueueSubjects(t, natsURL)
	b, addr, _ = startBrokerHandle(t, natsURL, append([]func(*natsmqtt5.Options){sweeping}, customise...)...)
	require.False(t, b.OfflineQueueActive(), "the squatter holds the queue's subjects")
	release = func() {
		nc, err := nats.Connect(natsURL)
		require.NoError(t, err)
		defer nc.Close()
		js, err := jetstream.New(nc)
		require.NoError(t, err)
		require.NoError(t, js.DeleteStream(context.Background(), "SQUATTER"))
		require.Eventually(t, b.OfflineQueueActive, 10*time.Second, 20*time.Millisecond,
			"the broker never retried the queue")
	}
	return b, addr, release
}

// A member that is holding a message and does not acknowledge it is made to give it up after
// SharedHoldLimit: the hold-back loop that does that is started by the queue, so a broker whose
// queue came up late must start it then, or the limit is silently not enforced for the rest of the
// broker's life. Same behaviour as TestAMessageHeldPastTheLimitGoesToAnotherMember on a broker whose
// queue came up at start-up. §4.8.2: the Server SHOULD send the Application Message to another
// Client of the Shared Subscription.
func TestAMemberHoldingASharedMessageGivesItUpOnABrokerWhoseQueueCameUpLate(t *testing.T) {
	natsmqtt5.SetSharedAckWait(t, handBackAckWait)
	natsURL := startNATS(t)
	_, addr, release := lateQueueBroker(t, natsURL, func(o *natsmqtt5.Options) { o.SharedHoldLimit = holdLimit })
	release()

	m1 := dialRaw(t, addr)
	m1.connect(oneAtATime("late-m1", 0))
	m1.subscribe(group, packet.QoS1)
	publishJobs(t, addr, 1)
	first := m1.expectPublish()
	require.Equal(t, "job-0", string(first.Payload))
	m2 := member(t, addr, "late-m2", 0)

	got := m2.expectPublish()
	assert.Equal(t, "job-0", string(got.Payload), "the held message is handed back once the limit passes")
	m2.send(&packet.Puback{Ack: packet.Ack{PacketID: got.PacketID}})
}

// The refresh loop that keeps a shared member's entry alive is started by the queue too. An entry lasts
// MaxSessionExpiry plus a slack and is rewritten every third of that, so one that is still there after
// several lifetimes proves the loop runs on a broker whose queue came up late (§4.8.2: the entry is what
// keeps the group, and its backlog, alive while the member is away).
//
// A shared subscription made while the queue was off is a plain queue-group subscription with no
// backlog, and so no entry, and the refresh does not add one: it is not a member until its session
// subscribes again. The test pins that (ROADMAP.md records it).
func TestASharedMemberOfABrokerWhoseQueueCameUpLateIsKeptAliveByTheRefresh(t *testing.T) {
	natsmqtt5.SetShareMemberSlack(t, time.Second)
	natsURL := startNATS(t)
	_, addr, release := lateQueueBroker(t, natsURL, func(o *natsmqtt5.Options) { o.MaxSessionExpiry = time.Second })

	early := dialRaw(t, addr)
	early.connect(rawConnect("early-m1", 1))
	early.subscribe(group, packet.QoS1)
	release()
	require.Zero(t, memberEntries(t, natsURL), "the subscription of before the queue is not a member")

	late := dialRaw(t, addr)
	late.connect(rawConnect("late-m2", 1))
	late.subscribe(group, packet.QoS1)
	require.Equal(t, 1, memberEntries(t, natsURL), "a subscription made once the queue is up is recorded")

	time.Sleep(5 * time.Second) // more than two lifetimes of an entry
	assert.Equal(t, 1, memberEntries(t, natsURL),
		"the entry lapsed: the refresh loop did not start with the late queue, or the early member was added")
}
