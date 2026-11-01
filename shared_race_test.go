package natsmqtt5_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
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
	joiner.connect(oneAtATime("joiner", 300))
	joiner.subscribe(group, packet.QoS1)
	require.Equal(t, 1, memberEntries(t, natsURL), "the joiner is a member")

	// Published after the joiner's SUBACK and before the leaver deletes: it is in the
	// consumer the leaver is about to delete. It used to go with it.
	publishJobs(t, addr, 2)
	first := joiner.expectPublish() // the one slot is taken: the other waits in the backlog

	close(release)
	<-left
	// The leaver, finding the joiner's entry under the group's lock, kept the consumer.
	requireConsumers(t, natsURL, 1, "the joiner's group was kept")
	joiner.send(&packet.Puback{Ack: packet.Ack{PacketID: first.PacketID}})
	got := joiner.expectPublish()
	// Which job comes first is not fixed: the leaver's own pull was still open on the
	// consumer when the jobs were published, took job-0 and handed it back (a NAK),
	// and JetStream redelivers a NAKed message after the ones behind it. What the
	// joiner is owed is both jobs, each once.
	assert.ElementsMatch(t, []string{"job-0", "job-1"}, []string{string(first.Payload), string(got.Payload)},
		"both messages published after the joiner's SUBACK are its own, whatever the last leaver does next")
	joiner.send(&packet.Puback{Ack: packet.Ack{PacketID: got.PacketID}})
}

// The joiner arrives while the leaver holds the group's lock, past its check: the
// consumer is going to be deleted. The joiner's SUBACK waits for that, and the
// consumer it then makes begins before the SUBACK, so what is published after the SUBACK
// is delivered. Before the lock the SUBACK came at once, and the consumer deleted
// after it was the joiner's.
func TestAJoinerWhoArrivesWhileTheLastLeaverDeletesWaitsAndThenHasAGroup_MQTT_5_0_4_8_2(t *testing.T) {
	natsURL := startNATS(t)
	_, addr, _ := startBrokerHandle(t, natsURL, sweeping)

	leaver := dialRaw(t, addr)
	leaver.connect(rawConnect("leaver", 300))
	leaver.subscribe(group, packet.QoS1)

	reached, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	natsmqtt5.SetAfterGroupLock(t, func() {
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
	subscribed := make(chan struct{})
	go func() {
		defer close(subscribed)
		joiner.subscribe(group, packet.QoS1)
	}()
	select {
	case <-subscribed:
		t.Fatal("the SUBACK came while the group's consumer was being deleted")
	case <-time.After(500 * time.Millisecond):
	}

	close(release)
	<-left
	select {
	case <-subscribed:
	case <-time.After(10 * time.Second):
		t.Fatal("the joiner never got its SUBACK")
	}
	requireConsumers(t, natsURL, 1, "the joiner's group was made after the deletion")
	publishJobs(t, addr, 1)
	got := joiner.expectPublish()
	assert.Equal(t, "job-0", string(got.Payload))
	joiner.send(&packet.Puback{Ack: packet.Ack{PacketID: got.PacketID}})
}

// A broker killed holding a group's lock does not hold the group for ever: the next joiner
// takes the lock over once it is older than shareLockStale.
func TestAGroupLockLeftByAKilledBrokerIsTakenOver(t *testing.T) {
	natsmqtt5.SetShareLockStale(t, 300*time.Millisecond)
	natsURL := startNATS(t)
	_, addr, _ := startBrokerHandle(t, natsURL, sweeping)

	first := dialRaw(t, addr)
	first.connect(rawConnect("first", 300))
	first.subscribe(group, packet.QoS1)
	plantGroupLock(t, natsURL, time.Now())

	started := time.Now()
	second := dialRaw(t, addr)
	second.connect(rawConnect("second", 300))
	second.subscribe(group, packet.QoS1)
	assert.Less(t, time.Since(started), 5*time.Second, "the dead broker's lock was waited out")
	assert.GreaterOrEqual(t, time.Since(started), 250*time.Millisecond, "a fresh lock was taken from its holder")
	require.Equal(t, 2, memberEntries(t, natsURL))
}

// plantGroupLock writes the lock a broker killed inside a group's deletion leaves behind.
func plantGroupLock(t *testing.T, natsURL string, at time.Time) {
	t.Helper()
	names := sharedConsumers(t, natsURL)
	require.Len(t, names, 1)
	sum := sha256.Sum256([]byte(names[0]))
	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	kv, err := js.KeyValue(ctx, "MQTT5_share_members")
	require.NoError(t, err)
	_, err = kv.Create(ctx, "lock."+hex.EncodeToString(sum[:12]), []byte(strconv.FormatInt(at.UnixNano(), 10)))
	require.NoError(t, err)
}
