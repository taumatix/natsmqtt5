package natsmqtt5_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// A session killed with its broker leaves its shared subscription's membership
// entry behind. With persistent sessions, once the session store has reclaimed
// its record (nothing can resume it) a surviving broker removes the entry too, and
// ends the group if that was the last, instead of waiting for the entry to lapse
// (MaxSessionExpiry plus a minute, here an hour).
//
// MQTT-5.0 §4.8.2 (quoted from the specification, the sentence carries no
// statement id): "A Shared Subscription ends, and any undelivered messages
// associated with it are deleted, when there are no longer any Sessions
// subscribed to it".
func TestASurvivorSweepsTheMembersOfSessionsWhoseRecordIsGone_MQTT_5_0_4_8_2(t *testing.T) {
	natsmqtt5.SetShareMemberSlack(t, time.Hour)
	natsmqtt5.SetShareMemberSweepGrace(t, 100*time.Millisecond)
	natsURL := startNATS(t)
	short := func(o *natsmqtt5.Options) {
		o.MaxSessionExpiry = time.Second
		o.PersistentSessions = true
		o.OfflineQueue = true
	}
	a, addrA, stopA := startBrokerHandle(t, natsURL, short)
	b, addrB, _ := startBrokerHandle(t, natsURL, short) // the survivor

	m1 := dialRaw(t, addrA)
	m1.connect(rawConnect("m1", 1))
	m1.subscribe(group, packet.QoS1)
	m2 := dialRaw(t, addrB)
	m2.connect(rawConnect("m2", 1))
	m2.subscribe(group, packet.QoS1)
	natsmqtt5.Kill(a)
	stopA()

	m2.unsubscribe(group)
	requireConsumers(t, natsURL, 1, "the dead broker's member is still counted: its session may be resumed")
	require.Equal(t, 1, memberEntries(t, natsURL))

	// Its record is reclaimed once it is older than MaxSessionExpiry and its
	// broker does not answer; then the entry goes too, and the group with it.
	time.Sleep(1500 * time.Millisecond)
	ctx := context.Background()
	require.NoError(t, natsmqtt5.SweepStore(ctx, b))
	require.NoError(t, natsmqtt5.SweepShareMembers(ctx, b))
	require.Equal(t, 0, memberEntries(t, natsURL), "the entry of a session with no record is swept")
	requireConsumers(t, natsURL, 0, "the group ended with its last (dead) member")
}

// A session that has a record keeps its entry, however old the entry is: the
// sweep only removes what no session can resume.
func TestTheMemberSweepKeepsTheEntryOfASessionWithARecord(t *testing.T) {
	natsmqtt5.SetShareMemberSweepGrace(t, 0)
	natsURL := startNATS(t)
	b, addr, _ := startBrokerHandle(t, natsURL, persistentWithQueue)

	m1 := dialRaw(t, addr)
	m1.connect(rawConnect("m1", 300))
	m1.subscribe(group, packet.QoS1)
	m1.send(&packet.Disconnect{})
	time.Sleep(200 * time.Millisecond)

	require.NoError(t, natsmqtt5.SweepShareMembers(context.Background(), b))
	require.Equal(t, 1, memberEntries(t, natsURL))
	requireConsumers(t, natsURL, 1)
}
