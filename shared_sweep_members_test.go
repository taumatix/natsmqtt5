package natsmqtt5_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5/v2"
	"github.com/taumatix/natsmqtt5/v2/packet"
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

// A released session whose record no longer lists the group (a restore that
// failed and kept the record, or an UNSUBSCRIBE whose leave failed) is not a
// member of it: its entry goes, and the group ends if it was the last.
//
// MQTT-5.0 §4.8.2 (quoted from the specification, the sentence carries no
// statement id): "A Shared Subscription ends, and any undelivered messages
// associated with it are deleted, when there are no longer any Sessions
// subscribed to it". The record is rewritten here through the store's bucket,
// as a failed restore would leave it, and the sweep is the real one.
func TestTheMemberSweepRemovesTheEntryOfAReleasedSessionWhoseRecordNoLongerListsTheGroup_MQTT_5_0_4_8_2(t *testing.T) {
	natsmqtt5.SetShareMemberSweepGrace(t, 0)
	natsURL := startNATS(t)
	b, addr, _ := startBrokerHandle(t, natsURL, persistentWithQueue)

	gone := dialRaw(t, addr)
	gone.connect(rawConnect("gone", 300))
	gone.subscribe(group, packet.QoS1)
	gone.send(&packet.Disconnect{})
	stay := dialRaw(t, addr)
	stay.connect(rawConnect("stay", 300))
	stay.subscribe(group, packet.QoS1)
	time.Sleep(200 * time.Millisecond)
	require.Equal(t, 2, memberEntries(t, natsURL))

	ctx := context.Background()
	require.NoError(t, natsmqtt5.SweepShareMembers(ctx, b))
	require.Equal(t, 2, memberEntries(t, natsURL), "a record that lists the group keeps the entry")

	forgetSubscriptions(t, natsURL, "gone")
	require.NoError(t, natsmqtt5.SweepShareMembers(ctx, b))
	require.Equal(t, 1, memberEntries(t, natsURL), "the entry of the session that no longer lists the group is swept")
	requireConsumers(t, natsURL, 1, "another session is still a member")

	stay.unsubscribe(group)
	requireConsumers(t, natsURL, 0, "the group ended with its last member")
}

// forgetSubscriptions rewrites a stored session's record without its
// subscriptions, leaving the rest of it as it was.
func forgetSubscriptions(t *testing.T, natsURL, clientID string) {
	t.Helper()
	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	kv, err := js.KeyValue(ctx, "MQTT5_sessions")
	require.NoError(t, err)
	key := base64.RawURLEncoding.EncodeToString([]byte(clientID))
	entry, err := kv.Get(ctx, key)
	require.NoError(t, err)
	var rec map[string]any
	require.NoError(t, json.Unmarshal(entry.Value(), &rec))
	require.NotEmpty(t, rec["Subscriptions"], "the record lists the group before it is rewritten")
	require.Equal(t, false, rec["Attached"], "the session was released")
	rec["Subscriptions"] = nil
	raw, err := json.Marshal(rec)
	require.NoError(t, err)
	_, err = kv.Update(ctx, key, raw, entry.Revision())
	require.NoError(t, err)
}
