package natsmqtt5_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// A shared-subscription member that stays connected but never acknowledges
// keeps its message for as long as the connection lives, because the keepalive
// that stops JetStream redelivering it does not look at progress.
// SharedHoldLimit bounds that. Real nats-server, two brokers, raw MQTT clients.

const holdLimit = 700 * time.Millisecond

// holdingMember connects m1 (one message at a time), has it receive job-0 and
// never acknowledge it, then brings m2 into the group on the other broker.
func holdingMember(t *testing.T, limit time.Duration) (m1, m2 *rawClient, first *packet.Publish) {
	m1, m2, first, _ = holdingMemberWithExpiry(t, limit, 0)
	return m1, m2, first
}

// holdingMemberWithExpiry is holdingMember with m1's Session Expiry Interval,
// which a test that has m1 come back needs, and the fleet its brokers are in.
func holdingMemberWithExpiry(t *testing.T, limit time.Duration, expiry uint32) (m1, m2 *rawClient, first *packet.Publish, f sharedFleet) {
	t.Helper()
	f = newSharedFleet(t, func(o *natsmqtt5.Options) { o.SharedHoldLimit = limit })
	m1 = dialRaw(t, f.a)
	m1.connect(oneAtATime("m1", expiry))
	m1.subscribe(group, packet.QoS1)
	f.publish(t, "job-0")
	first = m1.expectPublish()
	require.Equal(t, "job-0", string(first.Payload))
	m2 = member(t, f.b, "m2", 0)
	return m1, m2, first, f
}

// A message handed back by the limit is the group's now, so the member's
// session no longer owes it: when the client reconnects it is not sent the
// message a second time, which with the other member's copy would be a
// duplicate no one asked for. What hands it over is §4.8.2's SHOULD that the
// Server send the message to another Client of the Shared Subscription
// ("If the Client's Session terminates before the Client reconnects, the Server
// SHOULD send the Application Message to another Client that is subscribed to
// the same Shared Subscription"; no statement id), which SharedHoldLimit
// extends to a member that holds a message too long. The late PUBACK of the old
// identifier is ignored and the identifier is not reused before it.
func TestAMessageHandedBackByTheLimitIsNotResentToItsFirstMember(t *testing.T) {
	m1, m2, first, f := holdingMemberWithExpiry(t, holdLimit, 300)
	got := m2.expectPublish()
	require.Equal(t, "job-0", string(got.Payload))
	m2.send(&packet.Puback{Ack: packet.Ack{PacketID: got.PacketID}})

	m1.drop()
	back := dialRaw(t, f.a)
	require.True(t, back.connect(oneAtATime("m1", 300)).SessionPresent)
	back.expectNothing() // before the change: job-0 again, Dup=1, with the original identifier
	back.send(&packet.Puback{Ack: packet.Ack{PacketID: first.PacketID}})
	back.send(&packet.Pingreq{})
	_, ok := back.read().(*packet.Pingresp)
	require.True(t, ok, "a PUBACK for the withdrawn identifier is ignored, not a protocol error")
}

func TestAMessageHeldPastTheLimitGoesToAnotherMember(t *testing.T) {
	m1, m2, first := holdingMember(t, holdLimit)

	got := m2.expectPublish()
	require.Equal(t, "job-0", string(got.Payload), "the held message is handed back once the limit passes")
	m2.send(&packet.Puback{Ack: packet.Ack{PacketID: got.PacketID}})

	// m1 is still connected, and its late acknowledgement is accepted quietly.
	m1.send(&packet.Puback{Ack: packet.Ack{PacketID: first.PacketID}})
	m1.send(&packet.Pingreq{})
	_, ok := m1.read().(*packet.Pingresp)
	require.True(t, ok, "a late PUBACK must not end the connection")
}

func TestAMessageHeldWithNoLimitStaysWithItsMember(t *testing.T) {
	_, m2, _ := holdingMember(t, 0)
	m2.expectNoPublishFor(holdLimit + 6*handBackAckWait)
}
