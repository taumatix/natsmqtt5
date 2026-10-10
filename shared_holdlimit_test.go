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
	t.Helper()
	f := newSharedFleet(t, func(o *natsmqtt5.Options) { o.SharedHoldLimit = limit })
	m1 = dialRaw(t, f.a)
	m1.connect(oneAtATime("m1", 0))
	m1.subscribe(group, packet.QoS1)
	f.publish(t, "job-0")
	first = m1.expectPublish()
	require.Equal(t, "job-0", string(first.Payload))
	m2 = member(t, f.b, "m2", 0)
	return m1, m2, first
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
