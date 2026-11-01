package natsmqtt5_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// SharedHoldLimit across a three-server cluster with replicated streams: the member that holds a
// message without acknowledging it is on a broker on one NATS server, the member that gets it
// handed back on a broker on another. The hand-back is a JetStream redelivery of a message in a
// stream whose leader may be on neither, so this is the path a deployment takes; the limit is the
// same one for every group (a per-group value has no case that this measurement, or a report,
// has found).
func TestClusterAMessageHeldPastTheSharedHoldLimitGoesToAMemberOnAnotherServer(t *testing.T) {
	c := startNATSCluster(t)
	natsmqtt5.SetSharedAckWait(t, handBackAckWait)
	withLimit := func(o *natsmqtt5.Options) { o.SharedHoldLimit = holdLimit }
	_, a, _ := startBrokerHandle(t, c.URL(0), sweeping, clusterReplicas, withLimit)
	_, b, _ := startBrokerHandle(t, c.URL(1), sweeping, clusterReplicas, withLimit)
	f := sharedFleet{a: a, b: b}

	m1 := dialRaw(t, f.a)
	m1.connect(oneAtATime("m1", 0))
	m1.subscribe(group, packet.QoS1)
	f.publish(t, "job-0")
	first := m1.expectPublish()
	require.Equal(t, "job-0", string(first.Payload))
	m2 := member(t, f.b, "m2", 0)

	got := m2.expectPublish()
	require.Equal(t, "job-0", string(got.Payload), "the held message is handed back once the limit passes")
	m2.send(&packet.Puback{Ack: packet.Ack{PacketID: got.PacketID}})

	m1.send(&packet.Puback{Ack: packet.Ack{PacketID: first.PacketID}})
	m1.send(&packet.Pingreq{})
	_, ok := m1.read().(*packet.Pingresp)
	require.True(t, ok, "a late PUBACK must not end the connection")

	// And it was handed back once: nothing more reaches either member.
	m2.expectNoPublishFor(holdLimit + 3*handBackAckWait)
}
