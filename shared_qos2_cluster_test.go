package natsmqtt5_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// Shared QoS 2 over a real three-server NATS cluster with replicated streams and buckets:
// the group's stream, the session bucket and the broker that holds a message are on
// different servers, which is the path a deployment takes. The embedded-NATS twins are in
// shared_qos2_record_test.go and shared_qos2_killed_test.go.

const clusterAckWait = 4 * time.Second

// clusterQoS2Fleet is two brokers on two servers of a cluster, with persistent sessions and
// SharedQoS2, over streams and buckets replicated three times.
func clusterQoS2Fleet(t *testing.T, ackWait time.Duration) sharedFleet {
	t.Helper()
	c := startNATSCluster(t)
	natsmqtt5.SetSharedAckWait(t, ackWait)
	ba, a, _ := startBrokerHandle(t, c.URL(0), sweeping, clusterReplicas, persistentWithQueue, sharedQoS2)
	bb, b, _ := startBrokerHandle(t, c.URL(1), sweeping, clusterReplicas, persistentWithQueue, sharedQoS2)
	return sharedFleet{a: a, b: b, brokerA: ba, brokerB: bb}
}

func (c *rawClient) joinQoS2(t *testing.T, id string) {
	t.Helper()
	c.connect(rawConnect(id, 300))
	require.Equal(t, []packet.ReasonCode{packet.GrantedQoS2}, c.subscribeCodes(group, packet.QoS2))
}

// A member's broker cannot write the session record for a held message: nothing is sent,
// the message goes back to the group and a member on another server gets it, as a first
// attempt, and completes it. The holder is never sent it.
func TestClusterASharedQoS2MessageWhoseRecordCannotBeWrittenGoesToAMemberOnAnotherServer_MQTT_4_4_0_1_MQTT_4_8_2_4(t *testing.T) {
	f := clusterQoS2Fleet(t, handBackAckWait)

	holder := dialRaw(t, f.a)
	holder.joinQoS2(t, "holder")
	failingRecords(t, f.brokerA)

	f.publishQoS2From(t, f.b, "job-q2")
	holder.expectClosed() // no PUBLISH: the first byte read is the end of the connection

	other := dialRaw(t, f.b)
	other.joinQoS2(t, "other")
	got := other.expectPublish()
	assert.Equal(t, "job-q2", string(got.Payload))
	assert.False(t, got.Dup, "a first attempt: this client was never sent it")
	assert.Equal(t, packet.QoS2, got.QoS)
	other.completeQoS2(got)
	other.expectNoPublishFor(3 * handBackAckWait)
}

// A PUBREL whose record cannot be written waits for the client. It comes back on the other
// server once the write works, is sent the PUBREL on its original identifier, completes, and
// nobody else is given the message.
func TestClusterASharedQoS2PubrelWhoseRecordCannotBeWrittenIsCompletedAfterReconnectingToAnotherServer_MQTT_4_3_3_6_MQTT_4_4_0_1(t *testing.T) {
	f := clusterQoS2Fleet(t, handBackAckWait)

	holder := dialRaw(t, f.a)
	holder.joinQoS2(t, "holder")
	f.publishQoS2From(t, f.b, "job-own")
	sent := holder.expectPublish()
	other := dialRaw(t, f.b)
	other.joinQoS2(t, "other")

	restore := failingRecords(t, f.brokerA)
	holder.send(&packet.Pubrec{Ack: packet.Ack{PacketID: sent.PacketID}})
	holder.expectClosed() // no PUBREL
	restore()

	back := dialRaw(t, f.b)
	require.True(t, back.connect(rawConnect("holder", 300)).SessionPresent)
	assert.Equal(t, sent.PacketID, back.expectPubrel().PacketID, "[MQTT-4.4.0-1] the PUBREL, original identifier")
	back.send(&packet.Pubcomp{Ack: packet.Ack{PacketID: sent.PacketID}})
	other.expectNoPublishFor(3 * handBackAckWait)
	back.expectNoPublishFor(handBackAckWait)
}

// The member's broker is killed after its client has the PUBLISH; the client comes back on
// another server of the cluster, after the AckWait, and completes the same message on its
// original identifier. The group's other member is never given it.
func TestClusterASharedQoS2MessageHeldForAKilledBrokersClientIsCompletedAfterItReconnectsToAnotherServer_MQTT_4_8_2_4_MQTT_4_8_2_5(t *testing.T) {
	// The orphan scan reads the replicated session bucket and probes the dead broker within
	// AckWait/3; a cluster read does not fit in 300 ms, so the AckWait is longer here.
	f := clusterQoS2Fleet(t, clusterAckWait)

	m := dialRaw(t, f.a)
	m.joinQoS2(t, "sq2-cluster")
	f.publishQoS2From(t, f.b, "job-away")
	sent := m.expectPublish()
	require.Equal(t, packet.QoS2, sent.QoS)
	natsmqtt5.Kill(f.brokerA)
	t.Cleanup(func() { natsmqtt5.Reap(f.brokerA) })

	other := dialRaw(t, f.b)
	other.joinQoS2(t, "sq2-cluster-other")
	other.expectNoPublishFor(5 * clusterAckWait)

	back := dialRaw(t, f.b)
	require.True(t, back.connect(rawConnect("sq2-cluster", 300)).SessionPresent)
	again := back.expectPublish()
	assert.True(t, again.Dup, "a resend")
	assert.Equal(t, sent.PacketID, again.PacketID, "[MQTT-4.4.0-1] with the original Packet Identifier")
	assert.Equal(t, "job-away", string(again.Payload))
	back.completeQoS2(again)
	other.expectNoPublishFor(2 * clusterAckWait)
	back.expectNoPublishFor(time.Second)
}
