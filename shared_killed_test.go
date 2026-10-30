package natsmqtt5_test

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5/packet"
)

// A shared member's broker is killed (SIGKILL) while its QoS 1 message is
// unacknowledged, and the session is restored on another broker.
//
// The killed broker stops saying "still working" (held.go), so JetStream gives
// the message to the group's other member sharedAckWait later (30 seconds in the
// broker binary), unless the session has been restored by then: the broker that
// restores it holds the message again. Two outcomes follow, and both are pinned:
//
//   - restored before AckWait: the client is sent the message once (resent, DUP
//     set, original Packet Identifier) and the group never gets it;
//   - restored after AckWait: the client is sent it resent from the record and
//     the other member is sent it as a new message. That is at-least-once, which
//     QoS 1 allows, and a change must not make it at-most-once (a lost message)
//     or deliver it a third time.
//
// Statement wording, read from the OASIS MQTT 5.0 specification (os edition):
//
//	[MQTT-4.4.0-1] "When a Client reconnects with Clean Start set to 0 and a
//	  session is present, both the Client and Server MUST resend any
//	  unacknowledged PUBLISH packets (where QoS > 0) and PUBREL packets using
//	  their original Packet Identifiers."
//	§4.8.2 (no statement id) "If the Server is in the process of sending a QoS 1
//	  message to its chosen subscribing Client and the connection to that Client
//	  breaks before the Server has received an acknowledgement from the Client,
//	  the Server MAY wait for the Client to reconnect and retransmit the message
//	  to that Client."
//
// They wait out JetStream's real AckWait, so they are gated:
// NATSMQTT5_SLOW_TESTS=1.

const killedAckWait = 30 * time.Second

// killedSharedMember starts a member on a broker binary, has it be sent job-k
// (checkpointed as in flight), puts a second member on a surviving broker, and
// kills the binary. keepalive is off on the raw clients, which send no pings.
func killedSharedMember(t *testing.T) (survivor string, sent *packet.Publish, other *rawClient, natsURL string) {
	t.Helper()
	if os.Getenv("NATSMQTT5_SLOW_TESTS") == "" {
		t.Skip("waits out the 30 s AckWait of the broker binary; set NATSMQTT5_SLOW_TESTS=1")
	}
	natsURL = startNATS(t)
	child := startBrokerBinary(t, natsURL, "-persistent-sessions", "-session-checkpoint-interval", killedBrokerCheckpoint)
	survivor = startBroker(t, natsURL, persistentWithQueue)

	victim := dialRaw(t, child.addr)
	victim.connect(noKeepAlive(rawConnect("killed-member", 300)))
	victim.subscribe(group, packet.QoS1)
	sharedFleet{a: survivor, b: survivor}.publish(t, "job-k")
	sent = victim.expectPublish()
	require.False(t, sent.Dup)
	other = dialRaw(t, survivor)
	other.connect(noKeepAlive(rawConnect("other-member", 300)))
	other.subscribe(group, packet.QoS1)

	awaitCheckpointedInflight(t, natsURL, "killed-member", sent.PacketID, false)
	child.kill9(t)
	return survivor, sent, other, natsURL
}

func noKeepAlive(cp *packet.Connect) *packet.Connect {
	cp.KeepAlive = 0
	return cp
}

// restoreKilledMember connects the killed member's session on the survivor and
// returns what it is resent.
func restoreKilledMember(t *testing.T, survivor string, sent *packet.Publish) (*rawClient, *packet.Publish) {
	t.Helper()
	back := dialRaw(t, survivor)
	require.True(t, back.connect(noKeepAlive(rawConnect("killed-member", 300))).SessionPresent)
	again := back.expectPublish()
	assert.True(t, again.Dup, "the restored session resends it with DUP set")
	assert.Equal(t, sent.PacketID, again.PacketID, "[MQTT-4.4.0-1] the original Packet Identifier")
	assert.Equal(t, "job-k", string(again.Payload))
	return back, again
}

// Restored before AckWait: the restored session holds the message again, so the
// group is not given it as well.
func TestAKilledBrokersHeldSharedMessageIsHeldAgainByTheRestoredSession(t *testing.T) {
	t.Parallel()
	survivor, sent, other, _ := killedSharedMember(t)
	back, again := restoreKilledMember(t, survivor, sent)

	other.expectNoPublishFor(killedAckWait + 5*time.Second)
	back.send(&packet.Puback{Ack: packet.Ack{PacketID: again.PacketID}})
	other.expectNoPublishFor(2 * time.Second)
	back.expectNoPublishFor(2 * time.Second)
}

// Restored after AckWait: JetStream has already given the message to the other
// member, so it is seen twice; once each is acknowledged, neither is sent it
// again, and the late acknowledgement of the restored copy is accepted.
func TestAKilledBrokersHeldSharedMessageRestoredLateIsSeenTwiceNotThrice(t *testing.T) {
	t.Parallel()
	survivor, sent, other, _ := killedSharedMember(t)

	fresh := other.readPublishWithin(killedAckWait + 15*time.Second)
	assert.Equal(t, "job-k", string(fresh.Payload))
	assert.False(t, fresh.Dup, "a message from the group is a new one")
	back, again := restoreKilledMember(t, survivor, sent)

	other.send(&packet.Puback{Ack: packet.Ack{PacketID: fresh.PacketID}})
	back.send(&packet.Puback{Ack: packet.Ack{PacketID: again.PacketID}})
	back.send(&packet.Pingreq{})
	_, ok := back.read().(*packet.Pingresp)
	require.True(t, ok, "the late PUBACK of the restored copy is accepted")
	other.expectNoPublishFor(3 * time.Second)
	back.expectNoPublishFor(time.Second)
}

// readPublishWithin waits up to d for the next packet, which must be a PUBLISH.
func (c *rawClient) readPublishWithin(d time.Duration) *packet.Publish {
	c.t.Helper()
	require.NoError(c.t, c.nc.SetReadDeadline(time.Now().Add(d)))
	p, err := packet.Read(c.r, 0)
	require.NoError(c.t, err)
	pub, ok := p.(*packet.Publish)
	require.True(c.t, ok, "expected a PUBLISH, got %s: %+v", p.Type(), p)
	return pub
}
