package natsmqtt5_test

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// A shared QoS 2 message whose record cannot be written is not sent. The record is what
// lets a successor of a broker killed after the client has the PUBLISH resend it
// [MQTT-4.4.0-1]; without it the group gives the message to another member after the
// AckWait and the client has it twice. A message that is the session's alone is served
// as it was (the README's limit of QoS 2 across a killed broker); one held in JetStream
// has somewhere better to go: back to the group, unsent.

// failingRecords makes every write of b's session records fail until the returned
// function is called.
func failingRecords(t *testing.T, b *natsmqtt5.Broker) (restore func()) {
	t.Helper()
	var failing atomic.Bool
	failing.Store(true)
	natsmqtt5.SetSessionSaveFault(b, func() error {
		if failing.Load() {
			return errors.New("the bucket is unavailable")
		}
		return nil
	})
	t.Cleanup(func() { natsmqtt5.SetSessionSaveFault(b, nil) })
	return func() { failing.Store(false) }
}

func (f sharedFleet) publishQoS2From(t *testing.T, addr, payload string) {
	t.Helper()
	pub, _ := connectClient(t, addr, connectOpts("producer-q2"))
	pub.publish(&paho.Publish{Topic: "jobs/x", QoS: 2, Payload: []byte(payload)})
}

func TestASharedQoS2MessageWhoseRecordCannotBeWrittenIsGivenToAnotherMemberAndNotSent_MQTT_4_4_0_1_MQTT_4_8_2_4(t *testing.T) {
	f := newSharedFleet(t, persistentWithQueue, sharedQoS2)

	holder := dialRaw(t, f.a)
	holder.connect(rawConnect("holder", 300))
	require.Equal(t, []packet.ReasonCode{packet.GrantedQoS2}, holder.subscribeCodes(group, packet.QoS2))
	failingRecords(t, f.brokerA)

	f.publishQoS2From(t, f.b, "job-q2")
	holder.expectClosed() // no PUBLISH: the first byte read is the end of the connection

	other := member(t, f.b, "other", 300)
	got := other.expectPublish()
	assert.Equal(t, "job-q2", string(got.Payload), "the group gave the message to the member that could be recorded")
	other.send(&packet.Puback{Ack: packet.Ack{PacketID: got.PacketID}})
	other.expectNoPublishFor(2 * handBackAckWait)
}

// A failed record at the PUBREL stage: no PUBREL goes out, because a broker killed
// after the client answered it would leave the message to the group. The client that
// comes back is sent the PUBREL once the record can be written, completes the exchange,
// and the other member is never given the message.
func TestASharedQoS2PubrelWhoseRecordCannotBeWrittenWaitsForTheClientsReturn_MQTT_4_3_3_6_MQTT_4_4_0_1(t *testing.T) {
	f := newSharedFleet(t, persistentWithQueue, sharedQoS2)

	holder := dialRaw(t, f.a)
	holder.connect(rawConnect("holder", 300))
	require.Equal(t, []packet.ReasonCode{packet.GrantedQoS2}, holder.subscribeCodes(group, packet.QoS2))
	f.publishQoS2From(t, f.b, "job-own")
	sent := holder.expectPublish()
	other := member(t, f.b, "other", 300)

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
