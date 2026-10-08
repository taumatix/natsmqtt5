package natsmqtt5_test

import (
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// The Receive Maximum a client states is a limit on the broker. MQTT-5.0
// §3.3.4, read from the OASIS text on 2026-10-08:
//
//	[MQTT-3.3.4-9]  "The Server MUST NOT send more than Receive Maximum QoS 1 and
//	                 QoS 2 PUBLISH packets for which it has not received PUBACK,
//	                 PUBCOMP, or PUBREC with a Reason Code of 128 or greater from
//	                 the Client."
//
// A resumed connection resends what the client had not acknowledged. When the
// client's acknowledgement of the original reaches the broker after the resend
// has been begun and before the copy is written, the copy still goes out, and
// the client owes an acknowledgement of it. The send-quota slot it holds must
// stay held until then: with a Receive Maximum of 1, the next message must wait
// for the copy's acknowledgement, not follow the original's.
//
// SetResendGate holds the resend in that window, so the acknowledgement lands in
// it every time, over a real socket and a real NATS server.
func TestMQTT_3_3_4_9_AnAckThatOvertookAResendDoesNotFreeTheCopysSlot(t *testing.T) {
	cases := []struct {
		name string
		qos  packet.QoS
		// original ends the exchange for the message the first connection
		// received; copyAck is what the client sends for the copy, and finish
		// whatever more the broker then asks of it.
		original func(id uint16) packet.Packet
		copyAck  func(c *rawClient, id uint16)
	}{
		{
			name:     "QoS 1, PUBACK",
			qos:      packet.QoS1,
			original: func(id uint16) packet.Packet { return &packet.Puback{Ack: packet.Ack{PacketID: id}} },
			copyAck: func(c *rawClient, id uint16) {
				c.send(&packet.Puback{Ack: packet.Ack{PacketID: id}})
			},
		},
		{
			name: "QoS 2, PUBREC with a Reason Code of 128 or greater",
			qos:  packet.QoS2,
			original: func(id uint16) packet.Packet {
				return &packet.Pubrec{Ack: packet.Ack{PacketID: id, ReasonCode: packet.UnspecifiedError}}
			},
			copyAck: func(c *rawClient, id uint16) {
				c.send(&packet.Pubrec{Ack: packet.Ack{PacketID: id, ReasonCode: packet.UnspecifiedError}})
				// The exchange is over for the broker, which says so; the client's
				// PUBCOMP is what closes the copy's.
				require.Equal(t, id, c.expectPubrel().PacketID)
				c.send(&packet.Pubcomp{Ack: packet.Ack{PacketID: id}})
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			natsURL := startNATS(t)
			broker, addr, _ := startBrokerHandle(t, natsURL)
			id := "overtaken"

			a := dialRaw(t, addr)
			a.connect(rawConnect(id, 300))
			a.subscribe("q/x", tc.qos)
			pub, _ := connectClient(t, addr, connectOpts("q-pub"))
			pub.publish(&paho.Publish{Topic: "q/x", QoS: byte(tc.qos), Payload: []byte("first")})
			sent := a.expectPublish()
			a.drop()

			reached := make(chan struct{})
			release := make(chan struct{})
			natsmqtt5.SetResendGate(broker, func(uint16) {
				close(reached)
				<-release
			})
			t.Cleanup(func() { natsmqtt5.SetResendGate(broker, nil) })

			b := dialRaw(t, addr)
			cp := rawConnect(id, 300)
			cp.Properties.ReceiveMaximum = packet.Uint16(1)
			require.True(t, b.connect(cp).SessionPresent)
			select {
			case <-reached:
			case <-time.After(5 * time.Second):
				t.Fatal("the broker never began resending")
			}

			b.send(tc.original(sent.PacketID))
			require.Eventually(t, func() bool { return natsmqtt5.InflightCount(broker, id) == 0 },
				5*time.Second, 5*time.Millisecond)
			close(release)

			copyPub := b.expectPublish()
			assert.True(t, copyPub.Dup, "[MQTT-3.3.1-1]: a resend sets DUP")

			// One PUBLISH is unacknowledged (the copy) and Receive Maximum is 1.
			pub.publish(&paho.Publish{Topic: "q/x", QoS: byte(tc.qos), Payload: []byte("second")})
			b.expectNoPublishFor(700 * time.Millisecond) // [MQTT-3.3.4-9]

			tc.copyAck(b, copyPub.PacketID)
			next := b.expectPublish()
			assert.Equal(t, "second", string(next.Payload))
		})
	}
}
