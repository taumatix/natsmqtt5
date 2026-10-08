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

// A resumed connection that resends a message the client acknowledged a moment
// ago has made a mistake the client cannot be blamed for. The broker "MUST
// resend any unacknowledged PUBLISH packets (where QoS > 0) and PUBREL packets"
// on resumption [MQTT-4.4.0-1]; this resend was of a message that had just been
// acknowledged, and the client's acknowledgement of the copy names a Packet
// Identifier the session no longer holds. That must be ignored: answering it
// with a Protocol Error disconnects a conforming client.
//
// The window is the one between the broker claiming the in-flight entry and
// writing the packet; SetResendGate holds the resend there so the acknowledgement
// lands in it every time, over a real socket and a real NATS server.
func TestAnAckThatLandsWhileAResendIsBeingWrittenDoesNotDisconnectTheClient_MQTT_4_4_0_1(t *testing.T) {
	cases := []struct {
		name string
		qos  packet.QoS
		// firstAck is what the client sends for the message it received on the
		// connection that dropped; copyAck for the copy the broker then resends.
		prepare func(c *rawClient, id uint16)
		first   func(id uint16) packet.Packet
		resent  func(c *rawClient) uint16
		copyAck func(id uint16) packet.Packet
	}{
		{
			name: "QoS 1 PUBLISH, acknowledged with PUBACK",
			qos:  packet.QoS1,
			first: func(id uint16) packet.Packet {
				return &packet.Puback{Ack: packet.Ack{PacketID: id}}
			},
			resent: func(c *rawClient) uint16 {
				p := c.expectPublish()
				assert.True(t, p.Dup, "[MQTT-3.3.1-1]: a resend sets DUP")
				return p.PacketID
			},
			copyAck: func(id uint16) packet.Packet {
				return &packet.Puback{Ack: packet.Ack{PacketID: id}}
			},
		},
		{
			name: "QoS 2 PUBREL, acknowledged with PUBCOMP",
			qos:  packet.QoS2,
			prepare: func(c *rawClient, id uint16) {
				c.send(&packet.Pubrec{Ack: packet.Ack{PacketID: id}})
				require.Equal(t, id, c.expectPubrel().PacketID)
			},
			first: func(id uint16) packet.Packet {
				return &packet.Pubcomp{Ack: packet.Ack{PacketID: id}}
			},
			resent: func(c *rawClient) uint16 { return c.expectPubrel().PacketID },
			copyAck: func(id uint16) packet.Packet {
				return &packet.Pubcomp{Ack: packet.Ack{PacketID: id}}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			natsURL := startNATS(t)
			broker, addr, _ := startBrokerHandle(t, natsURL)
			id := "late-ack"

			a := dialRaw(t, addr)
			a.connect(rawConnect(id, 300))
			a.subscribe("late/x", tc.qos)
			pub, _ := connectClient(t, addr, connectOpts("late-pub"))
			pub.publish(&paho.Publish{Topic: "late/x", QoS: byte(tc.qos), Payload: []byte("m")})
			sent := a.expectPublish()
			if tc.prepare != nil {
				tc.prepare(a, sent.PacketID)
			}
			a.drop()

			reached := make(chan struct{})
			release := make(chan struct{})
			natsmqtt5.SetResendGate(broker, func(uint16) {
				close(reached)
				<-release
			})
			t.Cleanup(func() { natsmqtt5.SetResendGate(broker, nil) })

			b := dialRaw(t, addr)
			require.True(t, b.connect(rawConnect(id, 300)).SessionPresent)
			select {
			case <-reached:
			case <-time.After(5 * time.Second):
				t.Fatal("the broker never began resending")
			}

			// The client flushes the acknowledgement it owed, and the broker has
			// applied it before the resend is written.
			b.send(tc.first(sent.PacketID))
			require.Eventually(t, func() bool { return natsmqtt5.InflightCount(broker, id) == 0 },
				5*time.Second, 5*time.Millisecond)
			close(release)

			// The resend goes out regardless (that is the broker's mistake), and
			// the client acknowledges it as it must.
			b.send(tc.copyAck(tc.resent(b)))

			// The connection is still up: a message published now arrives.
			pub.publish(&paho.Publish{Topic: "late/x", QoS: 1, Payload: []byte("after")})
			require.NoError(t, b.nc.SetReadDeadline(time.Now().Add(5*time.Second)))
			got, err := packet.Read(b.r, 0)
			require.NoError(t, err)
			if d, ok := got.(*packet.Disconnect); ok {
				t.Fatalf("the client was disconnected with 0x%02X for acknowledging a resent message", byte(d.ReasonCode))
			}
			p, ok := got.(*packet.Publish)
			require.True(t, ok, "expected a PUBLISH, got %s", got.Type())
			assert.Equal(t, "after", string(p.Payload))
		})
	}
}
