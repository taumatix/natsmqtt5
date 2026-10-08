package natsmqtt5_test

import (
	"sync/atomic"
	"testing"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5/packet"
)

// A withdrawn identifier is owed one particular acknowledgement: a PUBACK for a
// QoS 1 message, the PUBCOMP that ends the exchange for a QoS 2 one. Any other
// acknowledgement of it is a Protocol Error like an acknowledgement of an
// identifier that was never sent (the specification's wording for that is not
// in this repository, so it is not quoted: unverified), and it must not use up
// the record of the one that is owed. The client below is disconnected for the
// wrong packet and, on its next connection, is still allowed to send the right
// one without being disconnected [MQTT-4.4.0-1].
func TestAWithdrawnIdentifierOnlyAcceptsTheAcknowledgementItIsOwed_MQTT_4_4_0_1(t *testing.T) {
	cases := []struct {
		name         string
		qos          packet.QoS
		wrong, right func(id uint16) packet.Packet
	}{
		{"QoS 1 owes a PUBACK, a PUBCOMP is wrong", packet.QoS1,
			func(id uint16) packet.Packet { return &packet.Pubcomp{Ack: packet.Ack{PacketID: id}} },
			func(id uint16) packet.Packet { return &packet.Puback{Ack: packet.Ack{PacketID: id}} }},
		{"QoS 2 owes a PUBCOMP, a PUBACK is wrong", packet.QoS2,
			func(id uint16) packet.Packet { return &packet.Puback{Ack: packet.Ack{PacketID: id}} },
			func(id uint16) packet.Packet { return &packet.Pubcomp{Ack: packet.Ack{PacketID: id}} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var deny atomic.Bool
			addr := startBroker(t, startNATS(t), narrowingAuthorizer(&deny))
			const id = "withdrawn-type"

			c1 := dialRaw(t, addr)
			require.False(t, c1.connect(rawConnect(id, 300)).SessionPresent)
			c1.subscribe("secret/#", tc.qos)
			pub, _ := connectClient(t, addr, connectOpts("withdrawn-type-pub"))
			pub.publish(&paho.Publish{Topic: "secret/a", QoS: byte(tc.qos), Payload: []byte("classified")})
			owed := c1.expectPublish().PacketID
			c1.drop()

			deny.Store(true)
			c2 := dialRaw(t, addr)
			require.True(t, c2.connect(rawConnect(id, 300)).SessionPresent)
			c2.expectNothing()
			c2.send(tc.wrong(owed))
			c2.expectDisconnect(byte(packet.ProtocolError))

			c3 := dialRaw(t, addr)
			require.True(t, c3.connect(rawConnect(id, 300)).SessionPresent)
			c3.send(tc.right(owed))
			c3.expectNothing()
		})
	}
}
