package natsmqtt5_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5/packet"
)

// A client that sets a very small Maximum Packet Size is still told it is
// connected. The success CONNACK states the broker's limits; the ones that are
// optional are left out, cheapest first, until it fits (fitConnack).
// Statements, worded as in the OASIS MQTT 5.0 text read on 2026-10-08:
//   - [MQTT-3.1.2-24] "The Server MUST NOT send packets exceeding Maximum
//     Packet Size to the Client."
//   - [MQTT-3.1.2-25] "Where a Packet is too large to send, the Server MUST
//     discard it without sending it and then behave as if it had completed
//     sending that Packet."
//   - [MQTT-3.2.2-16] the Assigned Client Identifier is returned in the CONNACK
//     (a required property: it is never the one that is left out).
//   - [MQTT-3.3.2-12] "A Server MUST accept all Topic Alias values greater than
//     0 and less than or equal to the Topic Alias Maximum value that it returned
//     in the CONNACK."
//
// The sizes below are of the CONNACK as the broker builds it by default: 24
// bytes in all (Receive Maximum 3, Maximum Packet Size 5, Topic Alias Maximum 3
// and three availability flags and Retain Available at 2 each, plus the 5 of
// the packet itself).
func TestMQTT_3_1_2_24_ASuccessConnackFitsASmallMaximumPacketSize(t *testing.T) {
	cases := []struct {
		limit uint32
		// the properties that must still be there, and those that must not
		has, lacks []string
	}{
		{24, []string{"receive", "maxpacket", "alias", "flags"}, nil},
		{16, []string{"receive", "maxpacket", "alias"}, []string{"flags"}},
		{13, []string{"receive", "maxpacket"}, []string{"alias", "flags"}},
		{8, []string{"receive"}, []string{"maxpacket", "alias", "flags"}},
		{5, nil, []string{"receive", "maxpacket", "alias", "flags"}},
	}
	for _, tc := range cases {
		t.Run(sizeName(tc.limit), func(t *testing.T) {
			addr := startBroker(t, startNATS(t))
			c := dialRaw(t, addr)
			c.send(connectWithMaxPacketSize("small-connack", tc.limit))
			ack, ok := c.read().(*packet.Connack)
			require.True(t, ok, "the client must be told it is connected")
			assert.Equal(t, packet.Success, ack.ReasonCode)

			raw, err := packet.Encode(ack)
			require.NoError(t, err)
			assert.LessOrEqual(t, uint32(len(raw)), tc.limit, "[MQTT-3.1.2-24] the CONNACK exceeds the limit")

			p := ack.Properties
			if p == nil {
				p = &packet.Properties{}
			}
			present := map[string]bool{
				"receive":   p.ReceiveMaximum != nil,
				"maxpacket": p.MaximumPacketSize != nil,
				"alias":     p.TopicAliasMaximum != nil,
				"flags":     p.WildcardSubAvailable != nil && p.SubIDAvailable != nil && p.SharedSubAvailable != nil,
			}
			for _, k := range tc.has {
				assert.True(t, present[k], "%s must still be stated", k)
			}
			for _, k := range tc.lacks {
				assert.False(t, present[k], "%s had to be left out", k)
			}
		})
	}
}

// Leaving Topic Alias Maximum out of the CONNACK tells the client it is 0, and
// the broker holds it to that [MQTT-3.3.2-12]: an alias is a Topic Alias Invalid
// (0x94). A client given the full CONNACK may use one.
func TestMQTT_3_3_2_12_ATopicAliasMaximumLeftOutOfTheConnackIsZero(t *testing.T) {
	publishWithAlias := &packet.Publish{
		Topic: "small/alias", Payload: []byte("x"),
		Properties: &packet.Properties{TopicAlias: packet.Uint16(1)},
	}

	t.Run("left out: the alias is refused", func(t *testing.T) {
		addr := startBroker(t, startNATS(t))
		c := dialRaw(t, addr)
		c.connect(connectWithMaxPacketSize("small-alias-out", 8))
		c.send(publishWithAlias)
		c.expectDisconnect(byte(packet.TopicAliasInvalid))
	})

	t.Run("stated: the alias is accepted", func(t *testing.T) {
		addr := startBroker(t, startNATS(t))
		c := dialRaw(t, addr)
		c.connect(connectWithMaxPacketSize("small-alias-in", 1000))
		c.send(publishWithAlias)
		c.expectNothing()
	})
}

// [MQTT-3.2.2-16] the Assigned Client Identifier is a property the CONNACK has
// to carry, so it is never the one dropped to fit. Where it fits, it is there;
// where even it does not, the client is told in a CONNACK that does (0x85) to
// send an identifier of its own, rather than being left to wait for nothing.
func TestMQTT_3_2_2_16_AnAssignedClientIdentifierIsNeverDroppedToFit(t *testing.T) {
	t.Run("fits once the optional properties are gone", func(t *testing.T) {
		addr := startBroker(t, startNATS(t))
		c := dialRaw(t, addr)
		cp := connectWithMaxPacketSize("", 40)
		c.send(cp)
		ack, ok := c.read().(*packet.Connack)
		require.True(t, ok)
		assert.Equal(t, packet.Success, ack.ReasonCode)
		require.NotNil(t, ack.Properties)
		assert.NotEmpty(t, ack.Properties.AssignedClientID, "[MQTT-3.2.2-16]")
		raw, err := packet.Encode(ack)
		require.NoError(t, err)
		assert.LessOrEqual(t, len(raw), 40)
	})

	t.Run("does not fit: refused with a CONNACK that does", func(t *testing.T) {
		addr := startBroker(t, startNATS(t))
		c := dialRaw(t, addr)
		c.send(connectWithMaxPacketSize("", 20))
		// 0x20, Remaining Length 3, flags 0, Reason Code 0x85, no properties.
		c.expectRawBytes([]byte{0x20, 0x03, 0x00, 0x85, 0x00})
		c.expectClosed() // [MQTT-3.2.2-7]
	})

	t.Run("a client that names itself is not affected", func(t *testing.T) {
		addr := startBroker(t, startNATS(t))
		c := dialRaw(t, addr)
		ack := c.connect(connectWithMaxPacketSize("short", 20))
		assert.Equal(t, packet.Success, ack.ReasonCode)
	})
}

// [MQTT-3.1.2-24], [MQTT-3.1.2-25]: below 5 bytes nothing the broker could say
// fits, so a success CONNACK is discarded and the connection carries on as if it
// had been sent. This is the one case left that the specification gives no
// conforming way to improve.
func TestMQTT_3_1_2_25_ANoConnackFitsBelowFiveBytes(t *testing.T) {
	addr := startBroker(t, startNATS(t))
	c := dialRaw(t, addr)
	c.send(connectWithMaxPacketSize("tiny", 4))
	c.expectNothing()
}

func sizeName(limit uint32) string {
	return fmt.Sprintf("Maximum Packet Size %d", limit)
}
