package natsmqtt5_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// rawSubscribe is a SUBSCRIBE written byte by byte from MQTT-5.0 §3.8 for one
// Topic Filter and its Subscription Options byte (§3.8.3.1), not through this
// module's encoder.
func rawSubscribe(packetID uint16, filtersAndOptions ...any) []byte {
	body := []byte{byte(packetID >> 8), byte(packetID), 0x00} // Packet Identifier, Property Length 0
	for i := 0; i < len(filtersAndOptions); i += 2 {
		filter := filtersAndOptions[i].(string)
		body = append(body, 0x00, byte(len(filter)))
		body = append(body, filter...)
		body = append(body, filtersAndOptions[i+1].(byte))
	}
	return append([]byte{0x82, byte(len(body))}, body...)
}

const (
	optQoS1    = 0x01
	optNoLocal = 0x04 // bit 2 of the Subscription Options (§3.8.3.1)
)

// expectDisconnect reads the next packet, requires it to be a DISCONNECT
// (0xE0, §3.14) carrying the Reason Code, and requires the broker to close the
// connection after it.
func (c *rawClient) expectDisconnect(reason byte) {
	c.t.Helper()
	first, body := c.readRawPacket()
	require.Equal(c.t, byte(0xE0), first, "the packet after the error must be a DISCONNECT, not an acknowledgement")
	require.NotEmpty(c.t, body, "a DISCONNECT with a non-zero Reason Code carries it")
	require.Equal(c.t, reason, body[0], "DISCONNECT Reason Code")
	c.expectClosed()
}

// "It is a Protocol Error to set the No Local bit to 1 on a Shared
// Subscription" [MQTT-3.8.3-4] (OASIS text read 2026-10-08). "The Server sending
// a SUBACK packet MUST use one of the Subscribe Reason Codes for each Topic
// Filter received" [MQTT-3.9.3-2]: 0x82 is not one (Table 3-8), so the broker may
// not answer with a SUBACK. "When a Server detects a Malformed Packet or Protocol
// Error, and a Reason Code is given in the specification, it MUST close the
// Network Connection" [MQTT-4.13.1-1]; the Reason Codes to use are 0x81 or 0x82.
func TestNoLocalOnASharedSubscriptionIsAProtocolErrorThatClosesTheConnection(t *testing.T) {
	addr := startBroker(t, startNATS(t))

	t.Run("a single shared filter with No Local", func(t *testing.T) {
		c, _ := sessionPresentOnConnect(t, addr, "nl-single", true, 0)
		_, err := c.nc.Write(rawSubscribe(1, "$share/g/a/b", byte(optQoS1|optNoLocal)))
		require.NoError(t, err)
		c.expectDisconnect(0x82)
	})

	t.Run("a valid filter before it is not granted either", func(t *testing.T) {
		// The packet as a whole is the error, so the broker must not answer the
		// first filter with a SUBACK and keep the connection.
		c, _ := sessionPresentOnConnect(t, addr, "nl-second", true, 0)
		_, err := c.nc.Write(rawSubscribe(1, "plain/a", byte(optQoS1), "$share/g/a/b", byte(optQoS1|optNoLocal)))
		require.NoError(t, err)
		c.expectDisconnect(0x82)
	})

	t.Run("a shared filter without No Local is granted", func(t *testing.T) {
		c, _ := sessionPresentOnConnect(t, addr, "nl-control", true, 0)
		_, err := c.nc.Write(rawSubscribe(1, "$share/g/a/b", byte(optQoS1)))
		require.NoError(t, err)
		first, body := c.readRawPacket()
		assert.Equal(t, byte(0x90), first, "a SUBACK")
		// Packet Identifier 1, Property Length 0, Reason Code 0x01 (Granted QoS 1).
		assert.Equal(t, []byte{0x00, 0x01, 0x00, 0x01}, body)
	})

	t.Run("No Local on an ordinary filter is granted", func(t *testing.T) {
		c, _ := sessionPresentOnConnect(t, addr, "nl-plain", true, 0)
		_, err := c.nc.Write(rawSubscribe(1, "a/b", byte(optQoS1|optNoLocal)))
		require.NoError(t, err)
		first, body := c.readRawPacket()
		assert.Equal(t, byte(0x90), first, "a SUBACK")
		assert.Equal(t, []byte{0x00, 0x01, 0x00, 0x01}, body)
	})
}
