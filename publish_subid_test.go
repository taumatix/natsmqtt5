package natsmqtt5_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5/packet"
)

// rawPublish is a PUBLISH written byte by byte from MQTT-5.0 §3.3 to topic
// "a/b" with the given QoS (0 or 1; Packet Identifier 1 at QoS 1), the given
// property bytes (§3.3.2.3, without their length) and the payload "x".
func rawPublish(qos byte, props []byte) []byte {
	body := []byte{0x00, 0x03, 'a', '/', 'b'}
	if qos > 0 {
		body = append(body, 0x00, 0x01)
	}
	body = append(body, byte(len(props)))
	body = append(body, props...)
	body = append(body, 'x')
	return append([]byte{0x30 | qos<<1, byte(len(body))}, body...)
}

// "A PUBLISH packet sent from a Client to a Server MUST NOT contain a
// Subscription Identifier" [MQTT-3.3.4-6] (OASIS text read 2026-10-08), and a
// packet that does is a Protocol Error: "an error that is detected after the
// packet has been parsed and found to contain data that is not allowed by the
// protocol" (§1.2). On it the Server uses Reason Code 0x82, "MUST close the
// Network Connection" [MQTT-4.13.1-1], and must not act on the packet: nothing
// is acknowledged and nothing reaches a subscriber.
func TestAClientPublishWithASubscriptionIdentifierIsAProtocolError(t *testing.T) {
	addr := startBroker(t, startNATS(t))
	subIDProperty := []byte{0x0B, 0x01} // Subscription Identifier (0x0B) = 1, a Variable Byte Integer

	listener, _ := sessionPresentOnConnect(t, addr, "subid-listener", true, 0)
	listener.subscribe("a/b", packet.QoS1)

	for _, qos := range []byte{0, 1} {
		name := map[byte]string{0: "QoS 0", 1: "QoS 1"}[qos]
		t.Run(name, func(t *testing.T) {
			c, _ := sessionPresentOnConnect(t, addr, "subid-pub-"+name, true, 0)
			_, err := c.nc.Write(rawPublish(qos, subIDProperty))
			require.NoError(t, err)
			c.expectDisconnect(0x82) // not a PUBACK (0x40), and the connection closes
			listener.expectNothing()
		})
	}

	t.Run("the same PUBLISH without the property is acknowledged and forwarded", func(t *testing.T) {
		c, _ := sessionPresentOnConnect(t, addr, "subid-control", true, 0)
		_, err := c.nc.Write(rawPublish(1, nil))
		require.NoError(t, err)
		first, body := c.readRawPacket()
		assert.Equal(t, byte(0x40), first, "a PUBACK")
		assert.Equal(t, []byte{0x00, 0x01}, body[:2], "for Packet Identifier 1")
		assert.Equal(t, "a/b", listener.expectPublish().Topic)
	})
}
