package natsmqtt5_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5/packet"
)

// responseTopicProperty is the Response Topic property (0x08, §3.3.2.3.5): the
// identifier, then a UTF-8 Encoded String.
func responseTopicProperty(topic string) []byte {
	return append([]byte{0x08, 0x00, byte(len(topic))}, topic...)
}

// rawConnectWithWill is a CONNECT with Clean Start and a Will (flags 0x06) whose
// Will Properties are willProps, written from MQTT-5.0 §3.1: the Will Properties
// and Will Topic follow the Client Identifier (§3.1.3).
func rawConnectWithWill(clientID string, willProps []byte) []byte {
	body := []byte{
		0x00, 0x04, 'M', 'Q', 'T', 'T', 0x05,
		0x06,       // Clean Start + Will Flag, Will QoS 0, no Will Retain
		0x00, 0x1E, // keep alive 30 s
		0x00, // no CONNECT properties
		0x00, byte(len(clientID)),
	}
	body = append(body, clientID...)
	body = append(body, byte(len(willProps)))
	body = append(body, willProps...)
	body = append(body, 0x00, 0x03, 'w', '/', 't') // Will Topic
	body = append(body, 0x00, 0x01, 'x')           // Will Payload (binary data)
	return append([]byte{0x10, byte(len(body))}, body...)
}

// "The Response Topic MUST NOT contain wildcard characters" [MQTT-3.3.2-14]
// (OASIS text read 2026-10-08, §3.3.2.3.5). The PUBLISH parses, so it is not a
// Malformed Packet (§1.2: one "that cannot be parsed"); it holds "data that is
// not allowed by the protocol", which §1.2 defines as a Protocol Error. §4.13.1
// prescribes 0x81 or 0x82 for these, and the Server "MUST close the Network
// Connection" [MQTT-4.13.1-1]. The broker therefore sends DISCONNECT 0x82 and
// closes, and forwards nothing. 0x90 (Topic Name invalid) is not the answer: it
// describes the PUBLISH's own Topic Name, and there is no PUBACK at QoS 0.
func TestAResponseTopicWithWildcardsIsAProtocolError(t *testing.T) {
	addr := startBroker(t, startNATS(t))

	listener, _ := sessionPresentOnConnect(t, addr, "rt-listener", true, 0)
	listener.subscribe("a/b", packet.QoS1)

	for _, tc := range []struct {
		name  string
		topic string
		qos   byte
	}{
		{"single-level wildcard, QoS 0", "reply/+", 0},
		{"multi-level wildcard, QoS 0", "reply/#", 0},
		{"single-level wildcard, QoS 1", "reply/+", 1},
		{"multi-level wildcard, QoS 1", "#", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, _ := sessionPresentOnConnect(t, addr, "rt-pub", true, 0)
			_, err := c.nc.Write(rawPublish(tc.qos, responseTopicProperty(tc.topic)))
			require.NoError(t, err)
			c.expectDisconnect(0x82) // not a PUBACK, and the connection closes
			listener.expectNothing()
		})
	}

	t.Run("a Response Topic without wildcards is forwarded unaltered [MQTT-3.3.2-15]", func(t *testing.T) {
		c, _ := sessionPresentOnConnect(t, addr, "rt-control", true, 0)
		_, err := c.nc.Write(rawPublish(1, responseTopicProperty("reply/to")))
		require.NoError(t, err)
		first, body := c.readRawPacket()
		assert.Equal(t, byte(0x40), first, "a PUBACK")
		assert.Equal(t, []byte{0x00, 0x01}, body[:2])
		got := listener.expectPublish()
		require.NotNil(t, got.Properties)
		assert.Equal(t, "reply/to", got.Properties.ResponseTopic)
	})
}

// The same property in a Will Message is the same statement: a Will is
// published by the Server later as an ordinary PUBLISH, so a wildcard accepted
// at CONNECT would be forwarded then. The CONNECT is refused with CONNACK 0x82
// (§3.2.2.2: "Data in the CONNECT packet does not conform to this
// specification") and the connection closed [MQTT-4.13.1-1].
func TestAWillWithAResponseTopicWithWildcardsIsRefusedAtConnect(t *testing.T) {
	addr := startBroker(t, startNATS(t))

	c := dialRaw(t, addr)
	typ, _, reason := c.handshake(rawConnectWithWill("rt-will", responseTopicProperty("reply/+")))
	assert.Equal(t, byte(0x20), typ, "a CONNACK")
	assert.Equal(t, byte(0x82), reason, "Protocol Error")
	c.expectClosed()

	ok := dialRaw(t, addr)
	typ, _, reason = ok.handshake(rawConnectWithWill("rt-will-ok", responseTopicProperty("reply/to")))
	assert.Equal(t, byte(0x20), typ)
	assert.Equal(t, byte(0x00), reason, "a Will whose Response Topic has no wildcard is accepted")
}
