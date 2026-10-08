package natsmqtt5_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/taumatix/natsmqtt5/packet"
)

// Statements, worded as in the OASIS MQTT 5.0 text read on 2026-10-08. Each
// says the sender "MUST NOT send this property if it would increase the size
// of the <packet> beyond the Maximum Packet Size specified by the receiver":
//   - [MQTT-3.2.2-19] Reason String on CONNACK, [MQTT-3.2.2-20] User Property
//   - [MQTT-3.4.2-2] Reason String on PUBACK, [MQTT-3.5.2-2] on PUBREC
//   - [MQTT-3.14.2-3] Reason String on DISCONNECT
//
// The broker only ever adds a Reason String to these packets (never a User
// Property, and nothing to PUBREL or PUBCOMP), so the User Property statements
// hold because there is nothing to send; the helper that drops the optional
// properties drops both. Each test fixes the client's limit below the size of
// the packet with its Reason String and compares the bytes received with the
// ones §3.2, §3.4, §3.5 and §3.14 lay out for the packet without it. Before the
// fix the whole packet was discarded and the client heard nothing.

// connectWithMaxPacketSize is a Clean Start CONNECT whose Maximum Packet Size
// (0x27) is limit.
func connectWithMaxPacketSize(id string, limit uint32) *packet.Connect {
	cp := rawConnect(id, 0)
	cp.CleanStart = true
	cp.Properties.MaximumPacketSize = packet.Uint32(limit)
	return cp
}

func (c *rawClient) expectRawBytes(want []byte) {
	c.t.Helper()
	first, body := c.readRawPacket()
	got := append([]byte{first, byte(len(body))}, body...)
	assert.Equal(c.t, want, got)
}

// [MQTT-3.2.2-19]: a refused CONNECT carries a Reason String; with a limit of
// exactly 5 bytes the CONNACK arrives without it: 0x20, Remaining Length 3,
// flags 0, Reason Code 0x8C (Bad authentication method), Property Length 0.
func TestAnOversizeConnackLosesItsReasonStringAndIsStillSent(t *testing.T) {
	addr := startBroker(t, startNATS(t))
	c := dialRaw(t, addr)
	cp := connectWithMaxPacketSize("oversize-connack", 5)
	cp.Properties.AuthenticationMethod = "SCRAM"
	c.send(cp)
	c.expectRawBytes([]byte{0x20, 0x03, 0x00, 0x8C, 0x00})
	c.expectClosed() // [MQTT-3.2.2-7]
}

// [MQTT-3.1.2-24] still holds when even the bare packet is too large: nothing
// is sent, and the connection closes.
func TestAConnackThatCannotFitIsStillNotSent(t *testing.T) {
	addr := startBroker(t, startNATS(t))
	c := dialRaw(t, addr)
	cp := connectWithMaxPacketSize("oversize-connack-bare", 4)
	cp.Properties.AuthenticationMethod = "SCRAM"
	c.send(cp)
	c.expectClosed()
}

// The limit of 60 admits the success CONNACK (24 bytes, no Reason String) and
// the bare acknowledgements, and excludes the ones that carry the 70-byte
// Reason String the broker gives a Topic Name with a wildcard.

// [MQTT-3.4.2-2]: PUBACK 0x40, Remaining Length 3, Packet Identifier 1, Reason
// Code 0x90 (Topic Name invalid), no Property Length (the short form, §3.4.2.1).
func TestAnOversizePubackLosesItsReasonStringAndIsStillSent(t *testing.T) {
	addr := startBroker(t, startNATS(t))
	c := dialRaw(t, addr)
	c.connect(connectWithMaxPacketSize("oversize-puback", 60))
	c.send(&packet.Publish{Topic: "a/+", QoS: packet.QoS1, PacketID: 1, Payload: []byte("x")})
	c.expectRawBytes([]byte{0x40, 0x03, 0x00, 0x01, 0x90})
}

// [MQTT-3.5.2-2]: the same for PUBREC, 0x50.
func TestAnOversizePubrecLosesItsReasonStringAndIsStillSent(t *testing.T) {
	addr := startBroker(t, startNATS(t))
	c := dialRaw(t, addr)
	c.connect(connectWithMaxPacketSize("oversize-pubrec", 60))
	c.send(&packet.Publish{Topic: "a/+", QoS: packet.QoS2, PacketID: 7, Payload: []byte("x")})
	c.expectRawBytes([]byte{0x50, 0x03, 0x00, 0x07, 0x90})
}

// [MQTT-3.14.2-3]: DISCONNECT 0xE0, Remaining Length 1, Reason Code 0x82
// (Protocol Error) without its Reason String, then the connection closes
// [MQTT-3.14.4-3].
func TestAnOversizeDisconnectLosesItsReasonStringAndIsStillSent(t *testing.T) {
	addr := startBroker(t, startNATS(t))
	c := dialRaw(t, addr)
	c.connect(connectWithMaxPacketSize("oversize-disconnect", 40))
	_, err := c.nc.Write(rawPublish(1, []byte{0x0B, 0x01})) // Subscription Identifier: a Protocol Error
	assert.NoError(t, err)
	c.expectRawBytes([]byte{0xE0, 0x01, 0x82})
	c.expectClosed()
}

// A packet that carries a Reason String and fits is sent whole.
func TestAPubackThatFitsKeepsItsReasonString(t *testing.T) {
	addr := startBroker(t, startNATS(t))
	c := dialRaw(t, addr)
	c.connect(connectWithMaxPacketSize("fits-puback", 1000))
	c.send(&packet.Publish{Topic: "a/+", QoS: packet.QoS1, PacketID: 1, Payload: []byte("x")})
	ack, ok := c.read().(*packet.Puback)
	assert.True(t, ok)
	assert.Equal(t, packet.TopicNameInvalid, ack.ReasonCode)
	assert.NotEmpty(t, ack.Properties.ReasonString)
}
