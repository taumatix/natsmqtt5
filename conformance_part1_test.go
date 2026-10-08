package natsmqtt5_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// Integration tests for the server-bound statements of MQTT 5.0 sections 1.5 to
// 3.2.2 (rows 2-85 of conformance/mqtt5-statements.tsv), per CONTRIBUTING.md,
// "Every statement has an integration test". Each test runs a real embedded NATS
// server, a real broker and a real TCP socket, and writes or judges the bytes
// itself where the statement is about the wire. The wording quoted is the OASIS
// MQTT 5.0 text (mqtt-v5.0-os.html) fetched on 2026-10-08.
//
// Statements that an existing test already proves are not repeated here; the
// audit report that accompanies this file says which test does.

// ---- Raw wire helpers ----------------------------------------------------

// vbiBytes is the Variable Byte Integer encoding of n (§1.5.5), written here so
// a test does not trust the module's own encoder.
func vbiBytes(n int) []byte {
	var out []byte
	for {
		b := byte(n % 128)
		n /= 128
		if n > 0 {
			b |= 0x80
		}
		out = append(out, b)
		if n == 0 {
			return out
		}
	}
}

// rawPacket assembles a control packet: first byte, Remaining Length, body.
func rawPacket(first byte, body ...[]byte) []byte {
	all := bytes.Join(body, nil)
	out := append([]byte{first}, vbiBytes(len(all))...)
	return append(out, all...)
}

// rawUTF8 is a two-byte length prefix followed by the bytes, whatever they are.
func rawUTF8(s string) []byte {
	return append([]byte{byte(len(s) >> 8), byte(len(s))}, s...)
}

// rawConnectBytes is a CONNECT with Keep Alive 30 and no properties. payload
// fields follow the variable header in the order given.
func rawConnectBytes(protocolName string, version, flags byte, payload ...[]byte) []byte {
	header := append(rawUTF8(protocolName), version, flags, 0x00, 0x1E, 0x00)
	return rawPacket(0x10, append([][]byte{header}, payload...)...)
}

// rawQoS0Publish is a QoS 0 PUBLISH with no properties.
func rawQoS0Publish(topic, payload string) []byte {
	return rawPacket(0x30, rawUTF8(topic), []byte{0x00}, []byte(payload))
}

// rawQoSPublish is a PUBLISH of QoS 1 or 2 with the given Packet Identifier.
func rawQoSPublish(qos byte, id uint16, topic, payload string) []byte {
	return rawPacket(0x30|qos<<1, rawUTF8(topic), []byte{byte(id >> 8), byte(id)}, []byte{0x00}, []byte(payload))
}

func (c *rawClient) write(b []byte) {
	c.t.Helper()
	require.NoError(c.t, c.nc.SetWriteDeadline(time.Now().Add(5*time.Second)))
	_, err := c.nc.Write(b)
	require.NoError(c.t, err)
}

// readFrame is readRawPacket that also reports how many bytes the Remaining
// Length took.
func (c *rawClient) readFrame() (first byte, body []byte, lenBytes int) {
	c.t.Helper()
	require.NoError(c.t, c.nc.SetReadDeadline(time.Now().Add(5*time.Second)))
	first, err := c.r.ReadByte()
	require.NoError(c.t, err)
	length, shift := 0, 0
	for {
		b, err := c.r.ReadByte()
		require.NoError(c.t, err)
		lenBytes++
		length |= int(b&0x7F) << shift
		if b&0x80 == 0 {
			break
		}
		shift += 7
	}
	body = make([]byte, length)
	_, err = io.ReadFull(c.r, body)
	require.NoError(c.t, err)
	return first, body, lenBytes
}

type rawFrame struct {
	first byte
	body  []byte
}

// drain reads every packet the broker sends until it closes the connection,
// failing if it does not close within five seconds.
func (c *rawClient) drain() []rawFrame {
	c.t.Helper()
	var frames []rawFrame
	for {
		require.NoError(c.t, c.nc.SetReadDeadline(time.Now().Add(5*time.Second)))
		first, err := c.r.ReadByte()
		if err != nil {
			var ne interface{ Timeout() bool }
			if errors.As(err, &ne) && ne.Timeout() {
				c.t.Fatalf("the broker left the connection open; frames so far: %v", frames)
			}
			return frames
		}
		length, shift := 0, 0
		for {
			b, err := c.r.ReadByte()
			require.NoError(c.t, err)
			length |= int(b&0x7F) << shift
			if b&0x80 == 0 {
				break
			}
			shift += 7
		}
		body := make([]byte, length)
		_, err = io.ReadFull(c.r, body)
		require.NoError(c.t, err)
		frames = append(frames, rawFrame{first, body})
	}
}

// expectRejectedAndClosed requires the broker to refuse whatever the client
// just sent and close the connection: nothing it sends before closing may be an
// acknowledgement, a Success CONNACK, or anything but a CONNACK with an error
// code or a DISCONNECT (the packets §4.13 allows for reporting an error).
func (c *rawClient) expectRejectedAndClosed() {
	c.t.Helper()
	for _, f := range c.drain() {
		switch f.first {
		case 0x20:
			require.GreaterOrEqual(c.t, len(f.body), 2)
			require.GreaterOrEqual(c.t, f.body[1], byte(0x80), "a CONNACK that refuses carries an error Reason Code")
			require.Zero(c.t, f.body[0]&0x01, "a refusing CONNACK has Session Present 0")
		case 0xE0:
			// DISCONNECT with the reason for the error.
		default:
			c.t.Fatalf("the broker answered with packet 0x%02X instead of refusing", f.first)
		}
	}
}

func startConformanceBroker(t *testing.T, customise ...func(*natsmqtt5.Options)) string {
	t.Helper()
	return startBroker(t, startNATS(t), customise...)
}

// ---- 1.5 Data representation ---------------------------------------------

// §1.5.4: "The character data in a UTF-8 Encoded String MUST be well-formed
// UTF-8 as defined by the Unicode specification [Unicode] and restated in RFC
// 3629 [RFC3629]. In particular, the character data MUST NOT include encodings
// of code points between U+D800 and U+DFFF" [MQTT-1.5.4-1].
//
// A surrogate (0xED 0xA0 0x80 is U+D800) is refused in the Client Identifier of
// a CONNECT and in the Topic Name of a PUBLISH, and the message does not reach a
// subscriber.
func TestUTF8String_MQTT_1_5_4_1(t *testing.T) {
	addr := startConformanceBroker(t)

	c := dialRaw(t, addr)
	c.write(rawConnectBytes("MQTT", 5, 0x02, rawUTF8("a\xED\xA0\x80b")))
	c.expectRejectedAndClosed()

	sub, _ := connectClient(t, addr, connectOpts("utf8-sub"))
	sub.subscribe(paho.SubscribeOptions{Topic: "#", QoS: 0})
	pub, _ := sessionPresentOnConnect(t, addr, "utf8-pub", true, 0)
	pub.write(rawQoS0Publish("t/\xED\xA0\x80", "surrogate"))
	pub.expectRejectedAndClosed()
	sub.expectNoMessage()
}

// §1.5.4: "A UTF-8 Encoded String MUST NOT include an encoding of the null
// character U+0000." [MQTT-1.5.4-2]
func TestUTF8String_MQTT_1_5_4_2(t *testing.T) {
	addr := startConformanceBroker(t)

	c := dialRaw(t, addr)
	c.write(rawConnectBytes("MQTT", 5, 0x02, rawUTF8("a\x00b")))
	c.expectRejectedAndClosed()

	sub, _ := connectClient(t, addr, connectOpts("null-sub"))
	sub.subscribe(paho.SubscribeOptions{Topic: "#", QoS: 0})
	pub, _ := sessionPresentOnConnect(t, addr, "null-pub", true, 0)
	pub.write(rawQoS0Publish("t/a\x00b", "null"))
	pub.expectRejectedAndClosed()
	sub.expectNoMessage()
}

// §1.5.4: "A UTF-8 encoded sequence 0xEF 0xBB 0xBF is always interpreted as
// U+FEFF ("ZERO WIDTH NO-BREAK SPACE") wherever it appears in a string and MUST
// NOT be skipped over or stripped off by a packet receiver" [MQTT-1.5.4-3].
//
// A Topic Name carrying the sequence reaches the subscriber with it intact.
func TestUTF8String_MQTT_1_5_4_3(t *testing.T) {
	addr := startConformanceBroker(t)

	sub, _ := connectClient(t, addr, connectOpts("bom-sub"))
	sub.subscribe(paho.SubscribeOptions{Topic: "bom/+", QoS: 1})

	pub, _ := sessionPresentOnConnect(t, addr, "bom-pub", true, 0)
	pub.write(rawQoSPublish(1, 7, "bom/\xEF\xBB\xBFx", "p"))
	first, _, _ := pub.readFrame()
	require.Equal(t, byte(0x40), first, "PUBACK")

	got := sub.expectMessage()
	assert.Equal(t, []byte("bom/\xEF\xBB\xBFx"), []byte(got.Topic), "the byte order mark is part of the Topic Name")
}

// §1.5.5: "The encoded value MUST use the minimum number of bytes necessary to
// represent the value" [MQTT-1.5.5-1].
//
// The Remaining Length of a PUBLISH the broker sends is a Variable Byte Integer;
// the boundaries 127, 128, 16383 and 16384 take 1, 2, 2 and 3 bytes.
func TestVariableByteInteger_MQTT_1_5_5_1(t *testing.T) {
	addr := startConformanceBroker(t)
	sub := dialRaw(t, addr)
	sub.connect(rawConnect("vbi-sub", 0))
	sub.subscribe("vbi/t", packet.QoS0)
	pub, _ := connectClient(t, addr, connectOpts("vbi-pub"))

	// Remaining Length of a QoS 0 PUBLISH to "vbi/t": 2+5 (topic) + 1 (property
	// length) + payload.
	for _, tc := range []struct{ remaining, lenBytes int }{
		{127, 1}, {128, 2}, {16383, 2}, {16384, 3},
	} {
		pub.publish(&paho.Publish{Topic: "vbi/t", QoS: 0, Payload: bytes.Repeat([]byte("x"), tc.remaining-8)})
		first, body, lenBytes := sub.readFrame()
		assert.Equal(t, byte(0x30), first)
		assert.Equal(t, tc.remaining, len(body))
		assert.Equal(t, tc.lenBytes, lenBytes, "Remaining Length %d takes %d byte(s)", tc.remaining, tc.lenBytes)
	}
}

// §1.5.7: "Both strings MUST comply with the requirements for UTF-8 Encoded
// Strings" [MQTT-1.5.7-1]. A User Property whose value is an encoded surrogate is
// malformed, and the PUBLISH carrying it is not delivered.
func TestUTF8StringPair_MQTT_1_5_7_1(t *testing.T) {
	addr := startConformanceBroker(t)
	sub, _ := connectClient(t, addr, connectOpts("pair-sub"))
	sub.subscribe(paho.SubscribeOptions{Topic: "pair/#", QoS: 0})

	pub, _ := sessionPresentOnConnect(t, addr, "pair-pub", true, 0)
	props := append([]byte{0x26}, rawUTF8("k")...)
	props = append(props, rawUTF8("v\xED\xA0\x80")...)
	pub.write(rawPacket(0x30, rawUTF8("pair/t"), vbiBytes(len(props)), props, []byte("x")))
	pub.expectRejectedAndClosed()
	sub.expectNoMessage()
}

// ---- 2 Control packet format ---------------------------------------------

// §2.1.3: "Where a flag bit is marked as "Reserved", it is reserved for future
// use and MUST be set to the value listed" [MQTT-2.1.3-1]. A receiver that finds
// a reserved bit wrong treats the packet as malformed (§2.1.3, §4.13).
func TestFixedHeaderFlags_MQTT_2_1_3_1(t *testing.T) {
	addr := startConformanceBroker(t)

	t.Run("CONNECT with flag bits 0001", func(t *testing.T) {
		c := dialRaw(t, addr)
		pkt := rawConnectBytes("MQTT", 5, 0x02, rawUTF8("flags-connect"))
		pkt[0] = 0x11
		c.write(pkt)
		c.expectRejectedAndClosed()
	})

	for name, bad := range map[string][]byte{
		"SUBSCRIBE with flag bits 0000":   rawPacket(0x80, []byte{0, 1, 0}, rawUTF8("a"), []byte{0}),
		"UNSUBSCRIBE with flag bits 0000": rawPacket(0xA0, []byte{0, 1, 0}, rawUTF8("a")),
		"PUBREL with flag bits 0000":      rawPacket(0x60, []byte{0, 1}),
		"PUBACK with flag bits 0001":      rawPacket(0x41, []byte{0, 1}),
		"PINGREQ with flag bits 0001":     {0xC1, 0x00},
	} {
		t.Run(name, func(t *testing.T) {
			c, _ := sessionPresentOnConnect(t, addr, "flags-"+strings.Fields(name)[0], true, 0)
			c.write(bad)
			c.expectRejectedAndClosed()
		})
	}

	// The well-formed counterpart is answered, so the cases above fail for the
	// flags and not for something else about the exchange.
	c, _ := sessionPresentOnConnect(t, addr, "flags-good", true, 0)
	c.write([]byte{0xC0, 0x00})
	first, _, _ := c.readFrame()
	assert.Equal(t, byte(0xD0), first, "PINGRESP to a well-formed PINGREQ")
}

// §2.2.1: "A PUBLISH packet MUST NOT contain a Packet Identifier if its QoS value
// is set to 0" [MQTT-2.2.1-2]. A QoS 0 PUBLISH from the broker to a subscriber
// that asked for QoS 1 is, byte for byte: fixed header 0x30, topic, Property
// Length 0, payload; no identifier between topic and properties.
func TestPacketIdentifier_MQTT_2_2_1_2(t *testing.T) {
	addr := startConformanceBroker(t)
	sub := dialRaw(t, addr)
	sub.connect(rawConnect("pid-sub", 0))
	sub.subscribe("pid/t", packet.QoS1)
	pub, _ := connectClient(t, addr, connectOpts("pid-pub"))
	pub.publish(&paho.Publish{Topic: "pid/t", QoS: 0, Payload: []byte("x")})

	first, body, _ := sub.readFrame()
	assert.Equal(t, byte(0x30), first)
	assert.Equal(t, append(append(rawUTF8("pid/t"), 0x00), 'x'), body)
}

// §2.2.1: "Each time a Server sends a new PUBLISH (with QoS > 0) MQTT Control
// Packet it MUST assign it a non zero Packet Identifier that is currently
// unused" [MQTT-2.2.1-4]. Five messages are outstanding at once; their
// identifiers are non-zero and pairwise different. (withdrawn_expiry_test.go
// proves the same statement across a resumption.)
func TestPacketIdentifier_MQTT_2_2_1_4(t *testing.T) {
	addr := startConformanceBroker(t)
	sub := dialRaw(t, addr)
	sub.connect(rawConnect("pid4-sub", 0))
	sub.subscribe("pid4/t", packet.QoS1)
	pub, _ := connectClient(t, addr, connectOpts("pid4-pub"))
	for i := 0; i < 5; i++ {
		pub.publish(&paho.Publish{Topic: "pid4/t", QoS: 1, Payload: []byte{byte('a' + i)}})
	}

	seen := map[uint16]bool{}
	for i := 0; i < 5; i++ {
		p := sub.expectPublish() // never acknowledged, so every identifier stays in use
		assert.NotZero(t, p.PacketID)
		assert.False(t, seen[p.PacketID], "identifier %d is already in use", p.PacketID)
		seen[p.PacketID] = true
	}
}

// §2.2.1: "A PUBACK, PUBREC , PUBREL, or PUBCOMP packet MUST contain the same
// Packet Identifier as the PUBLISH packet that was originally sent"
// [MQTT-2.2.1-5].
//
// Inbound: the PUBACK to a QoS 1 PUBLISH and the PUBREC and PUBCOMP of a QoS 2
// exchange carry the identifier the client chose. Outbound: the PUBREL the broker
// sends for a QoS 2 delivery carries the identifier of its PUBLISH.
func TestPacketIdentifier_MQTT_2_2_1_5(t *testing.T) {
	addr := startConformanceBroker(t)

	c, _ := sessionPresentOnConnect(t, addr, "ack-ids", true, 0)
	c.write(rawQoSPublish(1, 0x1234, "ack/one", "p"))
	first, body, _ := c.readFrame()
	assert.Equal(t, byte(0x40), first, "PUBACK")
	assert.Equal(t, []byte{0x12, 0x34}, body[:2])

	c.write(rawQoSPublish(2, 0x2345, "ack/two", "p"))
	first, body, _ = c.readFrame()
	assert.Equal(t, byte(0x50), first, "PUBREC")
	assert.Equal(t, []byte{0x23, 0x45}, body[:2])
	c.write(rawPacket(0x62, []byte{0x23, 0x45}))
	first, body, _ = c.readFrame()
	assert.Equal(t, byte(0x70), first, "PUBCOMP")
	assert.Equal(t, []byte{0x23, 0x45}, body[:2])

	sub := dialRaw(t, addr)
	sub.connect(rawConnect("ack-ids-sub", 0))
	sub.subscribe("ack/out", packet.QoS2)
	pub, _ := connectClient(t, addr, connectOpts("ack-ids-pub"))
	pub.publish(&paho.Publish{Topic: "ack/out", QoS: 2, Payload: []byte("q2")})
	p := sub.expectPublish()
	require.Equal(t, packet.QoS2, p.QoS)
	sub.send(&packet.Pubrec{Ack: packet.Ack{PacketID: p.PacketID}})
	assert.Equal(t, p.PacketID, sub.expectPubrel().PacketID, "the PUBREL echoes the PUBLISH's identifier")
}

// §2.2.1: "A SUBACK and UNSUBACK MUST contain the Packet Identifier that was used
// in the corresponding SUBSCRIBE and UNSUBSCRIBE packet respectively"
// [MQTT-2.2.1-6]. Also [MQTT-2.2.2-1]: the properties of both are absent, which
// the exact bytes show as a Property Length of zero.
func TestPacketIdentifier_MQTT_2_2_1_6(t *testing.T) {
	addr := startConformanceBroker(t)
	c, _ := sessionPresentOnConnect(t, addr, "sub-ids", true, 0)

	c.write(rawSubscribe(0xBEEF, "s/a", byte(0)))
	first, body, _ := c.readFrame()
	assert.Equal(t, byte(0x90), first, "SUBACK")
	assert.Equal(t, []byte{0xBE, 0xEF, 0x00, 0x00}, body, "identifier, Property Length 0, granted QoS 0")

	c.write(rawPacket(0xA2, []byte{0xCA, 0xFE, 0x00}, rawUTF8("s/a")))
	first, body, _ = c.readFrame()
	assert.Equal(t, byte(0xB0), first, "UNSUBACK")
	assert.Equal(t, []byte{0xCA, 0xFE, 0x00, 0x00}, body, "identifier, Property Length 0, Success")
}

// §2.2.2: "If there are no properties, this MUST be indicated by including a
// Property Length of zero" [MQTT-2.2.2-1]. Packets the broker sends that carry
// none, judged as bytes: SUBACK and UNSUBACK (above, in the _2_2_1_6 test), a
// QoS 0 PUBLISH (in _2_2_1_2), and here the CONNACK of a minimal CONNECT is
// parsed to its end, and a PUBLISH delivered at QoS 1.
func TestPropertyLength_MQTT_2_2_2_1(t *testing.T) {
	addr := startConformanceBroker(t)
	sub := dialRaw(t, addr)
	sub.connect(rawConnect("proplen-sub", 0))
	sub.subscribe("proplen/t", packet.QoS1)
	pub, _ := connectClient(t, addr, connectOpts("proplen-pub"))
	pub.publish(&paho.Publish{Topic: "proplen/t", QoS: 1, Payload: []byte("x")})

	first, body, _ := sub.readFrame()
	assert.Equal(t, byte(0x32), first)
	topicEnd := 2 + len("proplen/t")
	require.Greater(t, len(body), topicEnd+2)
	assert.Equal(t, byte(0x00), body[topicEnd+2], "after topic and Packet Identifier comes Property Length 0")
	assert.Equal(t, []byte("x"), body[topicEnd+3:])
}

// ---- 3.1 CONNECT ---------------------------------------------------------

// §3.1: "The Server MUST process a second CONNECT packet sent from a Client as a
// Protocol Error and close the Network Connection" [MQTT-3.1.0-2]. The answer, if
// any, is a DISCONNECT with 0x82 (§4.13); never a second CONNACK [MQTT-3.2.0-2].
func TestConnect_MQTT_3_1_0_2(t *testing.T) {
	addr := startConformanceBroker(t)
	c, _ := sessionPresentOnConnect(t, addr, "twice", true, 0)

	c.write(connectPacketBytes("twice", true, 0))
	frames := c.drain()
	for _, f := range frames {
		require.Equal(t, byte(0xE0), f.first, "only a DISCONNECT may follow a second CONNECT")
		require.NotEmpty(t, f.body)
		assert.Equal(t, byte(0x82), f.body[0], "Protocol Error")
	}
}

// §3.2: "The Server MUST NOT send more than one CONNACK in a Network Connection"
// [MQTT-3.2.0-2]. One CONNACK answers the CONNECT; a second CONNECT, the
// Protocol Error, and a SUBSCRIBE sent in between draw none.
func TestConnack_MQTT_3_2_0_2(t *testing.T) {
	addr := startConformanceBroker(t)
	c, _ := sessionPresentOnConnect(t, addr, "one-connack", true, 0)

	c.write(rawSubscribe(1, "x/y", byte(0)))
	first, _, _ := c.readFrame()
	assert.Equal(t, byte(0x90), first, "SUBACK, not a CONNACK")

	c.write(connectPacketBytes("one-connack", true, 0))
	for _, f := range c.drain() {
		assert.NotEqual(t, byte(0x20), f.first, "no second CONNACK")
	}
}

// §3.2: "The Server MUST send a CONNACK with a 0x00 (Success) Reason Code before
// sending any Packet other than AUTH" [MQTT-3.2.0-1]. A session resumed with
// messages waiting in its offline queue is the case where something else could
// get in first: the first packet on the wire is still the CONNACK.
func TestConnack_MQTT_3_2_0_1(t *testing.T) {
	addr := startConformanceBroker(t)
	c, _ := sessionPresentOnConnect(t, addr, "connack-first", false, 300)
	c.subscribe("first/#", packet.QoS1)
	c.write(rawDisconnect)

	pub, _ := connectClient(t, addr, connectOpts("connack-first-pub"))
	for i := 0; i < 3; i++ {
		pub.publish(&paho.Publish{Topic: "first/t", QoS: 1, Payload: []byte{byte('0' + i)}})
	}

	back := dialRaw(t, addr)
	back.write(connectPacketBytes("connack-first", false, 300))
	first, body, _ := back.readFrame()
	require.Equal(t, byte(0x20), first, "the first packet is the CONNACK")
	assert.Equal(t, []byte{0x01, 0x00}, body[:2], "Session Present 1, Success")
	first, _, _ = back.readFrame()
	assert.Equal(t, byte(0x32), first, "the queued PUBLISH follows the CONNACK")
}

// §3.1.2.2: "If the Protocol Version is not 5 and the Server does not want to
// accept the CONNECT packet, the Server MAY send a CONNACK packet with Reason
// Code 0x84 (Unsupported Protocol Version) and then MUST close the Network
// Connection" [MQTT-3.1.2-2].
func TestConnect_MQTT_3_1_2_2(t *testing.T) {
	addr := startConformanceBroker(t)
	for _, version := range []byte{3, 4, 6} {
		c := dialRaw(t, addr)
		c.write(rawConnectBytes("MQTT", version, 0x02, rawUTF8("version")))
		frames := c.drain()
		for _, f := range frames {
			require.Equal(t, byte(0x20), f.first)
			assert.Equal(t, byte(0x84), f.body[1], "Unsupported Protocol Version for version %d", version)
		}
	}
}

// §3.1.2.3: "The Server MUST validate that the reserved flag in the CONNECT
// packet is set to 0" [MQTT-3.1.2-3]. Bit 0 of the Connect Flags set is a
// Malformed Packet.
func TestConnect_MQTT_3_1_2_3(t *testing.T) {
	addr := startConformanceBroker(t)
	c := dialRaw(t, addr)
	c.write(rawConnectBytes("MQTT", 5, 0x03, rawUTF8("reserved")))
	c.expectRejectedAndClosed()
}

// §3.1.2.4: "If a CONNECT packet is received with Clean Start set to 0 and there
// is no Session associated with the Client Identifier, the Server MUST create a
// new Session" [MQTT-3.1.2-6]. The session is new (Session Present 0), and it is
// a session: the next Clean Start 0 CONNECT for the identifier resumes it.
func TestCleanStart_MQTT_3_1_2_6(t *testing.T) {
	addr := startConformanceBroker(t)

	first, present := sessionPresentOnConnect(t, addr, "fresh-6", false, 300)
	assert.False(t, present, "no session existed")
	first.write(rawDisconnect)

	_, present = sessionPresentOnConnect(t, addr, "fresh-6", false, 300)
	assert.True(t, present, "the session created by the first CONNECT is there")
}

// §3.1.2.7: "If the Will Flag is set to 1 and Will Retain is set to 0, the Server
// MUST publish the Will Message as a non-retained message" [MQTT-3.1.2-14].
// A subscriber that was present sees the Will; one that subscribes after it does
// not, because nothing was retained.
func TestWillRetain_MQTT_3_1_2_14(t *testing.T) {
	addr := startConformanceBroker(t)
	live, _ := connectClient(t, addr, connectOpts("will14-live"))
	live.subscribe(paho.SubscribeOptions{Topic: "will14/t", QoS: 1})

	cp := connectOpts("will14-dying")
	cp.WillMessage = &paho.WillMessage{Topic: "will14/t", Payload: []byte("gone"), QoS: 1, Retain: false}
	dying, _ := connectClient(t, addr, cp)
	dying.dropConnection()

	got := live.expectMessage()
	assert.Equal(t, "gone", got.Payload)
	assert.False(t, got.Retain)

	late, _ := connectClient(t, addr, connectOpts("will14-late"))
	late.subscribe(paho.SubscribeOptions{Topic: "will14/t", QoS: 1})
	late.expectNoMessage()
}

// §3.1.2.7: "If the Will Flag is set to 1 and Will Retain is set to 1, the Server
// MUST publish the Will Message as a retained message" [MQTT-3.1.2-15]. A
// subscriber that subscribes after the Will was published is sent it, flagged
// retained.
func TestWillRetain_MQTT_3_1_2_15(t *testing.T) {
	addr := startConformanceBroker(t)
	live, _ := connectClient(t, addr, connectOpts("will15-live"))
	live.subscribe(paho.SubscribeOptions{Topic: "will15/t", QoS: 1})

	cp := connectOpts("will15-dying")
	cp.WillMessage = &paho.WillMessage{Topic: "will15/t", Payload: []byte("gone"), QoS: 1, Retain: true}
	dying, _ := connectClient(t, addr, cp)
	dying.dropConnection()
	require.Equal(t, "gone", live.expectMessage().Payload)

	late, _ := connectClient(t, addr, connectOpts("will15-late"))
	late.subscribe(paho.SubscribeOptions{Topic: "will15/t", QoS: 1})
	got := late.expectMessage()
	assert.Equal(t, "gone", got.Payload)
	assert.True(t, got.Retain, "delivered from the retained store, so the RETAIN flag is 1")
}

// §3.1.2.11.6: "A value of 0 indicates that the Server MUST NOT return Response
// Information" [MQTT-3.1.2-28]. (A value of 1 only lets the Server choose.)
func TestRequestResponseInformation_MQTT_3_1_2_28(t *testing.T) {
	addr := startConformanceBroker(t)
	c := dialRaw(t, addr)
	cp := rawConnect("no-resp-info", 0)
	cp.Properties.RequestResponseInfo = packet.Byte(0)
	ack := c.connect(cp)
	if ack.Properties != nil {
		assert.Empty(t, ack.Properties.ResponseInformation)
	}
}

// §3.1.2.11.7: "If the value of Request Problem Information is 0, the Server MAY
// return a Reason String or User Properties on a CONNACK or DISCONNECT packet,
// but MUST NOT send a Reason String or User Properties on any packet other than
// PUBLISH, CONNACK, or DISCONNECT" [MQTT-3.1.2-29].
//
// An Authorizer denies a topic and a wildcard Topic Name is invalid, so a PUBACK,
// a PUBREC, a SUBACK and an UNSUBACK that could explain a refusal are provoked.
// With the default (1) the PUBACK for the invalid Topic Name carries a Reason
// String, so the test would notice if the strip were what hid it; with 0 none of
// them carries a Reason String or a User Property.
func TestRequestProblemInformation_MQTT_3_1_2_29(t *testing.T) {
	addr := startConformanceBroker(t, func(o *natsmqtt5.Options) {
		o.Authorizer = natsmqtt5.AuthorizerFunc(func(_ context.Context, req *natsmqtt5.AuthzRequest) error {
			if strings.HasPrefix(req.Topic, "deny/") {
				return errors.New("not for you")
			}
			return nil
		})
	})

	noExplanation := func(t *testing.T, props *packet.Properties) {
		t.Helper()
		if props != nil {
			assert.Empty(t, props.ReasonString)
			assert.Empty(t, props.User)
		}
	}
	connectWith := func(id string, rpi *byte) *rawClient {
		c := dialRaw(t, addr)
		cp := rawConnect(id, 0)
		cp.Properties.RequestProblemInfo = rpi
		c.connect(cp)
		return c
	}

	t.Run("default 1 explains", func(t *testing.T) {
		c := connectWith("rpi-default", nil)
		c.send(&packet.Publish{Topic: "wild/+", QoS: packet.QoS1, PacketID: 1, Payload: []byte("x")})
		ack, ok := c.read().(*packet.Puback)
		require.True(t, ok)
		assert.Equal(t, packet.TopicNameInvalid, ack.ReasonCode)
		require.NotNil(t, ack.Properties)
		assert.NotEmpty(t, ack.Properties.ReasonString)
	})

	t.Run("0 does not", func(t *testing.T) {
		c := connectWith("rpi-zero", packet.Byte(0))

		c.send(&packet.Publish{Topic: "deny/x", QoS: packet.QoS1, PacketID: 1, Payload: []byte("x")})
		puback, ok := c.read().(*packet.Puback)
		require.True(t, ok)
		assert.Equal(t, packet.NotAuthorized, puback.ReasonCode)
		noExplanation(t, puback.Properties)
		c.send(&packet.Publish{Topic: "wild/+", QoS: packet.QoS1, PacketID: 5, Payload: []byte("x")})
		puback, ok = c.read().(*packet.Puback)
		require.True(t, ok)
		assert.Equal(t, packet.TopicNameInvalid, puback.ReasonCode)
		noExplanation(t, puback.Properties)

		c.send(&packet.Publish{Topic: "deny/x", QoS: packet.QoS2, PacketID: 2, Payload: []byte("x")})
		pubrec, ok := c.read().(*packet.Pubrec)
		require.True(t, ok)
		assert.Equal(t, packet.NotAuthorized, pubrec.ReasonCode)
		noExplanation(t, pubrec.Properties)

		c.send(&packet.Subscribe{PacketID: 3, Subscriptions: []packet.Subscription{{Filter: "deny/#", QoS: packet.QoS1}}})
		suback, ok := c.read().(*packet.Suback)
		require.True(t, ok)
		assert.Equal(t, []packet.ReasonCode{packet.NotAuthorized}, suback.ReasonCodes)
		noExplanation(t, suback.Properties)

		c.send(&packet.Unsubscribe{PacketID: 4, Filters: []string{"never/subscribed"}})
		unsuback, ok := c.read().(*packet.Unsuback)
		require.True(t, ok)
		noExplanation(t, unsuback.Properties)
	})
}

// ---- 3.1.3 Payload -------------------------------------------------------

// §3.1.3.1: "The ClientID MUST be used by Clients and by Servers to identify
// state that they hold relating to this MQTT Session between the Client and the
// Server" [MQTT-3.1.3-2]. Two Client Identifiers hold two sessions: what is
// queued for one is not delivered to, or resumed by, the other.
func TestClientID_MQTT_3_1_3_2(t *testing.T) {
	addr := startConformanceBroker(t)

	a, _ := sessionPresentOnConnect(t, addr, "owner-A", false, 300)
	a.subscribe("owner/#", packet.QoS1)
	a.write(rawDisconnect)

	pub, _ := connectClient(t, addr, connectOpts("owner-pub"))
	pub.publish(&paho.Publish{Topic: "owner/t", QoS: 1, Payload: []byte("for A")})

	b, present := sessionPresentOnConnect(t, addr, "owner-B", false, 300)
	assert.False(t, present, "B has no session of its own yet, and A's is not B's")
	b.expectNothing()

	backA := dialRaw(t, addr)
	backA.write(connectPacketBytes("owner-A", false, 300))
	first, body, _ := backA.readFrame()
	require.Equal(t, byte(0x20), first)
	assert.Equal(t, byte(0x01), body[0], "A's session is present")
	got := backA.expectPublish()
	assert.Equal(t, "for A", string(got.Payload))
}

// §3.1.3.1: "The Server MUST allow ClientID's which are between 1 and 23 UTF-8
// encoded bytes in length, and that contain only the characters
// "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
// [MQTT-3.1.3-5]. Persistent sessions are on, which is the configuration that
// limits Client Identifier length (128 bytes), and the longest permitted
// identifiers use the whole character set between them.
func TestClientID_MQTT_3_1_3_5(t *testing.T) {
	addr := startConformanceBroker(t, persistent)
	const set = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	for _, id := range []string{"a", set[:23], set[23:46], set[46:] + "0123456", "0"} {
		require.LessOrEqual(t, len(id), 23)
		c, _ := sessionPresentOnConnect(t, addr, id, true, 0)
		c.write(rawDisconnect)
	}
}

// §3.1.3.1: "If the Server rejects the ClientID it MAY respond to the CONNECT
// packet with a CONNACK using Reason Code 0x85 (Client Identifier not valid) as
// described in section 4.13 Handling errors, and then it MUST close the Network
// Connection" [MQTT-3.1.3-8]. With persistent sessions the broker rejects an
// identifier longer than MaxPersistentClientIDLen.
func TestClientID_MQTT_3_1_3_8(t *testing.T) {
	addr := startConformanceBroker(t, persistent)
	c := dialRaw(t, addr)
	typ, flags, reason := c.handshake(rawConnectBytes("MQTT", 5, 0x02, rawUTF8(strings.Repeat("x", natsmqtt5.MaxPersistentClientIDLen+1))))
	assert.Equal(t, byte(0x20), typ)
	assert.Zero(t, flags)
	assert.Equal(t, byte(0x85), reason)
	c.expectClosed()
}

// §3.1.3.2.8 / §3.1.3: "The Server MUST maintain the order of User Properties
// when publishing the Will Message" [MQTT-3.1.3-10]. The same key may repeat.
func TestWillProperties_MQTT_3_1_3_10(t *testing.T) {
	addr := startConformanceBroker(t)
	live, _ := connectClient(t, addr, connectOpts("will310-live"))
	live.subscribe(paho.SubscribeOptions{Topic: "will310/t", QoS: 1})

	cp := connectOpts("will310-dying")
	cp.WillMessage = &paho.WillMessage{Topic: "will310/t", Payload: []byte("gone"), QoS: 1}
	cp.WillProperties = &paho.WillProperties{User: paho.UserProperties{
		{Key: "a", Value: "1"}, {Key: "b", Value: "2"}, {Key: "a", Value: "3"}, {Key: "c", Value: "4"},
	}}
	dying, _ := connectClient(t, addr, cp)
	dying.dropConnection()

	got := live.expectMessage()
	require.NotNil(t, got.Properties)
	assert.Equal(t, paho.UserProperties{
		{Key: "a", Value: "1"}, {Key: "b", Value: "2"}, {Key: "a", Value: "3"}, {Key: "c", Value: "4"},
	}, got.Properties.User)
}

// ---- 3.1.4 CONNECT actions -----------------------------------------------

// §3.1.4: "The Server MUST validate that the CONNECT packet matches the format
// described in section 3.1 and close the Network Connection if it does not
// match" [MQTT-3.1.4-1].
func TestConnectActions_MQTT_3_1_4_1(t *testing.T) {
	addr := startConformanceBroker(t)
	good := rawConnectBytes("MQTT", 5, 0x02, rawUTF8("fmt"))

	short := rawPacket(0x10, append(rawUTF8("MQTT"), 5, 0x02)) // ends after the flags
	trailing := rawPacket(0x10, good[2:len(good)], []byte{0xFF})
	willWithoutFields := rawConnectBytes("MQTT", 5, 0x06, rawUTF8("fmt")) // Will Flag, no Will fields
	propsPastEnd := rawPacket(0x10, rawUTF8("MQTT"), []byte{5, 0x02, 0x00, 0x1E, 0x7F}, rawUTF8("fmt"))
	passwordFlagNoField := rawConnectBytes("MQTT", 5, 0x42, rawUTF8("fmt"))

	for name, pkt := range map[string][]byte{
		"truncated variable header": short,
		"bytes after the payload":   trailing,
		"Will Flag without a Will":  willWithoutFields,
		"Property Length too long":  propsPastEnd,
		"Password Flag, no field":   passwordFlagNoField,
	} {
		t.Run(name, func(t *testing.T) {
			c := dialRaw(t, addr)
			c.write(pkt)
			c.expectRejectedAndClosed()
		})
	}
}

// §3.1.4: "The Server MAY check that the contents of the CONNECT packet meet any
// further restrictions and SHOULD perform authentication and authorization
// checks. If any of these checks fail, it MUST close the Network Connection"
// [MQTT-3.1.4-2].
func TestConnectActions_MQTT_3_1_4_2(t *testing.T) {
	addr := startConformanceBroker(t, func(o *natsmqtt5.Options) {
		o.Authenticator = natsmqtt5.AuthenticatorFunc(func(_ context.Context, req *natsmqtt5.AuthRequest) (*natsmqtt5.AuthResult, error) {
			if req.Username != "good" {
				return nil, errors.New("no")
			}
			return nil, nil
		})
	})

	bad := dialRaw(t, addr)
	bad.send(&packet.Connect{ClientID: "auth-bad", CleanStart: true, KeepAlive: 30, Username: "bad", Password: []byte("pw")})
	bad.expectRejectedAndClosed()

	ok := dialRaw(t, addr)
	ack := ok.connect(&packet.Connect{ClientID: "auth-good", CleanStart: true, KeepAlive: 30, Username: "good", Password: []byte("pw")})
	assert.Equal(t, packet.Success, ack.ReasonCode)
}

// §3.1.4: "The Server MUST perform the processing of Clean Start that is
// described in section 3.1.2.4" [MQTT-3.1.4-4]. A CONNECT with Clean Start 1
// discards the session that was there: the subscription it held is gone from the
// session that replaces it.
func TestConnectActions_MQTT_3_1_4_4(t *testing.T) {
	addr := startConformanceBroker(t)

	old, _ := sessionPresentOnConnect(t, addr, "cleanstart-4", false, 300)
	old.subscribe("cs4/t", packet.QoS1)
	old.write(rawDisconnect)

	fresh, present := sessionPresentOnConnect(t, addr, "cleanstart-4", true, 300)
	require.False(t, present, "Clean Start 1 never reports a present session [MQTT-3.2.2-2]")
	fresh.write(rawDisconnect)

	resumed, present := sessionPresentOnConnect(t, addr, "cleanstart-4", false, 300)
	require.True(t, present, "the session the Clean Start created is kept")
	pub, _ := connectClient(t, addr, connectOpts("cleanstart-4-pub"))
	pub.publish(&paho.Publish{Topic: "cs4/t", QoS: 1, Payload: []byte("old subscription")})
	resumed.expectNothing()
}

// §3.1.4: "The Server MUST acknowledge the CONNECT packet with a CONNACK packet
// containing a 0x00 (Success) Reason Code" [MQTT-3.1.4-5]. A valid CONNECT,
// written byte by byte, is answered with a CONNACK, Success.
func TestConnectActions_MQTT_3_1_4_5(t *testing.T) {
	addr := startConformanceBroker(t)
	c := dialRaw(t, addr)
	typ, flags, reason := c.handshake(rawConnectBytes("MQTT", 5, 0x02, rawUTF8("ack-ok")))
	assert.Equal(t, byte(0x20), typ)
	assert.Zero(t, flags, "Clean Start 1: Session Present 0")
	assert.Zero(t, reason, "Success")
}

// §3.1.4: "If the Server rejects the CONNECT, it MUST NOT process any data sent
// by the Client after the CONNECT packet except AUTH packets" [MQTT-3.1.4-6].
// The refused CONNECT and a PUBLISH go out in one write, so the PUBLISH is
// already in the broker's buffer when the refusal happens; a subscriber never
// sees it.
func TestConnectActions_MQTT_3_1_4_6(t *testing.T) {
	natsURL := startNATS(t)
	addr := startBroker(t, natsURL, func(o *natsmqtt5.Options) {
		o.Authenticator = natsmqtt5.AuthenticatorFunc(func(context.Context, *natsmqtt5.AuthRequest) (*natsmqtt5.AuthResult, error) {
			return nil, errors.New("refused")
		})
	})
	// The refusing Authenticator would refuse a subscriber too, so watch NATS
	// itself: a processed PUBLISH would appear under the broker's subject prefix.
	published := natsTap(t, natsURL)

	var buf bytes.Buffer
	require.NoError(t, packet.Write(&buf, &packet.Connect{ClientID: "refused", CleanStart: true, KeepAlive: 30}))
	buf.Write(rawQoSPublish(1, 9, "after/refusal", "must not be processed"))

	c := dialRaw(t, addr)
	c.write(buf.Bytes())
	for _, f := range c.drain() {
		assert.NotEqual(t, byte(0x40), f.first, "no PUBACK: the PUBLISH was not processed")
	}
	assert.Empty(t, published(), "nothing reached NATS")
}

// ---- 3.2 CONNACK ---------------------------------------------------------

// §3.2.2.2: "If a Server sends a CONNACK packet containing a non-zero Reason Code
// it MUST set Session Present to 0" [MQTT-3.2.2-6]. The Client Identifier has a
// session, a Clean Start 0 CONNECT would resume it, and a refusal still answers
// with Session Present 0.
func TestConnack_MQTT_3_2_2_6(t *testing.T) {
	addr := startConformanceBroker(t, func(o *natsmqtt5.Options) {
		o.Authenticator = natsmqtt5.AuthenticatorFunc(func(_ context.Context, req *natsmqtt5.AuthRequest) (*natsmqtt5.AuthResult, error) {
			if req.Username == "bad" {
				return nil, errors.New("no")
			}
			return nil, nil
		})
	})
	c := dialRaw(t, addr)
	c.connect(&packet.Connect{ClientID: "sp-6", KeepAlive: 30, Username: "ok",
		Properties: &packet.Properties{SessionExpiryInterval: packet.Uint32(300)}})
	c.write(rawDisconnect)

	refused := dialRaw(t, addr)
	refused.send(&packet.Connect{ClientID: "sp-6", KeepAlive: 30, Username: "bad",
		Properties: &packet.Properties{SessionExpiryInterval: packet.Uint32(300)}})
	first, body, _ := refused.readFrame()
	require.Equal(t, byte(0x20), first)
	assert.Equal(t, byte(0x00), body[0], "Session Present 0")
	assert.GreaterOrEqual(t, body[1], byte(0x80), "a refusal")
	refused.expectClosed()
}

// §3.2.2.3.4: "If a Server does not support QoS 1 or QoS 2 PUBLISH packets it
// MUST send a Maximum QoS in the CONNACK packet specifying the highest QoS it
// supports" [MQTT-3.2.2-9]. A broker limited to QoS 0 or QoS 1 says so; the
// default broker supports QoS 2 and the property is absent (§3.2.2.3.4).
func TestConnack_MQTT_3_2_2_9(t *testing.T) {
	for _, tc := range []struct {
		name string
		max  *uint8
		want *byte
	}{
		{"QoS 0 only", natsmqtt5.Ptr(uint8(0)), packet.Byte(0)},
		{"QoS 1 at most", natsmqtt5.Ptr(uint8(1)), packet.Byte(1)},
		{"default", nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr := startConformanceBroker(t, func(o *natsmqtt5.Options) { o.MaximumQoS = tc.max })
			ack := dialRaw(t, addr).connect(rawConnect("maxqos-"+strings.ReplaceAll(tc.name, " ", ""), 0))
			require.NotNil(t, ack.Properties)
			assert.Equal(t, tc.want, ack.Properties.MaximumQoS)
		})
	}
}

// §3.2.2.3.4: "If a Server receives a CONNECT packet containing a Will QoS that
// exceeds its capabilities, it MUST reject the connection. It SHOULD use a
// CONNACK packet with Reason Code 0x9B (QoS not supported) ..., and MUST close
// the Network Connection" [MQTT-3.2.2-12].
func TestConnack_MQTT_3_2_2_12(t *testing.T) {
	for _, tc := range []struct {
		max     uint8
		willQoS packet.QoS
	}{{0, packet.QoS1}, {0, packet.QoS2}, {1, packet.QoS2}} {
		addr := startConformanceBroker(t, func(o *natsmqtt5.Options) { o.MaximumQoS = natsmqtt5.Ptr(tc.max) })
		c := dialRaw(t, addr)
		c.send(&packet.Connect{ClientID: "will-qos", KeepAlive: 30,
			Will: &packet.Will{Topic: "w", Payload: []byte("w"), QoS: tc.willQoS}})
		first, body, _ := c.readFrame()
		require.Equal(t, byte(0x20), first)
		assert.Equal(t, byte(0x9B), body[1], "QoS not supported: broker max %d, Will QoS %d", tc.max, tc.willQoS)
		c.expectClosed()
	}

	// Within its capabilities the same Will is accepted.
	addr := startConformanceBroker(t, func(o *natsmqtt5.Options) { o.MaximumQoS = natsmqtt5.Ptr(uint8(1)) })
	dialRaw(t, addr).connect(&packet.Connect{ClientID: "will-qos-ok", KeepAlive: 30,
		Will: &packet.Will{Topic: "w", Payload: []byte("w"), QoS: packet.QoS1}})
}

// §3.2.2.3.5: "If a Server receives a CONNECT packet containing a Will Message
// with the Will Retain set to 1, and it does not support retained messages, the
// Server MUST reject the connection request. It SHOULD send CONNACK with Reason
// Code 0x9A (Retain not supported) and then it MUST close the Network Connection"
// [MQTT-3.2.2-13].
func TestConnack_MQTT_3_2_2_13(t *testing.T) {
	addr := startConformanceBroker(t, func(o *natsmqtt5.Options) { o.DisableRetained = true })
	c := dialRaw(t, addr)
	c.send(&packet.Connect{ClientID: "will-retain", KeepAlive: 30,
		Will: &packet.Will{Topic: "w", Payload: []byte("w"), Retain: true}})
	first, body, _ := c.readFrame()
	require.Equal(t, byte(0x20), first)
	assert.Equal(t, byte(0x9A), body[1], "Retain not supported")
	c.expectClosed()

	// A Will that is not retained is fine.
	dialRaw(t, addr).connect(&packet.Connect{ClientID: "will-plain", KeepAlive: 30,
		Will: &packet.Will{Topic: "w", Payload: []byte("w")}})
}

// §3.2.2.3.7: "If the Client connects using a zero length Client Identifier, the
// Server MUST respond with a CONNACK containing an Assigned Client Identifier.
// The Assigned Client Identifier MUST be a new Client Identifier not used by any
// other Session currently in the Server" [MQTT-3.2.2-16].
func TestConnack_MQTT_3_2_2_16(t *testing.T) {
	addr := startConformanceBroker(t)
	named, _ := sessionPresentOnConnect(t, addr, "named-session", false, 300)
	_ = named

	seen := map[string]bool{"named-session": true}
	for i := 0; i < 3; i++ {
		ack := dialRaw(t, addr).connect(&packet.Connect{ClientID: "", CleanStart: true, KeepAlive: 30})
		require.NotNil(t, ack.Properties)
		id := ack.Properties.AssignedClientID
		assert.NotEmpty(t, id)
		assert.False(t, seen[id], "assigned identifier %q is already in use", id)
		seen[id] = true
	}
}
