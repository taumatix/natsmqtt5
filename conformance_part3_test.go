package natsmqtt5_test

import (
	"bufio"
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// Conformance audit, part 3: the Server statements of MQTT-5.0 from UNSUBACK
// (§3.11) through WebSocket (§6), rows 171-253 of conformance/mqtt5-statements.tsv.
//
// Each test starts a real embedded NATS server (JetStream on), a real broker,
// and drives it over a real TCP socket. Where the request needs bytes the codec
// would refuse, or the answer is judged byte for byte, the bytes are written by
// hand (conformance_part3_helpers_test.go). The wording quoted in each comment
// was read from the OASIS MQTT 5.0 text (mqtt-v5.0-os.html) on 2026-10-08.
//
// Statements already proved by an integration test elsewhere in this package
// are not repeated here: see the report that came with this file.

// ---- §3.11 UNSUBACK -------------------------------------------------------

// p3UnsubackFits shows an UNSUBACK that must stay within a client's Maximum
// Packet Size of 6 bytes (the bare packet is exactly 6: type, length, Packet
// Identifier, Property Length, one Reason Code), so it can carry neither a
// Reason String nor a User Property.
func p3UnsubackFits(t *testing.T, what string) {
	t.Helper()
	addr := p3Broker(t)
	c := p3Client(t, addr, "unsuback-limit", func(cp *packet.Connect) {
		cp.Properties.MaximumPacketSize = packet.Uint32(24)
	})
	// A filter that was subscribed (Success) and one that was not (0x11).
	require.Equal(t, []packet.ReasonCode{packet.Success}, c.p3Suback(p3SubscribeBytes(1, "u/a", 0x00)))
	for i, tc := range []struct {
		filter string
		code   byte
	}{{"u/a", 0x00}, {"u/never-subscribed", 0x11}} {
		c.writeRaw(p3UnsubscribeBytes(uint16(10+i), tc.filter))
		first, body := c.readRawPacket()
		require.Equal(t, byte(0xB0), first, "an UNSUBSCRIBE is answered with an UNSUBACK")
		assert.LessOrEqual(t, 2+len(body), 24, "the UNSUBACK is within the client's Maximum Packet Size of 24 bytes")
		assert.Equal(t, []byte{0x00, byte(10 + i), 0x00, tc.code}, body,
			"%s: Packet Identifier, Property Length 0, Reason Code 0x%02X", what, tc.code)
	}
}

// "The Server MUST NOT send this Property if it would increase the size of the
// UNSUBACK packet beyond the Maximum Packet Size specified by the Client"
// [MQTT-3.11.2-1] (Reason String).
func TestConformance_MQTT_3_11_2_1_UnsubackOmitsReasonStringBeyondMaximumPacketSize(t *testing.T) {
	p3UnsubackFits(t, "no Reason String [MQTT-3.11.2-1]")
}

// Same sentence for the User Property: [MQTT-3.11.2-2].
func TestConformance_MQTT_3_11_2_2_UnsubackOmitsUserPropertyBeyondMaximumPacketSize(t *testing.T) {
	p3UnsubackFits(t, "no User Property [MQTT-3.11.2-2]")
}

// "The order of Reason Codes in the UNSUBACK packet MUST match the order of
// Topic Filters in the UNSUBSCRIBE packet" [MQTT-3.11.3-1]. Subscribed and
// unsubscribed filters are interleaved so that a reordering shows.
func TestConformance_MQTT_3_11_3_1_UnsubackCodesFollowTheFilterOrder(t *testing.T) {
	addr := p3Broker(t)
	c := p3Client(t, addr, "unsub-order")
	c.subscribe("u/a", packet.QoS1)
	c.subscribe("u/c", packet.QoS1)

	c.writeRaw(p3UnsubscribeBytes(9, "u/a", "u/never", "u/c", "u/#"))
	ack, ok := c.read().(*packet.Unsuback)
	require.True(t, ok)
	assert.Equal(t, []packet.ReasonCode{0x00, 0x11, 0x00, 0x11}, ack.ReasonCodes)

	c.subscribe("u/a", packet.QoS1)
	c.writeRaw(p3UnsubscribeBytes(10, "u/x", "u/a"))
	ack, ok = c.read().(*packet.Unsuback)
	require.True(t, ok)
	assert.Equal(t, []packet.ReasonCode{0x11, 0x00}, ack.ReasonCodes)
}

// "The Server sending an UNSUBACK packet MUST use one of the Unsubscribe Reason
// Code values for each Topic Filter received" [MQTT-3.11.3-2]: 0x00, 0x11, 0x80,
// 0x83, 0x87, 0x8F, 0x91.
func TestConformance_MQTT_3_11_3_2_UnsubackUsesOnlyUnsubscribeReasonCodes(t *testing.T) {
	addr := p3Broker(t)
	c := p3Client(t, addr, "unsub-codes")
	c.subscribe("u/ok", packet.QoS1)
	allowed := map[packet.ReasonCode]bool{0x00: true, 0x11: true, 0x80: true, 0x83: true, 0x87: true, 0x8F: true, 0x91: true}

	filters := []string{"u/ok", "u/unknown", "u/#/bad", "u/+/ok", "$share/g/u/x", "$SYS/x", "u/\x01ctl", "é/ü"}
	c.writeRaw(p3UnsubscribeBytes(3, filters...))
	ack, ok := c.read().(*packet.Unsuback)
	require.True(t, ok)
	require.Len(t, ack.ReasonCodes, len(filters))
	for i, code := range ack.ReasonCodes {
		assert.True(t, allowed[code], "filter %q got 0x%02X, which is not an Unsubscribe Reason Code", filters[i], byte(code))
	}
}

// ---- §3.12 PINGREQ --------------------------------------------------------

// "The Server MUST send a PINGRESP packet in response to a PINGREQ packet"
// [MQTT-3.12.4-1]. PINGREQ is 0xC0 0x00 (§3.12.1) and PINGRESP 0xD0 0x00 (§3.13.1).
func TestConformance_MQTT_3_12_4_1_PingreqIsAnsweredWithPingresp(t *testing.T) {
	addr := p3Broker(t)
	c := p3Client(t, addr, "ping")
	for i := 0; i < 3; i++ {
		c.writeRaw([]byte{0xC0, 0x00})
		first, body := c.readRawPacket()
		assert.Equal(t, byte(0xD0), first)
		assert.Empty(t, body)
	}
}

// ---- §3.14 DISCONNECT -----------------------------------------------------

// "A Server MUST NOT send a DISCONNECT until after it has sent a CONNACK with
// Reason Code of less than 0x80" [MQTT-3.14.0-1]. Four ways a connection ends
// before any successful CONNACK; the broker may send a refusing CONNACK, and may
// send nothing, but never a DISCONNECT (type 14).
func TestConformance_MQTT_3_14_0_1_NoDisconnectBeforeASuccessfulConnack(t *testing.T) {
	addr := p3Broker(t)
	cases := map[string][]byte{
		"PINGREQ as the first packet":      {0xC0, 0x00},
		"PUBLISH as the first packet":      p3PublishBytes(0x00, 0, "a/b", "x"),
		"DISCONNECT as the first packet":   {0xE0, 0x00},
		"CONNECT of protocol version 4":    p3Frame(0x10, append(append(p3Str("MQTT"), 0x04, 0x02, 0x00, 0x1E), p3Str("v4")...)),
		"CONNECT with a wrong protocol":    p3Frame(0x10, append(append(p3Str("XXXX"), 0x05, 0x02, 0x00, 0x1E, 0x00), p3Str("bad")...)),
		"CONNECT with reserved flag bit 0": p3Frame(0x10, append(append(p3Str("MQTT"), 0x05, 0x03, 0x00, 0x1E, 0x00), p3Str("res")...)),
	}
	for name, first := range cases {
		t.Run(name, func(t *testing.T) {
			c := dialRaw(t, addr)
			c.writeRaw(first)
			types, _ := c.p3Rest()
			for _, typ := range types {
				assert.NotEqual(t, byte(14), typ>>4, "a DISCONNECT was sent before a successful CONNACK")
			}
		})
	}

	t.Run("CONNECT refused with 0x8C", func(t *testing.T) {
		c := dialRaw(t, addr)
		cp := rawConnect("refused", 0)
		cp.Properties.AuthenticationMethod = "SCRAM-SHA-1"
		c.send(cp)
		types, bodies := c.p3Rest()
		require.Len(t, types, 1, "exactly one packet, the CONNACK, then the close")
		assert.Equal(t, byte(0x20), types[0])
		assert.Equal(t, byte(0x8C), bodies[0][1])
	})
}

// "The Client or Server MUST validate that reserved bits are set to 0. If they
// are not zero it sends a DISCONNECT packet with a Reason code of 0x81 (Malformed
// Packet) as described in section 4.13" [MQTT-3.14.1-1].
func TestConformance_MQTT_3_14_1_1_DisconnectWithReservedBitsIsMalformed(t *testing.T) {
	addr := p3Broker(t)
	for _, first := range []byte{0xE1, 0xE2, 0xE4, 0xE8, 0xEF} {
		c := p3Client(t, addr, fmt.Sprintf("disc-flags-%02x", first))
		c.writeRaw([]byte{first, 0x00})
		f, body := c.readRawPacket()
		assert.Equal(t, byte(0xE0), f, "byte 1 0x%02X: answered with a DISCONNECT", first)
		require.NotEmpty(t, body)
		assert.Equal(t, byte(0x81), body[0], "byte 1 0x%02X: Malformed Packet", first)
		c.expectClosed()
	}
}

// serverDisconnects provokes a server DISCONNECT six ways and returns each
// DISCONNECT with the code the test expects. The socket is checked to close
// after it [MQTT-3.14.4-2] and to carry nothing more [MQTT-3.14.4-1].
type p3Disc struct {
	d    *packet.Disconnect
	want packet.ReasonCode
}

func serverDisconnects(t *testing.T) map[string]p3Disc {
	t.Helper()
	got := map[string]p3Disc{}
	record := func(name string, c *rawClient, want packet.ReasonCode) {
		t.Helper()
		types, bodies := c.p3Rest()
		require.NotEmpty(t, types, "%s: the broker closed without a DISCONNECT", name)
		require.Equal(t, byte(0xE0), types[0], "%s: expected a DISCONNECT first", name)
		assert.Len(t, types, 1, "%s: nothing follows the DISCONNECT and the broker closes the connection", name)
		p, err := packet.Read(bufio.NewReader(bytes.NewReader(p3Frame(0xE0, bodies[0]))), 0)
		require.NoError(t, err, name)
		d, ok := p.(*packet.Disconnect)
		require.True(t, ok, name)
		got[name] = p3Disc{d, want}
	}

	addr := p3Broker(t)
	c := p3Client(t, addr, "sd-flags")
	c.writeRaw([]byte{0xE1, 0x00})
	record("reserved bits in DISCONNECT", c, packet.MalformedPacket)

	c = p3Client(t, addr, "sd-second-connect")
	c.send(rawConnect("sd-second-connect", 0))
	record("second CONNECT", c, packet.ProtocolError)

	c = p3Client(t, addr, "sd-auth")
	c.writeRaw([]byte{0xF0, 0x00})
	record("AUTH from the client", c, packet.ProtocolError)

	c = p3Client(t, addr, "sd-alias")
	c.send(&packet.Publish{Topic: "a/b", Properties: &packet.Properties{TopicAlias: packet.Uint16(65)}})
	record("Topic Alias above the maximum", c, packet.TopicAliasInvalid)

	first := p3Client(t, addr, "sd-takeover")
	p3Client(t, addr, "sd-takeover")
	record("session taken over", first, packet.SessionTakenOver)

	c = p3Client(t, addr, "sd-keepalive", func(cp *packet.Connect) { cp.KeepAlive = 1 })
	record("Keep Alive timeout", c, packet.KeepAliveTimeout)

	small := p3Broker(t, func(o *natsmqtt5.Options) { o.MaximumPacketSize = 100 })
	c = p3Client(t, small, "sd-large")
	c.writeRaw(p3PublishBytes(0x00, 0, "a/b", strings.Repeat("x", 200)))
	record("packet above the broker's Maximum Packet Size", c, packet.PacketTooLarge)

	return got
}

// "The Client or Server sending the DISCONNECT packet MUST use one of the
// DISCONNECT Reason Code values" [MQTT-3.14.2-1] (§3.14.2.1);
// "The Session Expiry Interval MUST NOT be sent on a DISCONNECT by the Server"
// [MQTT-3.14.2-2]; and after a DISCONNECT the sender "MUST close the Network
// Connection" [MQTT-3.14.4-2] (checked by serverDisconnects).
func TestConformance_MQTT_3_14_2_ServerDisconnectsUseDisconnectCodesAndNoSessionExpiry(t *testing.T) {
	disconnectCodes := map[packet.ReasonCode]bool{
		0x00: true, 0x04: true, 0x80: true, 0x81: true, 0x82: true, 0x83: true, 0x87: true, 0x89: true,
		0x8B: true, 0x8D: true, 0x8E: true, 0x8F: true, 0x90: true, 0x93: true, 0x94: true, 0x95: true,
		0x96: true, 0x97: true, 0x98: true, 0x99: true, 0x9A: true, 0x9B: true, 0x9C: true, 0x9D: true,
		0x9E: true, 0x9F: true, 0xA0: true, 0xA1: true, 0xA2: true,
	}
	for name, r := range serverDisconnects(t) {
		assert.Equal(t, r.want, r.d.ReasonCode, name)
		assert.True(t, disconnectCodes[r.d.ReasonCode], "%s: 0x%02X is a DISCONNECT Reason Code [MQTT-3.14.2-1]", name, byte(r.d.ReasonCode))
		if r.d.Properties != nil {
			assert.Nil(t, r.d.Properties.SessionExpiryInterval, "%s: no Session Expiry Interval [MQTT-3.14.2-2]", name)
		}
	}
}

// "The sender MUST NOT send this property if it would increase the size of the
// DISCONNECT packet beyond the Maximum Packet Size specified by the receiver"
// [MQTT-3.14.2-4] (User Property). With a limit of 24 (the successful CONNACK must fit) the bare DISCONNECT is 3
// bytes (0xE0, Remaining Length 1, Reason Code 0x82); any property would need at
// least 4 more. The broker sends the bare packet and then closes.
func TestConformance_MQTT_3_14_2_4_DisconnectOmitsUserPropertyBeyondMaximumPacketSize(t *testing.T) {
	addr := p3Broker(t)
	c := dialRaw(t, addr)
	c.connect(connectWithMaxPacketSize("disc-limit", 24))
	c.send(rawConnect("disc-limit", 0)) // a second CONNECT is a Protocol Error
	c.expectRawBytes([]byte{0xE0, 0x01, 0x82})
	c.expectClosed()
}

// "On receipt of DISCONNECT with a Reason Code of 0x00 (Success) the Server: MUST
// discard any Will Message associated with the current Connection without
// publishing it" [MQTT-3.14.4-3]. The control is a connection that is lost
// without a DISCONNECT, whose Will is published.
func TestConformance_MQTT_3_14_4_3_NormalDisconnectDiscardsTheWill(t *testing.T) {
	addr := p3Broker(t)
	watch := p3Client(t, addr, "will-watch")
	watch.subscribe("will/#", packet.QoS1)

	withWill := func(topic string) func(*packet.Connect) {
		return func(cp *packet.Connect) {
			cp.Will = &packet.Will{Topic: topic, Payload: []byte("gone"), QoS: packet.QoS1}
		}
	}
	quiet := p3Client(t, addr, "will-quiet", withWill("will/quiet"))
	quiet.writeRaw([]byte{0xE0, 0x00})
	quiet.expectClosed()
	lost := p3Client(t, addr, "will-lost", withWill("will/lost"))
	lost.drop()

	got := p3Publishes(watch.p3Drain(time.Second))
	require.Len(t, got, 1, "only the Will of the connection that was lost is published")
	assert.Equal(t, "will/lost", got[0].Topic)
}

// ---- §3.15 AUTH -----------------------------------------------------------

// "Bits 3,2,1 and 0 of the Fixed Header of the AUTH packet are reserved and MUST
// all be set to 0. The Client or Server MUST treat any other value as malformed
// and close the Network Connection" [MQTT-3.15.1-1]. This broker offers no
// enhanced authentication, so it closes on any AUTH; the test shows it also does
// on the malformed one, and that it never answers with an AUTH.
func TestConformance_MQTT_3_15_1_1_AuthWithReservedBitsClosesTheConnection(t *testing.T) {
	addr := p3Broker(t)
	for _, first := range []byte{0xF1, 0xF2, 0xF4, 0xF8, 0xFF} {
		c := p3Client(t, addr, fmt.Sprintf("auth-flags-%02x", first))
		c.writeRaw([]byte{first, 0x00})
		types, bodies := c.p3Rest()
		for i, typ := range types {
			assert.Equal(t, byte(0xE0), typ, "byte 1 0x%02X: only a DISCONNECT may precede the close", first)
			assert.Contains(t, []byte{0x81, 0x82}, bodies[i][0])
		}
	}
}

// ---- §4.1 Session state ---------------------------------------------------

// "The Client and Server MUST NOT discard the Session State while the Network
// Connection is open" [MQTT-4.1.0-1] (also labelled [MQTT-4.2.0-1] in Appendix B).
// A session with a Session Expiry Interval of 1 s stays connected for 2.5 s,
// longer than the interval and with a sweeper running every 50 ms. It then still
// has its subscription, and it resumes with Session Present 1 on an immediate
// reconnect.
func TestConformance_MQTT_4_1_0_1_SessionStateSurvivesWhileConnected(t *testing.T) {
	addr := p3Broker(t, func(o *natsmqtt5.Options) { o.SessionSweepInterval = 50 * time.Millisecond })
	sub := dialRaw(t, addr)
	cp := rawConnect("keeper", 1)
	cp.CleanStart = true
	sub.connect(cp)
	sub.subscribe("keep/#", packet.QoS1)

	time.Sleep(2500 * time.Millisecond)

	pub := p3Client(t, addr, "keep-pub")
	pub.send(&packet.Publish{Topic: "keep/x", QoS: packet.QoS1, PacketID: 1, Payload: []byte("still here")})
	assert.Equal(t, "still here", string(sub.expectPublish().Payload), "the subscription outlived the interval")

	sub.drop()
	back := dialRaw(t, addr)
	ack := back.connect(rawConnect("keeper", 1))
	assert.True(t, ack.SessionPresent, "the session state was not discarded while the connection was open")
}

// ---- §4.2 Network connections ---------------------------------------------

// "A Client or Server MUST support the use of one or more underlying transport
// protocols that provide an ordered, lossless, stream of bytes from the Client to
// Server and Server to Client" [MQTT-4.2-1] (and [MQTT-4.2.0-1] is the Appendix B
// label of [MQTT-4.1.0-1], above). The broker's TCP listener carries 300 QoS 1
// messages in order and without a gap. TLS and WebSocket are not exercised.
func TestConformance_MQTT_4_2_1_TCPCarriesAnOrderedLosslessStream(t *testing.T) {
	addr := p3Broker(t)
	sub := p3Client(t, addr, "stream-sub")
	sub.subscribe("stream/#", packet.QoS1)
	pub := p3Client(t, addr, "stream-pub")
	const n = 300
	for i := 1; i <= n; i++ {
		pub.send(&packet.Publish{Topic: "stream/x", QoS: packet.QoS1, PacketID: uint16(i), Payload: []byte(fmt.Sprintf("%04d", i))})
		_, ok := pub.read().(*packet.Puback)
		require.True(t, ok)
	}
	for i := 1; i <= n; i++ {
		p := sub.expectPublish()
		require.Equal(t, fmt.Sprintf("%04d", i), string(p.Payload), "message %d", i)
		sub.send(&packet.Puback{Ack: packet.Ack{PacketID: p.PacketID}})
	}
}

// ---- §4.3 Delivery protocols ----------------------------------------------

// "the sender MUST send a PUBLISH packet with QoS 0 and DUP flag set to 0"
// [MQTT-4.3.1-1]. The first byte of the PUBLISH is judged: 0x30 is PUBLISH with
// DUP 0, QoS 0, RETAIN 0. The publisher sends QoS 1 with DUP 1, which must not be
// copied onward.
func TestConformance_MQTT_4_3_1_1_QoS0DeliveryHasDupZero(t *testing.T) {
	addr := p3Broker(t)
	sub := p3Client(t, addr, "q0-sub")
	sub.subscribe("q0/#", packet.QoS0)
	pub := p3Client(t, addr, "q0-pub")
	pub.writeRaw(p3PublishBytes(0x0A, 1, "q0/x", "one")) // DUP 1, QoS 1
	first, body := sub.readRawPacket()
	assert.Equal(t, byte(0x30), first, "PUBLISH, DUP 0, QoS 0, RETAIN 0")
	assert.Equal(t, append(p3Str("q0/x"), 0x00, 'o', 'n', 'e'), body)
}

// "MUST assign an unused Packet Identifier each time it has a new Application
// Message to publish" [MQTT-4.3.2-1] and "MUST send a PUBLISH packet containing
// this Packet Identifier with QoS 1 and DUP flag set to 0" [MQTT-4.3.2-2]. Five
// messages are left unacknowledged, so all five identifiers are in use together;
// the first byte of each is 0x32 (QoS 1, DUP 0) although the publisher set DUP.
func TestConformance_MQTT_4_3_2_1_QoS1DeliveriesGetUnusedIdentifiersAndDupZero(t *testing.T) {
	addr := p3Broker(t)
	sub := p3Client(t, addr, "q1-sub")
	sub.subscribe("q1/#", packet.QoS1)
	pub := p3Client(t, addr, "q1-pub")
	for i := 1; i <= 5; i++ {
		pub.writeRaw(p3PublishBytes(0x0A, uint16(i), "q1/x", fmt.Sprint(i))) // DUP 1: a retransmission from the publisher
		_, ok := pub.read().(*packet.Puback)
		require.True(t, ok)
	}
	seen := map[uint16]bool{}
	for i := 1; i <= 5; i++ {
		first, body := sub.readRawPacket()
		assert.Equal(t, byte(0x32), first, "DUP 0 and QoS 1 [MQTT-4.3.2-2]")
		topic := len(p3Str("q1/x"))
		id := uint16(body[topic])<<8 | uint16(body[topic+1])
		assert.NotZero(t, id)
		assert.False(t, seen[id], "Packet Identifier %d is still in use [MQTT-4.3.2-1]", id)
		seen[id] = true
	}
}

// "MUST treat the PUBLISH packet as unacknowledged until it has received the
// corresponding PUBACK packet from the receiver" [MQTT-4.3.2-3]. Seen two ways:
// with Receive Maximum 1 the next message waits for the PUBACK; and a message
// never acknowledged is the one resent (same identifier, DUP 1) when the session
// resumes.
func TestConformance_MQTT_4_3_2_3_QoS1StaysUnacknowledgedUntilPuback(t *testing.T) {
	addr := p3Broker(t)
	sub := p3Client(t, addr, "q1-unack", p3ReceiveMax(1))
	sub.subscribe("ua/#", packet.QoS1)
	pub := p3Client(t, addr, "q1-unack-pub")
	pub.send(&packet.Publish{Topic: "ua/x", QoS: packet.QoS1, PacketID: 1, Payload: []byte("m1")})
	pub.send(&packet.Publish{Topic: "ua/x", QoS: packet.QoS1, PacketID: 2, Payload: []byte("m2")})

	first := sub.expectPublish()
	assert.Empty(t, p3Publishes(sub.p3Drain(500*time.Millisecond)), "m2 waits: m1 is still unacknowledged")
	sub.send(&packet.Puback{Ack: packet.Ack{PacketID: first.PacketID}})
	assert.Equal(t, "m2", string(sub.expectPublish().Payload))

	durable := dialRaw(t, addr)
	durable.connect(rawConnect("q1-resume", 300))
	durable.subscribe("ur/#", packet.QoS1)
	pub.send(&packet.Publish{Topic: "ur/x", QoS: packet.QoS1, PacketID: 3, Payload: []byte("m3")})
	sent := durable.expectPublish()
	durable.drop()

	back := dialRaw(t, addr)
	ack := back.connect(rawConnect("q1-resume", 300))
	require.True(t, ack.SessionPresent)
	resent := back.expectPublish()
	assert.Equal(t, sent.PacketID, resent.PacketID, "the original Packet Identifier")
	assert.True(t, resent.Dup)
	assert.Equal(t, "m3", string(resent.Payload))
}

// "the receiver MUST respond with a PUBACK packet containing the Packet
// Identifier from the incoming PUBLISH packet, having accepted ownership of the
// Application Message" [MQTT-4.3.2-4]; "After it has sent a PUBACK packet the
// receiver MUST treat any incoming PUBLISH packet that contains the same Packet
// Identifier as being a new Application Message, irrespective of the setting of
// its DUP flag" [MQTT-4.3.2-5]. The same identifier is used three times, the
// second with DUP 1, and a subscriber receives all three.
func TestConformance_MQTT_4_3_2_4_QoS1IsAcknowledgedAndAReusedIdentifierIsANewMessage(t *testing.T) {
	addr := p3Broker(t)
	sub := p3Client(t, addr, "q1-recv-sub")
	sub.subscribe("rc/#", packet.QoS1)
	pub := p3Client(t, addr, "q1-recv-pub")
	for i, flags := range []byte{0x02, 0x0A, 0x02} { // QoS 1; QoS 1 + DUP; QoS 1
		pub.writeRaw(p3PublishBytes(flags, 0x1234, "rc/x", fmt.Sprint("msg", i)))
		first, body := pub.readRawPacket()
		require.Equal(t, byte(0x40), first, "PUBACK")
		require.GreaterOrEqual(t, len(body), 2)
		assert.Equal(t, []byte{0x12, 0x34}, body[:2], "the Packet Identifier of the PUBLISH [MQTT-4.3.2-4]")
		if len(body) > 2 {
			assert.Equal(t, byte(0x00), body[2], "Success")
		}
		assert.Equal(t, fmt.Sprint("msg", i), string(sub.expectPublish().Payload), "delivered as a new message [MQTT-4.3.2-5]")
	}
}

// "MUST assign an unused Packet Identifier when it has a new Application Message
// to publish" [MQTT-4.3.3-1]: five QoS 2 messages in flight together, each with
// its own non-zero identifier.
func TestConformance_MQTT_4_3_3_1_QoS2DeliveriesGetUnusedIdentifiers(t *testing.T) {
	addr := p3Broker(t)
	sub := p3Client(t, addr, "q2-sub")
	sub.subscribe("q2/#", packet.QoS2)
	pub := p3Client(t, addr, "q2-pub")
	for i := 1; i <= 5; i++ {
		pub.send(&packet.Publish{Topic: "q2/x", QoS: packet.QoS2, PacketID: uint16(i), Payload: []byte{byte(i)}})
		_, ok := pub.read().(*packet.Pubrec)
		require.True(t, ok)
	}
	seen := map[uint16]bool{}
	for i := 1; i <= 5; i++ {
		p := sub.expectPublish()
		assert.Equal(t, packet.QoS2, p.QoS)
		assert.NotZero(t, p.PacketID)
		assert.False(t, seen[p.PacketID], "Packet Identifier %d is still in use [MQTT-4.3.3-1]", p.PacketID)
		seen[p.PacketID] = true
	}
}

// "MUST treat the PUBLISH packet as unacknowledged until it has received the
// corresponding PUBREC packet from the receiver" [MQTT-4.3.3-3]: a QoS 2 message
// whose PUBREC never came is resent on resume with the same identifier and DUP 1.
func TestConformance_MQTT_4_3_3_3_QoS2StaysUnacknowledgedUntilPubrec(t *testing.T) {
	addr := p3Broker(t)
	sub := dialRaw(t, addr)
	sub.connect(rawConnect("q2-unrec", 300))
	sub.subscribe("ur2/#", packet.QoS2)
	pub := p3Client(t, addr, "q2-unrec-pub")
	pub.send(&packet.Publish{Topic: "ur2/x", QoS: packet.QoS2, PacketID: 1, Payload: []byte("m")})
	sent := sub.expectPublish()
	sub.drop()

	back := dialRaw(t, addr)
	require.True(t, back.connect(rawConnect("q2-unrec", 300)).SessionPresent)
	resent := back.expectPublish()
	assert.Equal(t, sent.PacketID, resent.PacketID)
	assert.True(t, resent.Dup)
	assert.Equal(t, packet.QoS2, resent.QoS)
}

// "MUST treat the PUBREL packet as unacknowledged until it has received the
// corresponding PUBCOMP packet from the receiver" [MQTT-4.3.3-5]. With Receive
// Maximum 1 the second message waits for the PUBCOMP, not just the PUBREC; and a
// PUBREL without its PUBCOMP is the packet resent on resume.
func TestConformance_MQTT_4_3_3_5_PubrelStaysUnacknowledgedUntilPubcomp(t *testing.T) {
	addr := p3Broker(t)
	rm1 := func(cp *packet.Connect) { cp.Properties.ReceiveMaximum = packet.Uint16(1) }
	sub := dialRaw(t, addr)
	cp := rawConnect("q2-pubrel", 300)
	rm1(cp)
	sub.connect(cp)
	sub.subscribe("pr/#", packet.QoS2)
	pub := p3Client(t, addr, "q2-pubrel-pub")
	pub.send(&packet.Publish{Topic: "pr/x", QoS: packet.QoS2, PacketID: 1, Payload: []byte("m1")})
	pub.send(&packet.Publish{Topic: "pr/x", QoS: packet.QoS2, PacketID: 2, Payload: []byte("m2")})

	m1 := sub.expectPublish()
	sub.send(&packet.Pubrec{Ack: packet.Ack{PacketID: m1.PacketID}})
	rel := sub.expectPubrel()
	require.Equal(t, m1.PacketID, rel.PacketID)
	assert.Empty(t, p3Publishes(sub.p3Drain(500*time.Millisecond)), "m2 waits for the PUBCOMP")
	sub.drop()

	back := dialRaw(t, addr)
	cp = rawConnect("q2-pubrel", 300)
	rm1(cp)
	require.True(t, back.connect(cp).SessionPresent)
	again := back.expectPubrel()
	assert.Equal(t, m1.PacketID, again.PacketID, "the PUBREL, not the PUBLISH, is resent")
	back.send(&packet.Pubcomp{Ack: packet.Ack{PacketID: m1.PacketID}})
	assert.Equal(t, "m2", string(back.expectPublish().Payload), "released by the PUBCOMP")
}

// "If it has sent a PUBREC with a Reason Code of 0x80 or greater, the receiver
// MUST treat any subsequent PUBLISH packet that contains that Packet Identifier
// as being a new Application Message" [MQTT-4.3.3-9]. The refusal is a PUBLISH to
// a $ topic on a broker with RestrictDollarTopics; the identifier is then used
// for a valid message, which must be accepted, delivered and completable.
func TestConformance_MQTT_4_3_3_9_AnIdentifierRefusedWithAnErrorPubrecIsFreeAgain(t *testing.T) {
	addr := p3Broker(t, restrictDollar)
	sub := p3Client(t, addr, "q2-err-sub")
	sub.subscribe("ok/#", packet.QoS2)
	pub := p3Client(t, addr, "q2-err-pub")

	pub.send(&packet.Publish{Topic: "$private/x", QoS: packet.QoS2, PacketID: 5, Payload: []byte("refused")})
	refused, ok := pub.read().(*packet.Pubrec)
	require.True(t, ok)
	require.GreaterOrEqual(t, byte(refused.ReasonCode), byte(0x80))

	pub.send(&packet.Publish{Topic: "ok/x", QoS: packet.QoS2, PacketID: 5, Payload: []byte("new")})
	accepted, ok := pub.read().(*packet.Pubrec)
	require.True(t, ok)
	assert.Equal(t, packet.ReasonCode(0), accepted.ReasonCode)
	assert.Equal(t, "new", string(sub.expectPublish().Payload), "a new Application Message")
	pub.send(&packet.Pubrel{Ack: packet.Ack{PacketID: 5}})
	done, ok := pub.read().(*packet.Pubcomp)
	require.True(t, ok)
	assert.Equal(t, packet.ReasonCode(0), done.ReasonCode, "the identifier was in use for the new message")
}

// "Until it has received the corresponding PUBREL packet, the receiver MUST
// acknowledge any subsequent PUBLISH packet with the same Packet Identifier by
// sending a PUBREC. It MUST NOT cause duplicate messages to be delivered to any
// onward recipients in this case" [MQTT-4.3.3-10]. (The cross-broker and restart
// cases are in restore_inflight_test.go.)
func TestConformance_MQTT_4_3_3_10_AResentQoS2PublishIsAcknowledgedAndForwardedOnce(t *testing.T) {
	addr := p3Broker(t)
	sub := p3Client(t, addr, "q2-dup-sub")
	sub.subscribe("dp/#", packet.QoS2)
	pub := p3Client(t, addr, "q2-dup-pub")
	for _, flags := range []byte{0x04, 0x0C, 0x0C} { // QoS 2; QoS 2 + DUP twice
		pub.writeRaw(p3PublishBytes(flags, 9, "dp/x", "once"))
		p, ok := pub.read().(*packet.Pubrec)
		require.True(t, ok)
		assert.Equal(t, uint16(9), p.PacketID)
		assert.Equal(t, packet.ReasonCode(0), p.ReasonCode)
	}
	pub.send(&packet.Pubrel{Ack: packet.Ack{PacketID: 9}})
	_, ok := pub.read().(*packet.Pubcomp)
	require.True(t, ok)
	got := p3Publishes(sub.p3Drain(700 * time.Millisecond))
	assert.Len(t, got, 1, "one delivery for three PUBLISH packets")
}

// "MUST respond to a PUBREL packet by sending a PUBCOMP packet containing the
// same Packet Identifier as the PUBREL" [MQTT-4.3.3-11]; "After it has sent a
// PUBCOMP, the receiver MUST treat any subsequent PUBLISH packet that contains
// that Packet Identifier as being a new Application Message" [MQTT-4.3.3-12].
// Identifier 7 is used twice, with a PUBREL for an identifier the broker never
// saw (answered with 0x92, which §3.7.2.1 allows) in between.
func TestConformance_MQTT_4_3_3_11_PubrelIsAnsweredWithPubcompAndTheIdentifierIsFreed(t *testing.T) {
	addr := p3Broker(t)
	sub := p3Client(t, addr, "q2-rel-sub")
	sub.subscribe("rl/#", packet.QoS2)
	pub := p3Client(t, addr, "q2-rel-pub")
	for round := 0; round < 2; round++ {
		pub.writeRaw(p3PublishBytes(0x04, 7, "rl/x", fmt.Sprint("round", round)))
		rec, ok := pub.read().(*packet.Pubrec)
		require.True(t, ok)
		require.Equal(t, packet.ReasonCode(0), rec.ReasonCode, "round %d: the reused identifier is a new message [MQTT-4.3.3-12]", round)
		pub.writeRaw(p3AckBytes(0x62, 7)) // PUBREL: 0x62, flags 0b0010 (§3.6.1)
		comp, ok := pub.read().(*packet.Pubcomp)
		require.True(t, ok)
		assert.Equal(t, uint16(7), comp.PacketID, "[MQTT-4.3.3-11]")
		assert.Equal(t, packet.ReasonCode(0), comp.ReasonCode)
		assert.Equal(t, fmt.Sprint("round", round), string(sub.expectPublish().Payload))
	}
	pub.writeRaw(p3AckBytes(0x62, 4242))
	comp, ok := pub.read().(*packet.Pubcomp)
	require.True(t, ok)
	assert.Equal(t, uint16(4242), comp.PacketID)
	assert.Equal(t, packet.ReasonCode(0x92), comp.ReasonCode, "Packet Identifier not found")
}

// "MUST continue the QoS 2 acknowledgement sequence even if it has applied
// message expiry" [MQTT-4.3.3-13]. A QoS 2 PUBLISH with a Message Expiry Interval
// of 1 s is accepted; the PUBREL arrives after 2.2 s, when the message has
// expired, and is still answered with a successful PUBCOMP.
func TestConformance_MQTT_4_3_3_13_AnExpiredQoS2MessageStillCompletesItsHandshake(t *testing.T) {
	addr := p3Broker(t)
	sub := p3Client(t, addr, "q2-exp-sub")
	sub.subscribe("ex/#", packet.QoS2)
	pub := p3Client(t, addr, "q2-exp-pub")
	pub.send(&packet.Publish{Topic: "ex/x", QoS: packet.QoS2, PacketID: 3, Payload: []byte("short-lived"),
		Properties: &packet.Properties{MessageExpiryInterval: packet.Uint32(1)}})
	rec, ok := pub.read().(*packet.Pubrec)
	require.True(t, ok)
	require.Equal(t, packet.ReasonCode(0), rec.ReasonCode)

	time.Sleep(2200 * time.Millisecond)
	pub.send(&packet.Pubrel{Ack: packet.Ack{PacketID: 3}})
	comp, ok := pub.read().(*packet.Pubcomp)
	require.True(t, ok)
	assert.Equal(t, uint16(3), comp.PacketID)
	assert.Equal(t, packet.ReasonCode(0), comp.ReasonCode)
}

// ---- §4.7 Topics ----------------------------------------------------------

// "The wildcard characters can be used in Topic Filters, but MUST NOT be used
// within a Topic Name" [MQTT-4.7.0-1]. A PUBLISH to "a/+" or "a/#" is refused
// (PUBACK 0x90, Topic Name invalid) and reaches a subscriber to "#" as nothing.
func TestConformance_MQTT_4_7_0_1_WildcardsAreRefusedInATopicName(t *testing.T) {
	addr := p3Broker(t)
	sub := p3Client(t, addr, "wild-sub")
	sub.subscribe("#", packet.QoS1)
	pub := p3Client(t, addr, "wild-pub")
	for i, name := range []string{"a/+", "a/#", "+", "#", "a/b+"} {
		pub.writeRaw(p3PublishBytes(0x02, uint16(i+1), name, "x"))
		ack, ok := pub.read().(*packet.Puback)
		require.True(t, ok, "%q", name)
		assert.Equal(t, packet.TopicNameInvalid, ack.ReasonCode, "%q", name)
	}
	assert.Empty(t, p3Publishes(sub.p3Drain(500*time.Millisecond)))
}

// "The multi-level wildcard character MUST be specified either on its own or
// following a topic level separator. In either case it MUST be the last
// character specified in the Topic Filter" [MQTT-4.7.1-1].
func TestConformance_MQTT_4_7_1_1_MultiLevelWildcardPlacement(t *testing.T) {
	addr := p3Broker(t)
	c := p3Client(t, addr, "hash")
	for i, f := range []string{"sport/tennis#", "sport/tennis/#/ranking", "#/x", "a/#/", "##", "#a"} {
		codes := c.p3Suback(p3SubscribeBytes(uint16(i+1), f, 0x00))
		assert.Equal(t, []packet.ReasonCode{packet.TopicFilterInvalid}, codes, "%q is not valid", f)
	}
	for i, f := range []string{"#", "sport/tennis/#", "/#"} {
		codes := c.p3Suback(p3SubscribeBytes(uint16(20+i), f, 0x00))
		assert.Equal(t, []packet.ReasonCode{packet.Success}, codes, "%q is valid", f)
	}
}

// "Where it is used, it MUST occupy an entire level of the filter"
// [MQTT-4.7.1-2], and "sport/+ does not match sport but it does match sport/".
func TestConformance_MQTT_4_7_1_2_SingleLevelWildcardPlacement(t *testing.T) {
	addr := p3Broker(t)
	c := p3Client(t, addr, "plus")
	for i, f := range []string{"sport+", "+sport", "sport/+tennis", "sport/te+nnis", "+/ten+"} {
		codes := c.p3Suback(p3SubscribeBytes(uint16(i+1), f, 0x00))
		assert.Equal(t, []packet.ReasonCode{packet.TopicFilterInvalid}, codes, "%q is not valid", f)
	}

	sub := p3Client(t, addr, "plus-sub")
	sub.subscribe("sport/+", packet.QoS1)
	sub.subscribe("+/+", packet.QoS1)
	pub := p3Client(t, addr, "plus-pub")
	pub.send(&packet.Publish{Topic: "sport", QoS: packet.QoS1, PacketID: 1, Payload: []byte("one level")})
	pub.read()
	assert.Empty(t, p3Publishes(sub.p3Drain(400*time.Millisecond)), "sport/+ does not match sport")
	pub.send(&packet.Publish{Topic: "sport/", QoS: packet.QoS1, PacketID: 2, Payload: []byte("empty level")})
	pub.read()
	got := p3Publishes(sub.p3Drain(500 * time.Millisecond))
	require.NotEmpty(t, got, "sport/+ matches sport/")
	assert.Equal(t, "sport/", got[0].Topic)
	pub.send(&packet.Publish{Topic: "/finance", QoS: packet.QoS1, PacketID: 3, Payload: []byte("leading slash")})
	pub.read()
	got = p3Publishes(sub.p3Drain(500 * time.Millisecond))
	require.NotEmpty(t, got, "/finance matches +/+")
	assert.Equal(t, "/finance", got[0].Topic)
}

// "The Server MUST NOT match Topic Filters starting with a wildcard character (#
// or +) with Topic Names beginning with a $ character" [MQTT-4.7.2-1]. The "#"
// case is in broker_test.go; this adds "+" and "+/#", with the matching explicit
// filter as the control.
func TestConformance_MQTT_4_7_2_1_PlusDoesNotMatchADollarTopic(t *testing.T) {
	addr := p3Broker(t)
	sub := p3Client(t, addr, "dollar-sub")
	sub.subscribe("+/monitor/Clients", packet.QoS1)
	sub.subscribe("+/#", packet.QoS1)
	pub := p3Client(t, addr, "dollar-pub")
	pub.send(&packet.Publish{Topic: "$SYS/monitor/Clients", QoS: packet.QoS1, PacketID: 1, Payload: []byte("x")})
	pub.read()
	assert.Empty(t, p3Publishes(sub.p3Drain(500*time.Millisecond)))
	pub.send(&packet.Publish{Topic: "SYS/monitor/Clients", QoS: packet.QoS1, PacketID: 2, Payload: []byte("y")})
	pub.read()
	assert.NotEmpty(t, p3Publishes(sub.p3Drain(500*time.Millisecond)), "the same filters do match a topic without the $")
}

// "All Topic Names and Topic Filters MUST be at least one character long"
// [MQTT-4.7.3-1].
func TestConformance_MQTT_4_7_3_1_EmptyTopicsAreRefused(t *testing.T) {
	addr := p3Broker(t)
	c := p3Client(t, addr, "empty-filter")
	assert.Equal(t, []packet.ReasonCode{packet.TopicFilterInvalid}, c.p3Suback(p3SubscribeBytes(1, "", 0x00)))

	watch := p3Client(t, addr, "empty-watch")
	watch.subscribe("#", packet.QoS1)
	pub := p3Client(t, addr, "empty-pub")
	pub.writeRaw(p3PublishBytes(0x00, 0, "", "nameless")) // no Topic Alias to stand in for the name
	types, bodies := pub.p3Rest()
	require.Len(t, types, 1)
	assert.Equal(t, byte(0xE0), types[0], "a zero-length Topic Name without an alias is a Protocol Error")
	assert.Equal(t, byte(0x82), bodies[0][0])
	assert.Empty(t, p3Publishes(watch.p3Drain(400*time.Millisecond)))
}

// "Topic Names and Topic Filters MUST NOT include the null character (Unicode
// U+0000)" [MQTT-4.7.3-2]. The broker refuses the SUBSCRIBE and the PUBLISH
// (MQTT-5.0 §1.5.4 makes the string itself invalid, so either a failure code or a
// closed connection is a refusal); nothing is delivered.
func TestConformance_MQTT_4_7_3_2_NullCharacterIsRefused(t *testing.T) {
	addr := p3Broker(t)
	watch := p3Client(t, addr, "null-watch")
	watch.subscribe("#", packet.QoS1)

	c := p3Client(t, addr, "null-filter")
	c.writeRaw(p3SubscribeBytes(1, "a/\x00/b", 0x00))
	types, bodies := c.p3Rest()
	require.NotEmpty(t, types, "the SUBSCRIBE must not be accepted silently")
	if types[0] == 0x90 {
		assert.GreaterOrEqual(t, bodies[0][len(bodies[0])-1], byte(0x80))
	} else {
		assert.Equal(t, byte(0xE0), types[0])
	}

	pub := p3Client(t, addr, "null-pub")
	pub.writeRaw(p3PublishBytes(0x00, 0, "a/\x00/b", "x"))
	types, _ = pub.p3Rest()
	assert.Equal(t, []byte{0xE0}, types, "the PUBLISH is refused with a DISCONNECT and the close")
	assert.Empty(t, p3Publishes(watch.p3Drain(400*time.Millisecond)))
}

// "When it performs subscription matching the Server MUST NOT perform any
// normalization of Topic Names or Topic Filters, or any modification or
// substitution of unrecognized characters" [MQTT-4.7.3-4]. "é" written as one
// code point (NFC) and as "e" plus a combining accent (NFD) are different
// topics, as are letters of different case; characters outside ASCII reach the
// subscriber byte for byte.
func TestConformance_MQTT_4_7_3_4_TopicsAreMatchedByteForByte(t *testing.T) {
	addr := p3Broker(t)
	const nfc, nfd = "café/x", "café/x"
	subNFC := p3Client(t, addr, "nfc-sub")
	subNFC.subscribe(nfc, packet.QoS1)
	subNFD := p3Client(t, addr, "nfd-sub")
	subNFD.subscribe(nfd, packet.QoS1)
	subCase := p3Client(t, addr, "case-sub")
	subCase.subscribe("Case/Topic", packet.QoS1)
	pub := p3Client(t, addr, "nfc-pub")

	pub.send(&packet.Publish{Topic: nfd, QoS: packet.QoS1, PacketID: 1, Payload: []byte("decomposed")})
	pub.read()
	got := subNFD.expectPublish()
	assert.Equal(t, nfd, got.Topic)
	assert.Empty(t, p3Publishes(subNFC.p3Drain(400*time.Millisecond)), "NFD is not matched by the NFC filter")

	pub.send(&packet.Publish{Topic: nfc, QoS: packet.QoS1, PacketID: 2, Payload: []byte("composed")})
	pub.read()
	assert.Equal(t, nfc, subNFC.expectPublish().Topic)
	assert.Empty(t, p3Publishes(subNFD.p3Drain(400*time.Millisecond)), "NFC is not matched by the NFD filter")

	pub.send(&packet.Publish{Topic: "case/topic", QoS: packet.QoS1, PacketID: 3, Payload: []byte("lower")})
	pub.read()
	assert.Empty(t, p3Publishes(subCase.p3Drain(400*time.Millisecond)), "topics are case sensitive")

	const exotic = "日本語/ü/😀/ /​"
	subExotic := p3Client(t, addr, "exotic-sub")
	subExotic.subscribe(exotic, packet.QoS1)
	pub.send(&packet.Publish{Topic: exotic, QoS: packet.QoS1, PacketID: 4, Payload: []byte("exotic")})
	pub.read()
	assert.Equal(t, exotic, subExotic.expectPublish().Topic)
}

// ---- §4.8 Shared subscriptions --------------------------------------------

// "A Shared Subscription's Topic Filter MUST start with $share/ and MUST contain
// a ShareName that is at least one character long" [MQTT-4.8.2-1]; "The
// ShareName MUST NOT contain the characters "/", "+" or "#", but MUST be followed
// by a "/" character. This "/" character MUST be followed by a Topic Filter"
// [MQTT-4.8.2-2].
func TestConformance_MQTT_4_8_2_1_SharedSubscriptionFilterSyntax(t *testing.T) {
	addr := p3Broker(t)
	c := p3Client(t, addr, "share-syntax")
	invalid := []string{
		"$share//t",   // ShareName of length 0 [MQTT-4.8.2-1]
		"$share/g",    // no "/" after the ShareName [MQTT-4.8.2-2]
		"$share/g/",   // no Topic Filter after the "/" [MQTT-4.8.2-2]
		"$share/g+/t", // "+" in the ShareName
		"$share/g#/t", // "#" in the ShareName
		"$share/+/t",
		"$share/#",
		"$share/g/a/#/b", // the Topic Filter has to be valid too
	}
	for i, f := range invalid {
		codes := c.p3Suback(p3SubscribeBytes(uint16(i+1), f, 0x01))
		require.Len(t, codes, 1)
		assert.GreaterOrEqual(t, byte(codes[0]), byte(0x80), "%q is not a valid Shared Subscription filter", f)
	}
	for i, f := range []string{"$share/g/t", "$share/g/a/+/b", "$share/g/#", "$share/a-b_c/x"} {
		codes := c.p3Suback(p3SubscribeBytes(uint16(100+i), f, 0x01))
		assert.Equal(t, []packet.ReasonCode{packet.GrantedQoS1}, codes, "%q is valid", f)
	}
}

// "When sending an Application Message to a Client, the Server MUST respect the
// granted QoS for the Client's subscription" [MQTT-4.8.2-3]: a shared and a
// non-shared subscriber both asked for QoS 0 and are sent QoS 0 for a QoS 1
// PUBLISH; the byte is 0x30, not 0x32.
func TestConformance_MQTT_4_8_2_3_DeliveryRespectsTheGrantedQoS(t *testing.T) {
	addr := p3Broker(t)
	shared := p3Client(t, addr, "granted-shared")
	shared.subscribe("$share/g/gq/#", packet.QoS0)
	plain := p3Client(t, addr, "granted-plain")
	plain.subscribe("gq/#", packet.QoS0)
	pub := p3Client(t, addr, "granted-pub")
	pub.send(&packet.Publish{Topic: "gq/x", QoS: packet.QoS2, PacketID: 1, Payload: []byte("q2")})
	_, ok := pub.read().(*packet.Pubrec)
	require.True(t, ok)

	first, _ := shared.readRawPacket()
	assert.Equal(t, byte(0x30), first, "shared subscription granted QoS 0")
	first, _ = plain.readRawPacket()
	assert.Equal(t, byte(0x30), first, "ordinary subscription granted QoS 0")
}

// Shared subscriptions are granted at most QoS 1, so the broker never starts the
// QoS 2 delivery that [MQTT-4.8.2-4] ("the Server MUST complete the delivery of
// the message to that Client when it reconnects") and [MQTT-4.8.2-5] ("the Server
// MUST NOT send the Application Message to any other subscribed Client") speak
// of: a QoS 2 PUBLISH reaches a member as QoS 1. This test pins that premise.
func TestConformance_MQTT_4_8_2_4_SharedSubscriptionsAreGrantedAtMostQoS1(t *testing.T) {
	addr := p3Broker(t)
	member := p3Client(t, addr, "share-q2")
	member.writeRaw(p3SubscribeBytes(1, "$share/g/sq/#", 0x02))
	ack, ok := member.read().(*packet.Suback)
	require.True(t, ok)
	assert.Equal(t, []packet.ReasonCode{packet.GrantedQoS1}, ack.ReasonCodes, "requested QoS 2, granted QoS 1")
	pub := p3Client(t, addr, "share-q2-pub")
	pub.send(&packet.Publish{Topic: "sq/x", QoS: packet.QoS2, PacketID: 1, Payload: []byte("x")})
	pub.read()
	assert.Equal(t, packet.QoS1, member.expectPublish().QoS)
}

// ---- §4.9 Flow control ----------------------------------------------------

// "The Client or Server MUST set its initial send quota to a non-zero value not
// exceeding the Receive Maximum" [MQTT-4.9.0-1]. A client announcing Receive
// Maximum 3 is sent six QoS 1 messages and acknowledges none: between one and
// three arrive.
func TestConformance_MQTT_4_9_0_1_InitialSendQuotaIsNonZeroAndWithinReceiveMaximum(t *testing.T) {
	addr := p3Broker(t)
	for _, max := range []uint16{1, 3} {
		id := fmt.Sprint("quota-", max)
		sub := p3Client(t, addr, id, p3ReceiveMax(max))
		sub.subscribe(id+"/#", packet.QoS1)
		pub := p3Client(t, addr, id+"-pub")
		for i := 1; i <= 6; i++ {
			pub.send(&packet.Publish{Topic: id + "/x", QoS: packet.QoS1, PacketID: uint16(i), Payload: []byte{byte(i)}})
			_, ok := pub.read().(*packet.Puback)
			require.True(t, ok)
		}
		got := p3Publishes(sub.p3Drain(700 * time.Millisecond))
		assert.GreaterOrEqual(t, len(got), 1, "Receive Maximum %d: the quota is not zero", max)
		assert.LessOrEqual(t, len(got), int(max), "Receive Maximum %d: the quota does not exceed it", max)
	}
}

// "The Client and Server MUST continue to process and respond to all other MQTT
// Control Packets even if the quota is zero" [MQTT-4.9.0-3]. With Receive Maximum
// 1 and one message unacknowledged, the broker answers PINGREQ, SUBSCRIBE,
// UNSUBSCRIBE and a QoS 1 PUBLISH, and the PUBACK it then gets releases the next.
func TestConformance_MQTT_4_9_0_3_PacketsAreStillProcessedWhileTheQuotaIsZero(t *testing.T) {
	addr := p3Broker(t)
	c := p3Client(t, addr, "zero-quota", p3ReceiveMax(1))
	c.subscribe("zq/#", packet.QoS1)
	pub := p3Client(t, addr, "zero-quota-pub")
	pub.send(&packet.Publish{Topic: "zq/x", QoS: packet.QoS1, PacketID: 1, Payload: []byte("m1")})
	pub.send(&packet.Publish{Topic: "zq/x", QoS: packet.QoS1, PacketID: 2, Payload: []byte("m2")})
	m1 := c.expectPublish() // the quota is now zero

	c.writeRaw([]byte{0xC0, 0x00})
	first, _ := c.readRawPacket()
	assert.Equal(t, byte(0xD0), first, "PINGRESP")
	assert.Equal(t, []packet.ReasonCode{packet.Success}, c.p3Suback(p3SubscribeBytes(7, "zq/other", 0x00)))
	c.writeRaw(p3UnsubscribeBytes(8, "zq/other"))
	first, _ = c.readRawPacket()
	assert.Equal(t, byte(0xB0), first, "UNSUBACK")
	c.writeRaw(p3PublishBytes(0x02, 9, "zq/elsewhere", "own"))
	first, _ = c.readRawPacket()
	assert.Equal(t, byte(0x40), first, "PUBACK for the client's own PUBLISH")

	c.send(&packet.Puback{Ack: packet.Ack{PacketID: m1.PacketID}})
	assert.Equal(t, "m2", string(c.expectPublish().Payload))
}

// ---- §4.12 Enhanced authentication ---------------------------------------

// "If the Server does not support the Authentication Method supplied by the
// Client, it MAY send a CONNACK with a Reason Code of 0x8C (Bad authentication
// method) or 0x87 (Not Authorized) ... and MUST close the Network Connection"
// [MQTT-4.12.0-1]; "The Server can reject the authentication at any point in this
// process. It MAY send a CONNACK with a Reason Code of 0x80 or above ... and MUST
// close the Network Connection" [MQTT-4.12.0-4]. Enhanced authentication is not
// offered (ROADMAP.md), so the first CONNECT is the only point there is; the
// broker sends one CONNACK with 0x8C, no AUTH packet, and closes.
func TestConformance_MQTT_4_12_0_1_AnUnsupportedAuthenticationMethodIsRefusedAndClosed(t *testing.T) {
	addr := p3Broker(t)
	c := dialRaw(t, addr)
	cp := rawConnect("auth-method", 0)
	cp.Properties.AuthenticationMethod = "SCRAM-SHA-256"
	cp.Properties.AuthenticationData = []byte("n,,n=user,r=nonce")
	c.send(cp)
	types, bodies := c.p3Rest()
	require.Equal(t, []byte{0x20}, types, "one CONNACK, no AUTH, then the close")
	assert.Equal(t, byte(0x8C), bodies[0][1], "Bad authentication method")
	assert.Equal(t, byte(0x00), bodies[0][0]&0x01, "Session Present 0 on a refusal [MQTT-3.2.2-6]")
}

// "If the Client does not include an Authentication Method in the CONNECT, the
// Server MUST NOT send an AUTH packet, and it MUST NOT send an Authentication
// Method in the CONNACK packet" [MQTT-4.12.0-6]. Every packet of an ordinary
// session, from CONNACK to the close, is inspected.
func TestConformance_MQTT_4_12_0_6_NoAuthPacketOrMethodWithoutAnAuthenticationMethod(t *testing.T) {
	addr := p3Broker(t)
	c := dialRaw(t, addr)
	c.send(rawConnect("no-auth", 0))
	connack, ok := c.read().(*packet.Connack)
	require.True(t, ok)
	assert.Empty(t, connack.Properties.AuthenticationMethod)
	assert.Nil(t, connack.Properties.AuthenticationData)

	c.writeRaw([]byte{0xC0, 0x00})
	first, _ := c.readRawPacket()
	require.Equal(t, byte(0xD0), first)
	c.subscribe("na/#", packet.QoS1)
	c.writeRaw(p3PublishBytes(0x02, 1, "na/x", "m"))
	c.writeRaw([]byte{0xF0, 0x00})                    // an AUTH the broker cannot answer in kind
	c.writeRaw(p3PublishBytes(0, 0, "na/y", "after")) // never read: the connection is closed
	types, _ := c.p3Rest()
	for _, typ := range types {
		assert.NotEqual(t, byte(15), typ>>4, "the Server sent an AUTH packet")
	}
}

// ---- §4.13 Errors ---------------------------------------------------------

// "When a Server detects a Malformed Packet or Protocol Error, and a Reason Code
// is given in the specification, it MUST close the Network Connection"
// [MQTT-4.13.1-1]. Each packet is invalid on its own terms (the cited statement
// is in brackets); the broker may send a DISCONNECT with 0x81 or 0x82 first, and
// must close. The three cases in nolocal_shared_test.go, publish_subid_test.go
// and response_topic_test.go are the ones that pin down the exact Reason Code.
func TestConformance_MQTT_4_13_1_1_MalformedPacketsAndProtocolErrorsCloseTheConnection(t *testing.T) {
	addr := p3Broker(t)
	cases := map[string][]byte{
		"SUBSCRIBE with reserved bits 0 [MQTT-3.8.1-1]":            p3Frame(0x80, append([]byte{0x00, 0x01, 0x00}, append(p3Str("a"), 0x00)...)),
		"UNSUBSCRIBE with reserved bits 0 [MQTT-3.10.1-1]":         p3Frame(0xA0, append([]byte{0x00, 0x01, 0x00}, p3Str("a")...)),
		"PUBREL with reserved bits 0 [MQTT-3.6.1-1]":               p3Frame(0x60, []byte{0x00, 0x01}),
		"PUBLISH with QoS 3 [MQTT-3.3.1-4]":                        p3Frame(0x36, append(p3Str("a"), 0x00, 0x01, 0x00)),
		"PUBLISH with DUP 1 at QoS 0 [MQTT-3.3.1-2]":               p3Frame(0x38, append(p3Str("a"), 0x00)),
		"PUBLISH QoS 1 with Packet Identifier 0":                   p3Frame(0x32, append(p3Str("a"), 0x00, 0x00, 0x00)),
		"SUBSCRIBE with no Topic Filter [MQTT-3.8.3-2]":            p3Frame(0x82, []byte{0x00, 0x01, 0x00}),
		"SUBSCRIBE with Packet Identifier 0":                       p3Frame(0x82, append([]byte{0x00, 0x00, 0x00}, append(p3Str("a"), 0x00)...)),
		"UNSUBSCRIBE with no Topic Filter [MQTT-3.10.3-2]":         p3Frame(0xA2, []byte{0x00, 0x01, 0x00}),
		"reserved packet type 0":                                   {0x00, 0x00},
		"CONNACK sent by a client":                                 p3Frame(0x20, []byte{0x00, 0x00, 0x00}),
		"SUBACK sent by a client":                                  p3Frame(0x90, []byte{0x00, 0x01, 0x00, 0x00}),
		"PINGRESP sent by a client":                                {0xD0, 0x00},
		"a Remaining Length of five bytes [MQTT-1.5.5-1]":          {0x30, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F},
		"a second CONNECT [MQTT-3.1.0-2]":                          p3Frame(0x10, append(append(p3Str("MQTT"), 0x05, 0x02, 0x00, 0x1E, 0x00), p3Str("again")...)),
		"PUBLISH with a malformed UTF-8 Topic Name [MQTT-1.5.4-1]": p3Frame(0x30, []byte{0x00, 0x02, 0xC3, 0x28, 0x00}),
	}
	for name, bad := range cases {
		t.Run(name, func(t *testing.T) {
			c := p3Client(t, addr, "bad-"+fmt.Sprint(len(name), name[:3]))
			c.writeRaw(bad)
			types, bodies := c.p3Rest()
			for i, typ := range types {
				require.Equal(t, byte(0xE0), typ, "only a DISCONNECT may precede the close")
				assert.Contains(t, []byte{0x81, 0x82}, bodies[i][0], "Malformed Packet or Protocol Error")
			}
		})
	}
}

// "If a Reason Code of 0x80 or greater is specified, then the Network Connection
// MUST be closed whether or not the CONNACK or DISCONNECT is sent"
// [MQTT-4.13.2-1]. CONNACK with an error code: an unsupported protocol version
// (0x84) and an unsupported authentication method (0x8C), each closed after the
// CONNACK; and a CONNACK that the client's Maximum Packet Size forbids sending
// at all, where the close is all there is. DISCONNECT with an error code: every
// server DISCONNECT of serverDisconnects is followed by the close.
func TestConformance_MQTT_4_13_2_1_AnErrorReasonCodeAlwaysClosesTheConnection(t *testing.T) {
	addr := p3Broker(t)

	c := dialRaw(t, addr)
	c.writeRaw(p3Frame(0x10, append(append(p3Str("MQTT"), 0x04, 0x02, 0x00, 0x1E), p3Str("v311")...)))
	types, bodies := c.p3Rest()
	require.Equal(t, []byte{0x20}, types)
	assert.Equal(t, byte(0x84), bodies[0][1], "Unsupported Protocol Version")

	c = dialRaw(t, addr)
	cp := rawConnect("closed-8c", 0)
	cp.Properties.AuthenticationMethod = "X"
	c.send(cp)
	types, bodies = c.p3Rest()
	require.Equal(t, []byte{0x20}, types)
	assert.Equal(t, byte(0x8C), bodies[0][1])

	c = dialRaw(t, addr)
	cp = connectWithMaxPacketSize("closed-unsent", 4) // smaller than any CONNACK
	cp.Properties.AuthenticationMethod = "X"
	c.send(cp)
	types, _ = c.p3Rest()
	assert.Empty(t, types, "the refusing CONNACK does not fit, so nothing is sent; the connection still closes")

	serverDisconnects(t) // asserts the close after each of seven error DISCONNECTs
}

// ---- §6 WebSocket ---------------------------------------------------------

// [MQTT-6.0.0-1], [MQTT-6.0.0-2] and [MQTT-6.0.0-4] bind a Server that offers a
// WebSocket transport. This broker offers none (ROADMAP.md), so they are not
// applicable; the one observable fact, that an HTTP upgrade request sent to the
// TCP listener does not produce an mqtt WebSocket, is shown here.
func TestConformance_MQTT_6_0_0_NoWebSocketIsOffered(t *testing.T) {
	addr := p3Broker(t)
	c := dialRaw(t, addr)
	c.writeRaw([]byte("GET /mqtt HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
		"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Protocol: mqtt\r\n\r\n"))
	_, bodies := c.p3Rest()
	for _, b := range bodies {
		assert.NotContains(t, string(b), "HTTP/", "no 101 Switching Protocols: the listener is MQTT over TCP only")
	}
}

// ---- §4.1 Session state: expiry -------------------------------------------

// "The Server MUST discard the Session State when the Network Connection is
// closed and the Session Expiry Interval has passed" [MQTT-4.1.0-2]. A session
// with an interval of 1 s subscribes and loses its connection; a message is
// published inside the interval (and queued), another after it. Reconnecting
// after 2.5 s with Clean Start 0 finds no session (Session Present 0) and no
// message, queued or new, for the old subscription.
func TestConformance_MQTT_4_1_0_2_SessionStateIsDiscardedWhenTheExpiryIntervalPasses(t *testing.T) {
	addr := p3Broker(t, func(o *natsmqtt5.Options) { o.SessionSweepInterval = 50 * time.Millisecond })
	first := dialRaw(t, addr)
	cp := rawConnect("expirer", 1)
	cp.CleanStart = true
	first.connect(cp)
	first.subscribe("ex2/#", packet.QoS1)
	first.drop()

	pub := p3Client(t, addr, "expirer-pub")
	pub.send(&packet.Publish{Topic: "ex2/inside", QoS: packet.QoS1, PacketID: 1, Payload: []byte("queued")})
	_, ok := pub.read().(*packet.Puback)
	require.True(t, ok)

	time.Sleep(2500 * time.Millisecond)
	pub.send(&packet.Publish{Topic: "ex2/after", QoS: packet.QoS1, PacketID: 2, Payload: []byte("late")})
	_, ok = pub.read().(*packet.Puback)
	require.True(t, ok)

	back := dialRaw(t, addr)
	ack := back.connect(rawConnect("expirer", 1)) // Clean Start 0
	assert.False(t, ack.SessionPresent, "the Session State was discarded")
	assert.Empty(t, p3Publishes(back.p3Drain(700*time.Millisecond)), "nothing is delivered for a subscription that expired")
}

// The Session State includes the subscriptions (§4.1.0), so a member of a shared
// subscription whose session has expired must stop receiving its share: the
// remaining member is sent every message.
func TestConformance_MQTT_4_1_0_2_AnExpiredSharedMemberStopsReceivingItsShare(t *testing.T) {
	addr := p3Broker(t, func(o *natsmqtt5.Options) { o.SessionSweepInterval = 50 * time.Millisecond })
	gone := dialRaw(t, addr)
	cp := rawConnect("share-gone", 1)
	cp.CleanStart = true
	gone.connect(cp)
	gone.subscribe("$share/g/shx/#", packet.QoS0)
	stays := p3Client(t, addr, "share-stays")
	stays.subscribe("$share/g/shx/#", packet.QoS0)
	gone.drop()

	time.Sleep(2500 * time.Millisecond)
	pub := p3Client(t, addr, "share-pub")
	const n = 20
	for i := 0; i < n; i++ {
		pub.writeRaw(p3PublishBytes(0x00, 0, "shx/m", fmt.Sprint(i)))
	}
	assert.Len(t, p3Publishes(stays.p3Drain(time.Second)), n, "the expired member took none of the messages")
}

// ---- §4.3.3 QoS 2, the broker as sender -----------------------------------

// "MUST send a PUBLISH packet containing this Packet Identifier with QoS 2 and
// DUP flag set to 0" [MQTT-4.3.3-2]; "MUST send a PUBREL packet when it receives
// a PUBREC packet from the receiver with a Reason Code value less than 0x80. This
// PUBREL packet MUST contain the same Packet Identifier as the original PUBLISH
// packet" [MQTT-4.3.3-4]; "MUST NOT re-send the PUBLISH once it has sent the
// corresponding PUBREL packet" [MQTT-4.3.3-6]. The first byte of the PUBLISH is
// 0x34 (QoS 2, DUP 0) although the publisher set DUP; after the PUBREC arrives
// a PUBREL (0x62) with the same identifier follows; when the connection is lost
// before the PUBCOMP, the resumed session sends the PUBREL again, and no PUBLISH.
func TestConformance_MQTT_4_3_3_2_QoS2SenderSequence(t *testing.T) {
	addr := p3Broker(t)
	sub := dialRaw(t, addr)
	sub.connect(rawConnect("q2-seq", 300))
	sub.subscribe("sq2/#", packet.QoS2)
	pub := p3Client(t, addr, "q2-seq-pub")
	pub.writeRaw(p3PublishBytes(0x0C, 1, "sq2/x", "m")) // QoS 2, DUP 1

	first, body := sub.readRawPacket()
	assert.Equal(t, byte(0x34), first, "PUBLISH, QoS 2, DUP 0 [MQTT-4.3.3-2]")
	topic := len(p3Str("sq2/x"))
	id := uint16(body[topic])<<8 | uint16(body[topic+1])
	require.NotZero(t, id)

	sub.writeRaw(p3AckBytes(0x50, id)) // PUBREC, Success
	first, body = sub.readRawPacket()
	assert.Equal(t, byte(0x62), first, "PUBREL")
	require.GreaterOrEqual(t, len(body), 2)
	assert.Equal(t, []byte{byte(id >> 8), byte(id)}, body[:2], "the original Packet Identifier [MQTT-4.3.3-4]")
	sub.drop()

	back := dialRaw(t, addr)
	require.True(t, back.connect(rawConnect("q2-seq", 300)).SessionPresent)
	first, body = back.readRawPacket()
	assert.Equal(t, byte(0x62), first, "the PUBREL is resent, not the PUBLISH [MQTT-4.3.3-6]")
	assert.Equal(t, []byte{byte(id >> 8), byte(id)}, body[:2])
	assert.Empty(t, p3Publishes(back.p3Drain(500*time.Millisecond)), "the PUBLISH is not sent again")
}
