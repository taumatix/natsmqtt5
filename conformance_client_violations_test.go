package natsmqtt5_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5/v2/packet"
)

// Statements of MQTT 5.0 that bind a Client alone ("the Client MUST ...") and
// that the conformance table once classed CLIENT-ONLY. A Client that breaks one
// hands the Server a packet it must not accept: a Malformed Packet or a Protocol
// Error, which §4.13.1 [MQTT-4.13.1-1] and §3.1.4 answer with a CONNACK or
// DISCONNECT carrying 0x81 or 0x82 (or the more specific code the statement's
// section names) and a closed connection. Each test sends the violating bytes
// over a real socket, written by hand from the OASIS text (read 2026-11-01)
// and not through this module's encoder, and judges what the broker does.

const (
	connectCleanStart = 0x02
	connectWillFlag   = 0x04
	connectWillRetain = 0x20
	connectPassword   = 0x40
	connectUserName   = 0x80
)

// willQoSBits places a Will QoS in bits 4 and 3 of the Connect Flags (§3.1.2.6).
func willQoSBits(qos byte) byte { return qos << 3 }

// cvID is a valid Client Identifier field.
var cvID = rawUTF8("cv-client")

// cvWill is a Will Properties length of 0, a Will Topic and a Will Payload.
func cvWill() [][]byte {
	return [][]byte{{0x00}, rawUTF8("will/topic"), {0x00, 0x01, 'w'}}
}

func cvPayload(parts ...[]byte) [][]byte { return parts }

// refusedAndClosed sends bytes in a fresh connection and requires the broker to
// refuse them (an error CONNACK or a DISCONNECT) and close the connection.
func refusedAndClosed(t *testing.T, addr string, bytes []byte) {
	t.Helper()
	c := dialRaw(t, addr)
	c.write(bytes)
	c.expectRejectedAndClosed()
}

// connectedRaw completes a CONNECT with the default properties and returns the
// client, for the tests whose violation comes after the CONNACK.
func connectedRaw(t *testing.T, addr, id string) *rawClient {
	t.Helper()
	return p3Client(t, addr, id)
}

// "After a Network Connection is established by a Client to a Server, the first
// packet sent from the Client to the Server MUST be a CONNECT packet"
// [MQTT-3.1.0-1]. Anything else first is a Protocol Error; the broker closes
// without answering it as it would a packet in a session.
func TestConnect_MQTT_3_1_0_1_AFirstPacketThatIsNotConnectClosesTheConnection(t *testing.T) {
	addr := startConformanceBroker(t)
	for name, first := range map[string][]byte{
		"PINGREQ":   {0xC0, 0x00},
		"PUBLISH":   rawQoS0Publish("a/b", "x"),
		"SUBSCRIBE": p3SubscribeBytes(1, "a/b", 0),
		"PUBACK":    p3AckBytes(0x40, 1),
		"AUTH":      {0xF0, 0x00},
	} {
		t.Run(name, func(t *testing.T) {
			refusedAndClosed(t, addr, first)
		})
	}
}

// "The protocol name MUST be the UTF-8 String "MQTT". If the Server does not
// want to accept the CONNECT ... it MAY send a CONNACK packet with Reason Code
// of 0x84 (Unsupported Protocol Version), and then it MUST close the Network
// Connection" [MQTT-3.1.2-1].
func TestConnect_MQTT_3_1_2_1_AWrongProtocolNameIsRefusedAndTheConnectionClosed(t *testing.T) {
	addr := startConformanceBroker(t)
	for _, name := range []string{"MQTX", "mqtt", "MQIsdp", ""} {
		c := dialRaw(t, addr)
		c.write(rawConnectBytes(name, 5, connectCleanStart, cvID))
		c.expectRejectedAndClosed()
	}
}

// "If the Will Flag is set to 0, then the Will QoS MUST be set to 0 (0x00)"
// [MQTT-3.1.2-11]; "If the Will Flag is set to 1, the value of Will QoS can be
// 0 (0x00), 1 (0x01), or 2 (0x02)" [MQTT-3.1.2-12], and "A value of 3 (0x03) is
// a Malformed Packet"; "If the Will Flag is set to 0, then Will Retain MUST be
// set to 0" [MQTT-3.1.2-13].
func TestConnect_MQTT_3_1_2_11_12_13_WillQoSAndRetainFollowTheWillFlag(t *testing.T) {
	addr := startConformanceBroker(t)

	for _, qos := range []byte{1, 2, 3} {
		refusedAndClosed(t, addr, rawConnectBytes("MQTT", 5, connectCleanStart|willQoSBits(qos), cvID)) // 3.1.2-11
	}
	refusedAndClosed(t, addr, rawConnectBytes("MQTT", 5, connectCleanStart|connectWillRetain, cvID)) // 3.1.2-13

	// 3.1.2-12: with the Will Flag set, 0, 1 and 2 are all accepted, and 3 is not.
	for _, qos := range []byte{0, 1, 2} {
		c := dialRaw(t, addr)
		typ, _, reason := c.handshake(rawConnectBytes("MQTT", 5, connectCleanStart|connectWillFlag|willQoSBits(qos), append([][]byte{cvID}, cvWill()...)...))
		assert.Equal(t, byte(0x20), typ)
		assert.Equal(t, byte(0x00), reason, "Will QoS %d with the Will Flag set is allowed [MQTT-3.1.2-12]", qos)
	}
	refusedAndClosed(t, addr, rawConnectBytes("MQTT", 5, connectCleanStart|connectWillFlag|willQoSBits(3), append([][]byte{cvID}, cvWill()...)...))
}

// "If the User Name Flag is set to 0, a User Name MUST NOT be present in the
// Payload" [MQTT-3.1.2-16]; "If the User Name Flag is set to 1, a User Name MUST
// be present" [MQTT-3.1.2-17]; the same for the Password Flag [MQTT-3.1.2-18],
// [MQTT-3.1.2-19]. The payload is parsed from the flags, so a field the flags do
// not announce is left over, and a field they announce that is missing is a read
// past the end: both are Malformed Packets.
func TestConnect_MQTT_3_1_2_16_17_18_19_UserNameAndPasswordPresenceFollowTheirFlags(t *testing.T) {
	addr := startConformanceBroker(t)
	user, pass := rawUTF8("alice"), []byte{0x00, 0x02, 'p', 'w'}
	for name, tc := range map[string]struct {
		flags   byte
		payload [][]byte
	}{
		"3.1.2-16 a User Name with the flag 0":            {connectCleanStart, cvPayload(cvID, user)},
		"3.1.2-17 the flag 1 and no User Name":            {connectCleanStart | connectUserName, cvPayload(cvID)},
		"3.1.2-18 a Password with the flag 0":             {connectCleanStart, cvPayload(cvID, pass)},
		"3.1.2-18 a Password with the flag 0, after name": {connectCleanStart | connectUserName, cvPayload(cvID, user, pass)},
		"3.1.2-19 the flag 1 and no Password":             {connectCleanStart | connectPassword, cvPayload(cvID)},
		"3.1.2-19 the flag 1, a User Name and no Password": {
			connectCleanStart | connectUserName | connectPassword, cvPayload(cvID, user)},
	} {
		t.Run(name, func(t *testing.T) {
			refusedAndClosed(t, addr, rawConnectBytes("MQTT", 5, tc.flags, tc.payload...))
		})
	}
}

// "The ClientID MUST be present and is the first field in the CONNECT packet
// Payload" [MQTT-3.1.3-3]: a CONNECT that stops after the variable header has
// no Client Identifier (a zero-length one is a field, so it is not this case).
func TestConnect_MQTT_3_1_3_3_ACONNECTWithoutAClientIdentifierFieldIsMalformed(t *testing.T) {
	addr := startConformanceBroker(t)
	refusedAndClosed(t, addr, rawConnectBytes("MQTT", 5, connectCleanStart))
	refusedAndClosed(t, addr, rawConnectBytes("MQTT", 5, connectCleanStart, []byte{0x00})) // half a length
}

// "The ClientID MUST be a UTF-8 Encoded String" [MQTT-3.1.3-4], "The Will Topic
// MUST be" one [MQTT-3.1.3-11], and so MUST the User Name [MQTT-3.1.3-12]: bytes
// that are not well-formed UTF-8, or that hold U+0000, are a Malformed Packet
// (§1.5.4, [MQTT-1.5.4-1], [MQTT-1.5.4-2]).
func TestConnect_MQTT_3_1_3_4_11_12_TheStringsOfTheConnectPayloadMustBeUTF8(t *testing.T) {
	addr := startConformanceBroker(t)
	bad := map[string]string{"overlong": "a\xC0\x80b", "lone continuation": "a\x80b", "surrogate": "a\xED\xA0\x80b", "NUL": "a\x00b"}
	for name, s := range bad {
		t.Run("ClientID "+name, func(t *testing.T) {
			refusedAndClosed(t, addr, rawConnectBytes("MQTT", 5, connectCleanStart, rawUTF8(s)))
		})
		t.Run("Will Topic "+name, func(t *testing.T) {
			refusedAndClosed(t, addr, rawConnectBytes("MQTT", 5, connectCleanStart|connectWillFlag,
				cvID, []byte{0x00}, rawUTF8(s), []byte{0x00, 0x01, 'w'}))
		})
		t.Run("User Name "+name, func(t *testing.T) {
			refusedAndClosed(t, addr, rawConnectBytes("MQTT", 5, connectCleanStart|connectUserName, cvID, rawUTF8(s)))
		})
	}
}

// "These fields, if present, MUST appear in the order Client Identifier, Will
// Properties, Will Topic, Will Payload, User Name, Password" [MQTT-3.1.3-1]. The
// payload carries no markers, so a Client that reorders hands the broker bytes
// that parse as something else: here a User Name where the Will Properties
// belong, and a Will Topic where the User Name does.
func TestConnect_MQTT_3_1_3_1_AReorderedConnectPayloadIsMalformed(t *testing.T) {
	addr := startConformanceBroker(t)
	flags := byte(connectCleanStart | connectWillFlag | connectUserName)
	will := cvWill()

	// User Name first, then the Will fields: the name's length byte 0x00 reads as
	// "no Will Properties", its second length byte as the high byte of the Will
	// Topic's length, and the text that follows makes that length run past the end.
	refusedAndClosed(t, addr, rawConnectBytes("MQTT", 5, flags,
		append(cvPayload(cvID, rawUTF8("alice")), will...)...))

	// Will Topic and Will Payload before the Will Properties.
	refusedAndClosed(t, addr, rawConnectBytes("MQTT", 5, connectCleanStart|connectWillFlag,
		cvID, rawUTF8("will/topic"), []byte{0x00}, []byte{0x00, 0x01, 'w'}))

	// The Client Identifier is not first: a Will Properties length and a topic go
	// ahead of it.
	refusedAndClosed(t, addr, rawConnectBytes("MQTT", 5, connectCleanStart|connectWillFlag,
		[]byte{0x00}, rawUTF8("will/topic"), []byte{0x00, 0x01, 'w'}, cvID))
}

// "If Keep Alive is non-zero and in the absence of sending any other MQTT
// Control Packets, the Client MUST send a PINGREQ packet" [MQTT-3.1.2-20]. The
// broker holds a Client that does to the contract and ends one that does not:
// after 1.5 times the Keep Alive of silence it closes the connection
// [MQTT-3.1.2-22].
func TestConnect_MQTT_3_1_2_20_AClientThatNeverSendsAPingreqIsDisconnected(t *testing.T) {
	addr := startConformanceBroker(t)

	quiet := dialRaw(t, addr)
	cp := rawConnect("cv-quiet", 0)
	cp.CleanStart = true
	cp.KeepAlive = 1
	quiet.connect(cp)

	pinging := dialRaw(t, addr)
	cp = rawConnect("cv-pinging", 0)
	cp.CleanStart = true
	cp.KeepAlive = 1
	pinging.connect(cp)

	deadline := time.Now().Add(2500 * time.Millisecond)
	for time.Now().Before(deadline) {
		pinging.send(&packet.Pingreq{})
		_, ok := pinging.read().(*packet.Pingresp)
		require.True(t, ok, "a Client that sends PINGREQ inside its Keep Alive keeps the connection")
		time.Sleep(400 * time.Millisecond)
	}
	d := quiet.closedWith()
	require.NotNil(t, d, "the silent Client is told why")
	assert.Equal(t, packet.ReasonCode(0x8D), d.ReasonCode, "Keep Alive timeout")
}
