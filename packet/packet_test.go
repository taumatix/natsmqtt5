package packet

import (
	"bufio"
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// decodeBytes runs the framing path over raw wire bytes, the same way a
// connection would.
func decodeBytes(t *testing.T, raw []byte) (Packet, error) {
	t.Helper()
	return Read(bufio.NewReader(bytes.NewReader(raw)), 0)
}

// mustEncode asserts that p serialises without error and returns the bytes.
func mustEncode(t *testing.T, p Packet) []byte {
	t.Helper()
	b, err := Encode(p)
	require.NoError(t, err)
	return b
}

// TestConnectVariableHeaderMatchesSpecExample builds the CONNECT variable
// header of MQTT-5.0 Figure 3-6 (§3.1.2.12) and asserts the literal bytes the
// figure specifies: protocol name "MQTT", version 5, connect flags 0xCE
// (User Name, Password, Will QoS 1, Will Flag, Clean Start), Keep Alive 10,
// and a five-byte property block holding Session Expiry Interval 10.
func TestConnectVariableHeaderMatchesSpecExample(t *testing.T) {
	c := &Connect{
		ClientID:   "",
		CleanStart: true,
		KeepAlive:  10,
		Username:   "u",
		Password:   []byte("p"),
		Will:       &Will{Topic: "w", Payload: []byte("bye"), QoS: QoS1},
		Properties: &Properties{SessionExpiryInterval: Uint32(10)},
	}
	raw := mustEncode(t, c)

	wantHeader := []byte{
		byte(CONNECT) << 4, // fixed header byte 1: type 1, flags 0
	}
	assert.Equal(t, wantHeader[0], raw[0], "fixed header byte 1")

	// Skip the fixed header (byte 1 plus a one-byte Remaining Length here).
	varHeader := raw[2:]
	wantVarHeader := []byte{
		0x00, 0x04, 'M', 'Q', 'T', 'T', // Protocol Name (§3.1.2.1)
		0x05,       // Protocol Version (§3.1.2.2)
		0xCE,       // Connect Flags (§3.1.2.3, Figure 3-6)
		0x00, 0x0A, // Keep Alive = 10 (§3.1.2.10)
		0x05,                         // Property Length = 5
		0x11, 0x00, 0x00, 0x00, 0x0A, // Session Expiry Interval = 10
	}
	assert.Equal(t, wantVarHeader, varHeader[:len(wantVarHeader)],
		"CONNECT variable header (MQTT-5.0 Figure 3-6)")
}

// TestPublishVariableHeaderMatchesSpecExample reproduces MQTT-5.0 Figure 3-9
// (§3.3.2): Topic Name "a/b", Packet Identifier 10, no properties.
func TestPublishVariableHeaderMatchesSpecExample(t *testing.T) {
	p := &Publish{Topic: "a/b", QoS: QoS1, PacketID: 10}
	raw := mustEncode(t, p)

	want := []byte{
		byte(PUBLISH)<<4 | 0x02, // type 3, QoS 1 in bits 2-1
		0x08,                    // Remaining Length
		0x00, 0x03, 'a', '/', 'b',
		0x00, 0x0A, // Packet Identifier = 10
		0x00, // Property Length = 0
	}
	assert.Equal(t, want, raw, "PUBLISH packet (MQTT-5.0 Figure 3-9)")
}

// TestSubscribeMatchesSpecExample reproduces MQTT-5.0 Figure 3-19 and
// Figure 3-21 (§3.8.2, §3.8.3): Packet Identifier 10, no properties, filters
// "a/b" with options 0x01 and "c/d" with options 0x02.
func TestSubscribeMatchesSpecExample(t *testing.T) {
	s := &Subscribe{
		PacketID: 10,
		Subscriptions: []Subscription{
			{Filter: "a/b", QoS: QoS1},
			{Filter: "c/d", QoS: QoS2},
		},
	}
	raw := mustEncode(t, s)

	want := []byte{
		byte(SUBSCRIBE)<<4 | 0x02, // reserved flags 0,0,1,0 [MQTT-3.8.1-1]
		0x0F,                      // Remaining Length: 2 + 1 + 6 + 6
		0x00, 0x0A,                // Packet Identifier = 10
		0x00, // Property Length = 0
		0x00, 0x03, 'a', '/', 'b', 0x01,
		0x00, 0x03, 'c', '/', 'd', 0x02,
	}
	assert.Equal(t, want, raw, "SUBSCRIBE packet (MQTT-5.0 Figures 3-19, 3-21)")
}

// The low nibble of byte 1 is fixed for every type except PUBLISH
// (MQTT-5.0 Table 2-2). Any other value is a Malformed Packet
// [MQTT-2.1.3-1].
func TestFixedHeaderReservedFlags(t *testing.T) {
	wire := map[Type]byte{
		CONNECT: 0x10, CONNACK: 0x20, PUBACK: 0x40, PUBREC: 0x50,
		PUBREL: 0x62, PUBCOMP: 0x70, SUBSCRIBE: 0x82, SUBACK: 0x90,
		UNSUBSCRIBE: 0xA2, UNSUBACK: 0xB0, PINGREQ: 0xC0, PINGRESP: 0xD0,
		DISCONNECT: 0xE0, AUTH: 0xF0,
	}
	for typ, first := range wire {
		assert.Equal(t, first, byte(typ)<<4|reservedFlags[typ],
			"%s first byte (MQTT-5.0 Table 2-2)", typ)
	}

	t.Run("wrong flags are malformed", func(t *testing.T) {
		// PUBREL with flags 0x00 instead of the required 0x02.
		_, err := decodeBytes(t, []byte{0x60, 0x02, 0x00, 0x01})
		assert.ErrorIs(t, err, ErrMalformed)
	})
}

func TestRoundTrip(t *testing.T) {
	packets := map[string]Packet{
		"CONNECT minimal": &Connect{ClientID: "c1", CleanStart: true, KeepAlive: 60},
		"CONNECT full": &Connect{
			ClientID:   "sensor-7",
			CleanStart: false,
			KeepAlive:  30,
			Username:   "alice",
			Password:   []byte("s3cret"),
			Will: &Will{
				Topic: "status/sensor-7", Payload: []byte("offline"), QoS: QoS1, Retain: true,
				Properties: &Properties{
					WillDelayInterval: Uint32(30),
					PayloadFormat:     Byte(1),
					ContentType:       "text/plain",
					User:              []UserProperty{{Key: "site", Value: "paris"}},
				},
			},
			Properties: &Properties{
				SessionExpiryInterval: Uint32(3600),
				ReceiveMaximum:        Uint16(20),
				MaximumPacketSize:     Uint32(65536),
				TopicAliasMaximum:     Uint16(10),
				RequestResponseInfo:   Byte(1),
				RequestProblemInfo:    Byte(0),
				AuthenticationMethod:  "SCRAM-SHA-256",
				AuthenticationData:    []byte{0x01, 0x02},
				User:                  []UserProperty{{Key: "a", Value: "1"}, {Key: "a", Value: "2"}},
			},
		},
		"CONNACK": &Connack{
			SessionPresent: true,
			ReasonCode:     Success,
			Properties: &Properties{
				SessionExpiryInterval: Uint32(120),
				ReceiveMaximum:        Uint16(100),
				MaximumQoS:            Byte(1),
				RetainAvailable:       Byte(1),
				AssignedClientID:      "auto-42",
				ServerKeepAlive:       Uint16(45),
				TopicAliasMaximum:     Uint16(8),
				WildcardSubAvailable:  Byte(1),
				SubIDAvailable:        Byte(1),
				SharedSubAvailable:    Byte(1),
				ResponseInformation:   "reply/",
			},
		},
		"CONNACK rejected": &Connack{ReasonCode: NotAuthorized,
			Properties: &Properties{ReasonString: "bad credentials"}},
		"PUBLISH QoS0": &Publish{Topic: "a/b", Payload: []byte("hi")},
		"PUBLISH QoS2 with properties": &Publish{
			Topic: "sensors/temp", QoS: QoS2, PacketID: 7, Retain: true, Dup: true,
			Payload: []byte(`{"c":21.5}`),
			Properties: &Properties{
				PayloadFormat:           Byte(1),
				MessageExpiryInterval:   Uint32(60),
				ContentType:             "application/json",
				ResponseTopic:           "reply/1",
				CorrelationData:         []byte{0xDE, 0xAD},
				TopicAlias:              Uint16(3),
				SubscriptionIdentifiers: []int{1, 268435455},
				User:                    []UserProperty{{Key: "unit", Value: "celsius"}},
			},
		},
		"PUBLISH empty payload": &Publish{Topic: "t", Payload: nil},
		"PUBACK short form":     &Puback{Ack{PacketID: 5}},
		"PUBACK with reason":    &Puback{Ack{PacketID: 5, ReasonCode: NoMatchingSubscribers}},
		"PUBACK with properties": &Puback{Ack{PacketID: 5, ReasonCode: NotAuthorized,
			Properties: &Properties{ReasonString: "nope"}}},
		"PUBREC":  &Pubrec{Ack{PacketID: 9}},
		"PUBREL":  &Pubrel{Ack{PacketID: 9}},
		"PUBCOMP": &Pubcomp{Ack{PacketID: 9, ReasonCode: PacketIdentifierNotFound}},
		"SUBSCRIBE": &Subscribe{PacketID: 1,
			Properties: &Properties{SubscriptionIdentifiers: []int{42}},
			Subscriptions: []Subscription{
				{Filter: "a/#", QoS: QoS2, NoLocal: true, RetainAsPublished: true, RetainHandling: RetainSendNever},
				{Filter: "$share/grp/b/+", QoS: QoS1},
			}},
		"SUBACK":            &Suback{PacketID: 1, ReasonCodes: []ReasonCode{GrantedQoS2, GrantedQoS0, TopicFilterInvalid}},
		"UNSUBSCRIBE":       &Unsubscribe{PacketID: 2, Filters: []string{"a/#", "b"}},
		"UNSUBACK":          &Unsuback{PacketID: 2, ReasonCodes: []ReasonCode{Success, NoSubscriptionExisted}},
		"PINGREQ":           &Pingreq{},
		"PINGRESP":          &Pingresp{},
		"DISCONNECT normal": &Disconnect{},
		"DISCONNECT with reason and properties": &Disconnect{
			ReasonCode: SessionTakenOver,
			Properties: &Properties{ReasonString: "taken over", ServerReference: "mqtt://other:1883"},
		},
		"AUTH":              &Auth{ReasonCode: ContinueAuthentication, Properties: &Properties{AuthenticationMethod: "SCRAM-SHA-256", AuthenticationData: []byte{0x09}}},
		"AUTH bare success": &Auth{},
	}

	for name, want := range packets {
		t.Run(name, func(t *testing.T) {
			raw := mustEncode(t, want)

			n, err := EncodedLen(want)
			require.NoError(t, err)
			assert.Equal(t, len(raw), n, "EncodedLen must agree with Encode")

			got, err := decodeBytes(t, raw)
			require.NoError(t, err)
			assert.Equal(t, want.Type(), got.Type())

			// Re-encoding the decoded packet must produce identical bytes.
			assert.Equal(t, raw, mustEncode(t, got), "re-encode of decoded packet")
		})
	}
}

// PUBACK, PUBREC, PUBREL and PUBCOMP omit the Reason Code when it is Success
// and there are no properties: "If the Remaining Length is 2, then there is no
// Reason Code and the value of 0x00 (Success) is used" (MQTT-5.0 §3.4.2.1).
func TestAckShortForms(t *testing.T) {
	t.Run("success with no properties is two bytes", func(t *testing.T) {
		raw := mustEncode(t, &Puback{Ack{PacketID: 0x1234}})
		assert.Equal(t, []byte{0x40, 0x02, 0x12, 0x34}, raw)
	})
	t.Run("reason code without properties is three bytes", func(t *testing.T) {
		raw := mustEncode(t, &Puback{Ack{PacketID: 1, ReasonCode: NotAuthorized}})
		assert.Equal(t, []byte{0x40, 0x03, 0x00, 0x01, 0x87}, raw)
	})
	t.Run("two-byte form decodes to Success", func(t *testing.T) {
		p, err := decodeBytes(t, []byte{0x40, 0x02, 0x12, 0x34})
		require.NoError(t, err)
		ack, ok := p.(*Puback)
		require.True(t, ok)
		assert.Equal(t, uint16(0x1234), ack.PacketID)
		assert.Equal(t, Success, ack.ReasonCode)
	})
	t.Run("three-byte form decodes with an empty property set", func(t *testing.T) {
		p, err := decodeBytes(t, []byte{0x40, 0x03, 0x00, 0x01, 0x87})
		require.NoError(t, err)
		ack, ok := p.(*Puback)
		require.True(t, ok)
		assert.Equal(t, NotAuthorized, ack.ReasonCode)
		assert.Empty(t, ack.Properties.User)
	})
}

// "If the Remaining Length is less than 1 the value of 0x00 (Normal
// disconnection) is used" (MQTT-5.0 §3.14.2.1).
func TestDisconnectShortForm(t *testing.T) {
	assert.Equal(t, []byte{0xE0, 0x00}, mustEncode(t, &Disconnect{}))

	p, err := decodeBytes(t, []byte{0xE0, 0x00})
	require.NoError(t, err)
	d, ok := p.(*Disconnect)
	require.True(t, ok)
	assert.Equal(t, NormalDisconnection, d.ReasonCode)
}

func TestDecodeRejectsProtocolViolations(t *testing.T) {
	cases := map[string]struct {
		raw     []byte
		wantErr error
	}{
		"reserved packet type 0": {
			raw: []byte{0x00, 0x00}, wantErr: ErrMalformed,
		},
		"PUBLISH with both QoS bits set": {
			raw: []byte{0x36, 0x05, 0x00, 0x01, 'a', 0x00, 0x00}, wantErr: ErrMalformed,
		},
		"PUBLISH with DUP at QoS 0": {
			raw: []byte{0x38, 0x04, 0x00, 0x01, 'a', 0x00}, wantErr: ErrMalformed,
		},
		"CONNECT with a reserved flag set": {
			raw: append([]byte{0x10, 0x0B, 0x00, 0x04, 'M', 'Q', 'T', 'T', 0x05, 0x01},
				0x00, 0x00, 0x00), wantErr: ErrMalformed,
		},
		"SUBSCRIBE with no filters": {
			raw: []byte{0x82, 0x03, 0x00, 0x01, 0x00}, wantErr: ErrProtocol,
		},
		"UNSUBSCRIBE with no filters": {
			raw: []byte{0xA2, 0x03, 0x00, 0x01, 0x00}, wantErr: ErrProtocol,
		},
		"PINGREQ with a body": {
			raw: []byte{0xC0, 0x01, 0x00}, wantErr: ErrMalformed,
		},
		"non-minimal Remaining Length": {
			raw: []byte{0xC0, 0x80, 0x00}, wantErr: ErrMalformed,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := decodeBytes(t, tc.raw)
			assert.ErrorIs(t, err, tc.wantErr)
		})
	}
}

// A CONNECT naming a protocol version other than 5 is reported distinctly so
// the server can answer 0x84 (Unsupported Protocol Version) rather than
// dropping the connection [MQTT-3.1.2-2].
func TestConnectWrongProtocolVersion(t *testing.T) {
	raw := []byte{0x10, 0x0D, 0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x02, 0x00, 0x3C, 0x00, 0x00, 0x00}
	_, err := decodeBytes(t, raw)

	var verErr *ErrUnsupportedProtocolVersion
	require.ErrorAs(t, err, &verErr)
	assert.Equal(t, byte(4), verErr.Version)
}

func TestReadEnforcesMaximumPacketSize(t *testing.T) {
	raw := mustEncode(t, &Publish{Topic: "some/long/topic", Payload: bytes.Repeat([]byte("x"), 100)})

	_, err := Read(bufio.NewReader(bytes.NewReader(raw)), uint32(len(raw)-1))
	assert.ErrorIs(t, err, ErrPacketTooLarge)

	_, err = Read(bufio.NewReader(bytes.NewReader(raw)), uint32(len(raw)))
	assert.NoError(t, err, "a packet exactly at the limit is accepted")
}

func TestReadTruncatedStreamReportsUnexpectedEOF(t *testing.T) {
	raw := mustEncode(t, &Publish{Topic: "a/b", Payload: []byte("hello")})

	_, err := Read(bufio.NewReader(bytes.NewReader(raw[:len(raw)-2])), 0)
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

func TestReadEmptyStreamReportsEOF(t *testing.T) {
	_, err := Read(bufio.NewReader(bytes.NewReader(nil)), 0)
	assert.ErrorIs(t, err, io.EOF)
}
