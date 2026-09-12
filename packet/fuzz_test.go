package packet

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

// FuzzRead checks that no byte sequence makes the decoder panic or read out of
// bounds, and that anything it does accept re-encodes and re-decodes to the
// same thing. A broker parses bytes from unauthenticated peers before it has
// decided whether to trust them, so the decoder is the first thing an attacker
// reaches.
func FuzzRead(f *testing.F) {
	seeds := []Packet{
		&Connect{ClientID: "c", CleanStart: true, KeepAlive: 10},
		&Connack{ReasonCode: Success},
		&Publish{Topic: "a/b", QoS: QoS1, PacketID: 1, Payload: []byte("x")},
		&Puback{Ack{PacketID: 1}},
		&Subscribe{PacketID: 1, Subscriptions: []Subscription{{Filter: "a/#", QoS: QoS2}}},
		&Suback{PacketID: 1, ReasonCodes: []ReasonCode{GrantedQoS2}},
		&Unsubscribe{PacketID: 1, Filters: []string{"a/#"}},
		&Unsuback{PacketID: 1, ReasonCodes: []ReasonCode{Success}},
		&Pingreq{}, &Pingresp{}, &Disconnect{}, &Auth{},
	}
	for _, p := range seeds {
		raw, err := Encode(p)
		require.NoError(f, err)
		f.Add(raw)
	}
	f.Add([]byte{})
	f.Add([]byte{0x00, 0x00})
	f.Add([]byte{0x10, 0xFF, 0xFF, 0xFF, 0xFF})

	f.Fuzz(func(t *testing.T, data []byte) {
		p, err := Read(bufio.NewReader(bytes.NewReader(data)), 0)
		if err != nil {
			return
		}
		raw, err := Encode(p)
		if err != nil {
			t.Fatalf("decoded a %s that will not re-encode: %v", p.Type(), err)
		}
		again, err := Read(bufio.NewReader(bytes.NewReader(raw)), 0)
		if err != nil {
			t.Fatalf("re-encoded %s will not decode: %v", p.Type(), err)
		}
		raw2, err := Encode(again)
		if err != nil {
			t.Fatalf("second encode of %s failed: %v", p.Type(), err)
		}
		if !bytes.Equal(raw, raw2) {
			t.Fatalf("%s does not round-trip stably:\nfirst  % X\nsecond % X", p.Type(), raw, raw2)
		}
	})
}
