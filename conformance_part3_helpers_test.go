package natsmqtt5_test

import (
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// Helpers for conformance_part3_test.go. Requests that need a byte the codec
// would refuse (a null character in a topic, reserved header bits set) are
// written by hand from MQTT-5.0 §2 and §3, so the broker is judged against the
// specification's layout and not against this module's own encoder.

// p3Broker starts an embedded NATS server with JetStream and a broker on it.
func p3Broker(t *testing.T, customise ...func(*natsmqtt5.Options)) string {
	t.Helper()
	return startBroker(t, startNATS(t), customise...)
}

// p3Client connects a raw client with a Clean Start session that ends with its
// connection, after tweak has adjusted the CONNECT.
func p3Client(t *testing.T, addr, id string, tweak ...func(*packet.Connect)) *rawClient {
	t.Helper()
	c := dialRaw(t, addr)
	cp := rawConnect(id, 0)
	cp.CleanStart = true
	for _, fn := range tweak {
		fn(cp)
	}
	c.connect(cp)
	return c
}

// p3ReceiveMax sets the Receive Maximum a client announces.
func p3ReceiveMax(n uint16) func(*packet.Connect) {
	return func(cp *packet.Connect) { cp.Properties.ReceiveMaximum = packet.Uint16(n) }
}

// p3Frame is a control packet: the fixed header byte, the Remaining Length as a
// variable byte integer (§1.5.5), then the body.
func p3Frame(first byte, body []byte) []byte {
	out := []byte{first}
	n := len(body)
	for {
		b := byte(n % 128)
		n /= 128
		if n > 0 {
			b |= 0x80
		}
		out = append(out, b)
		if n == 0 {
			break
		}
	}
	return append(out, body...)
}

// p3Str is a UTF-8 Encoded String: a two byte length, then the bytes (§1.5.4).
func p3Str(s string) []byte {
	return append([]byte{byte(len(s) >> 8), byte(len(s))}, s...)
}

func (c *rawClient) writeRaw(b []byte) {
	c.t.Helper()
	require.NoError(c.t, c.nc.SetWriteDeadline(time.Now().Add(5*time.Second)))
	_, err := c.nc.Write(b)
	require.NoError(c.t, err)
}

// p3SubscribeBytes is a SUBSCRIBE for one filter with the given Subscription
// Options byte (§3.8), flags 0b0010 as §3.8.1 requires.
func p3SubscribeBytes(id uint16, filter string, opts byte) []byte {
	body := []byte{byte(id >> 8), byte(id), 0x00}
	body = append(body, p3Str(filter)...)
	body = append(body, opts)
	return p3Frame(0x82, body)
}

// p3UnsubscribeBytes is an UNSUBSCRIBE for the filters (§3.10).
func p3UnsubscribeBytes(id uint16, filters ...string) []byte {
	body := []byte{byte(id >> 8), byte(id), 0x00}
	for _, f := range filters {
		body = append(body, p3Str(f)...)
	}
	return p3Frame(0xA2, body)
}

// p3PublishBytes is a PUBLISH with no properties (§3.3). flags is the low
// nibble of byte 1 (DUP, QoS, RETAIN); a Packet Identifier is written when the
// QoS bits are non-zero.
func p3PublishBytes(flags byte, id uint16, topic, payload string) []byte {
	body := p3Str(topic)
	if flags&0x06 != 0 {
		body = append(body, byte(id>>8), byte(id))
	}
	body = append(body, 0x00)
	body = append(body, payload...)
	return p3Frame(0x30|flags, body)
}

// p3AckBytes is a PUBACK, PUBREC, PUBREL or PUBCOMP with no Reason Code
// (Remaining Length 2, §3.4.2.1).
func p3AckBytes(first byte, id uint16) []byte {
	return p3Frame(first, []byte{byte(id >> 8), byte(id)})
}

// p3Suback sends a SUBSCRIBE and returns the SUBACK's Reason Codes.
func (c *rawClient) p3Suback(subscribe []byte) []packet.ReasonCode {
	c.t.Helper()
	c.writeRaw(subscribe)
	ack, ok := c.read().(*packet.Suback)
	require.True(c.t, ok, "a SUBSCRIBE is answered with a SUBACK")
	return ack.ReasonCodes
}

// p3Rest reads what the broker sends until it closes the connection and returns
// the first byte of every packet, so a test can say which packets came and that
// none followed the last.
func (c *rawClient) p3Rest() (types []byte, bodies [][]byte) {
	c.t.Helper()
	for {
		require.NoError(c.t, c.nc.SetReadDeadline(time.Now().Add(5*time.Second)))
		first, err := c.r.ReadByte()
		if err != nil {
			if ne, ok := err.(interface{ Timeout() bool }); ok {
				require.False(c.t, ne.Timeout(), "the broker left the connection open")
			}
			return types, bodies
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
		types = append(types, first)
		bodies = append(bodies, body)
	}
}

// p3Drain reads packets for window and returns them, for a test that asserts a
// packet did not come.
func (c *rawClient) p3Drain(window time.Duration) []packet.Packet {
	c.t.Helper()
	var got []packet.Packet
	deadline := time.Now().Add(window)
	for {
		require.NoError(c.t, c.nc.SetReadDeadline(deadline))
		p, err := packet.Read(c.r, 0)
		if err != nil {
			return got
		}
		got = append(got, p)
	}
}

// p3Publishes is the PUBLISH packets among got.
func p3Publishes(got []packet.Packet) []*packet.Publish {
	var out []*packet.Publish
	for _, p := range got {
		if pub, ok := p.(*packet.Publish); ok {
			out = append(out, pub)
		}
	}
	return out
}
