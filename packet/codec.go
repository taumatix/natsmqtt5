package packet

import (
	"errors"
	"fmt"
	"math"
	"unicode/utf8"
)

// Errors returned by the decoder. Callers map these onto the reason codes the
// spec prescribes: a malformed packet is 0x81, a protocol error is 0x82.
var (
	// ErrMalformed reports a packet the spec calls a Malformed Packet: the
	// bytes cannot be parsed as the declared type at all (MQTT-5.0 §4.13).
	ErrMalformed = errors.New("mqtt: malformed packet")
	// ErrProtocol reports a Protocol Error: the packet parses, but says
	// something the spec forbids (MQTT-5.0 §4.13).
	ErrProtocol = errors.New("mqtt: protocol error")
	// ErrPacketTooLarge reports a packet whose declared Remaining Length
	// exceeds the limit the receiver advertised (MQTT-5.0 §3.1.2.11.4).
	ErrPacketTooLarge = errors.New("mqtt: packet too large")
)

func malformed(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrMalformed, fmt.Sprintf(format, args...))
}

func protocolError(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrProtocol, fmt.Sprintf(format, args...))
}

// MaxVarByteInt is the largest value a Variable Byte Integer can carry
// (MQTT-5.0 §1.5.5, Table 1-1).
const MaxVarByteInt = 268435455

// reader walks a decoded packet body. Every read is bounds-checked, so a
// truncated or lying length field surfaces as ErrMalformed rather than a panic.
type reader struct {
	buf []byte
	pos int
}

func (r *reader) remaining() int { return len(r.buf) - r.pos }

func (r *reader) empty() bool { return r.pos >= len(r.buf) }

func (r *reader) byte() (byte, error) {
	if r.remaining() < 1 {
		return 0, malformed("truncated: want 1 byte, have %d", r.remaining())
	}
	b := r.buf[r.pos]
	r.pos++
	return b, nil
}

func (r *reader) uint16() (uint16, error) {
	if r.remaining() < 2 {
		return 0, malformed("truncated Two Byte Integer: have %d", r.remaining())
	}
	v := uint16(r.buf[r.pos])<<8 | uint16(r.buf[r.pos+1])
	r.pos += 2
	return v, nil
}

func (r *reader) uint32() (uint32, error) {
	if r.remaining() < 4 {
		return 0, malformed("truncated Four Byte Integer: have %d", r.remaining())
	}
	v := uint32(r.buf[r.pos])<<24 | uint32(r.buf[r.pos+1])<<16 |
		uint32(r.buf[r.pos+2])<<8 | uint32(r.buf[r.pos+3])
	r.pos += 4
	return v, nil
}

// varByteInt decodes a Variable Byte Integer (MQTT-5.0 §1.5.5). At most four
// bytes; the encoder MUST use the minimum number of bytes [MQTT-1.5.5-1], so a
// non-minimal encoding is malformed.
func (r *reader) varByteInt() (int, error) {
	var value, multiplier int
	for i := 0; i < 4; i++ {
		b, err := r.byte()
		if err != nil {
			return 0, malformed("truncated Variable Byte Integer")
		}
		value += int(b&0x7F) << multiplier
		if b&0x80 == 0 {
			if i > 0 && b == 0 {
				return 0, malformed("non-minimal Variable Byte Integer encoding")
			}
			return value, nil
		}
		multiplier += 7
	}
	return 0, malformed("Variable Byte Integer longer than 4 bytes")
}

// binary decodes Binary Data: a Two Byte Integer length followed by that many
// bytes (MQTT-5.0 §1.5.6). The returned slice aliases the packet buffer.
func (r *reader) binary() ([]byte, error) {
	n, err := r.uint16()
	if err != nil {
		return nil, err
	}
	if r.remaining() < int(n) {
		return nil, malformed("truncated Binary Data: want %d, have %d", n, r.remaining())
	}
	b := r.buf[r.pos : r.pos+int(n)]
	r.pos += int(n)
	return b, nil
}

// string decodes a UTF-8 Encoded String (MQTT-5.0 §1.5.4). The spec forbids
// U+0000 and the surrogate range D800-DFFF, and requires well-formed UTF-8
// [MQTT-1.5.4-1, MQTT-1.5.4-2].
func (r *reader) string() (string, error) {
	b, err := r.binary()
	if err != nil {
		return "", err
	}
	s := string(b)
	if err := validateUTF8(s); err != nil {
		return "", err
	}
	return s, nil
}

func (r *reader) stringPair() (string, string, error) {
	k, err := r.string()
	if err != nil {
		return "", "", err
	}
	v, err := r.string()
	if err != nil {
		return "", "", err
	}
	return k, v, nil
}

// validateUTF8 enforces the UTF-8 Encoded String rules of MQTT-5.0 §1.5.4.
func validateUTF8(s string) error {
	if !utf8.ValidString(s) {
		return malformed("string is not well-formed UTF-8 (MQTT-1.5.4-1)")
	}
	for _, r := range s {
		switch {
		case r == 0:
			return malformed("string contains U+0000 (MQTT-1.5.4-2)")
		case r >= 0xD800 && r <= 0xDFFF:
			return malformed("string contains a surrogate U+%04X (MQTT-1.5.4-1)", r)
		}
	}
	return nil
}

// writer accumulates an encoded packet body.
type writer struct {
	buf []byte
}

func (w *writer) byte(b byte) { w.buf = append(w.buf, b) }

func (w *writer) bytes(b []byte) { w.buf = append(w.buf, b...) }

func (w *writer) uint16(v uint16) { w.buf = append(w.buf, byte(v>>8), byte(v)) }

func (w *writer) uint32(v uint32) {
	w.buf = append(w.buf, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func (w *writer) varByteInt(v int) {
	w.buf = appendVarByteInt(w.buf, v)
}

func (w *writer) binary(b []byte) {
	w.uint16(uint16(len(b)))
	w.bytes(b)
}

func (w *writer) string(s string) {
	w.uint16(uint16(len(s)))
	w.buf = append(w.buf, s...)
}

func (w *writer) stringPair(k, v string) {
	w.string(k)
	w.string(v)
}

// appendVarByteInt encodes v using the algorithm in MQTT-5.0 §1.5.5. Values
// outside [0, MaxVarByteInt] cannot be represented; callers validate first.
func appendVarByteInt(dst []byte, v int) []byte {
	if v < 0 {
		v = 0
	}
	for {
		b := byte(v % 128)
		v /= 128
		if v > 0 {
			b |= 0x80
		}
		dst = append(dst, b)
		if v == 0 {
			return dst
		}
	}
}

// varByteIntLen reports how many bytes appendVarByteInt will emit for v.
func varByteIntLen(v int) int {
	switch {
	case v < 128:
		return 1
	case v < 16384:
		return 2
	case v < 2097152:
		return 3
	default:
		return 4
	}
}

// checkStringLen guards the 65,535-byte ceiling on UTF-8 Encoded Strings
// (MQTT-5.0 §1.5.4) before a length prefix is truncated into 16 bits.
func checkStringLen(field, s string) error {
	if len(s) > math.MaxUint16 {
		return fmt.Errorf("mqtt: %s is %d bytes, exceeds the 65535-byte limit on a UTF-8 Encoded String", field, len(s))
	}
	return nil
}

func checkBinaryLen(field string, b []byte) error {
	if len(b) > math.MaxUint16 {
		return fmt.Errorf("mqtt: %s is %d bytes, exceeds the 65535-byte limit on Binary Data", field, len(b))
	}
	return nil
}
