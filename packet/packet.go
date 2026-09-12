// Package packet implements the MQTT version 5.0 wire format as specified by
// the OASIS Standard of 07 March 2019.
//
//	https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html
//
// Section numbers in the comments and tests refer to that document. Normative
// statement labels such as [MQTT-3.3.1-4] are quoted verbatim from it, so a
// reader can check any rule this package claims to enforce.
//
// The package is transport-agnostic: Read decodes one packet from a reader and
// Write encodes one to a writer. It has no dependencies outside the standard
// library, and holds no connection or session state.
package packet

import (
	"bufio"
	"io"
)

// Packet is a decoded MQTT Control Packet. The interface is closed — every
// implementation lives in this package — so that adding a packet type or a
// field cannot break code that switches over it.
type Packet interface {
	// Type reports the control packet type.
	Type() Type
	// encode returns the low nibble of the fixed header and the packet body
	// (variable header plus payload).
	encode() (flags byte, body []byte, err error)
}

// Read decodes one MQTT Control Packet.
//
// maxPacketSize bounds the total packet size in bytes, as advertised by the
// Maximum Packet Size property (MQTT-5.0 §3.1.2.11.4); pass 0 for no limit.
// A packet that would exceed it returns ErrPacketTooLarge without the body
// being buffered, so a hostile length prefix cannot exhaust memory.
//
// The returned error wraps ErrMalformed, ErrProtocol or ErrPacketTooLarge for
// protocol faults; io.EOF and other I/O errors pass through unwrapped.
func Read(r *bufio.Reader, maxPacketSize uint32) (Packet, error) {
	first, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	t := Type(first >> 4)
	flags := first & 0x0F

	if t == 0 {
		return nil, malformed("control packet type 0 is reserved")
	}
	if t != PUBLISH && flags != reservedFlags[t] {
		return nil, malformed("%s has fixed header flags 0x%X, must be 0x%X [MQTT-2.1.3-1]",
			t, flags, reservedFlags[t])
	}

	remaining, headerLen, err := readRemainingLength(r)
	if err != nil {
		return nil, err
	}
	if maxPacketSize > 0 && uint64(1+headerLen+remaining) > uint64(maxPacketSize) {
		return nil, ErrPacketTooLarge
	}

	body := make([]byte, remaining)
	if _, err := io.ReadFull(r, body); err != nil {
		if err == io.EOF {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	return decode(t, flags, body)
}

// readRemainingLength decodes the fixed header's Remaining Length field,
// returning the value and the number of bytes it occupied (MQTT-5.0 §2.1.4).
func readRemainingLength(r *bufio.Reader) (value, width int, err error) {
	multiplier := 0
	for i := 0; i < 4; i++ {
		b, err := r.ReadByte()
		if err != nil {
			if err == io.EOF {
				return 0, 0, io.ErrUnexpectedEOF
			}
			return 0, 0, err
		}
		value += int(b&0x7F) << multiplier
		if b&0x80 == 0 {
			if i > 0 && b == 0 {
				return 0, 0, malformed("non-minimal Remaining Length encoding")
			}
			return value, i + 1, nil
		}
		multiplier += 7
	}
	return 0, 0, malformed("Remaining Length longer than 4 bytes")
}

func decode(t Type, flags byte, body []byte) (Packet, error) {
	r := &reader{buf: body}
	switch t {
	case CONNECT:
		return decodeConnect(r)
	case CONNACK:
		return decodeConnack(r)
	case PUBLISH:
		return decodePublish(flags, r)
	case PUBACK, PUBREC, PUBREL, PUBCOMP:
		return decodeAck(t, r)
	case SUBSCRIBE:
		return decodeSubscribe(r)
	case SUBACK:
		return decodeSuback(r)
	case UNSUBSCRIBE:
		return decodeUnsubscribe(r)
	case UNSUBACK:
		return decodeUnsuback(r)
	case PINGREQ:
		if r.remaining() != 0 {
			return nil, malformed("PINGREQ has a %d-byte body, must be empty", r.remaining())
		}
		return &Pingreq{}, nil
	case PINGRESP:
		if r.remaining() != 0 {
			return nil, malformed("PINGRESP has a %d-byte body, must be empty", r.remaining())
		}
		return &Pingresp{}, nil
	case DISCONNECT:
		return decodeDisconnect(r)
	case AUTH:
		return decodeAuth(r)
	default:
		return nil, malformed("unknown control packet type %d", byte(t))
	}
}

// Encode serialises a packet, fixed header included.
func Encode(p Packet) ([]byte, error) {
	flags, body, err := p.encode()
	if err != nil {
		return nil, err
	}
	if len(body) > MaxVarByteInt {
		return nil, malformed("%s body is %d bytes, exceeds the %d-byte Remaining Length limit",
			p.Type(), len(body), MaxVarByteInt)
	}
	out := make([]byte, 0, 5+len(body))
	out = append(out, byte(p.Type())<<4|flags)
	out = appendVarByteInt(out, len(body))
	return append(out, body...), nil
}

// Write encodes a packet and writes it to w in a single call, so that a packet
// is never interleaved with another writer's bytes at the transport layer.
func Write(w io.Writer, p Packet) error {
	b, err := Encode(p)
	if err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}

// EncodedLen reports the total wire size of p in bytes, without allocating a
// second copy of the body. It is what Maximum Packet Size is measured against
// (MQTT-5.0 §2.1.4).
func EncodedLen(p Packet) (int, error) {
	_, body, err := p.encode()
	if err != nil {
		return 0, err
	}
	return 1 + varByteIntLen(len(body)) + len(body), nil
}
