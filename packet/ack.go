package packet

// Puback, Pubrec, Pubrel and Pubcomp share one wire shape: a Packet
// Identifier, an optional Reason Code and optional Properties
// (MQTT-5.0 §3.4 - §3.7). They are distinct Go types so that a type switch
// over a Packet distinguishes the four stages of the QoS 2 handshake.
type (
	// Puback acknowledges a QoS 1 PUBLISH (MQTT-5.0 §3.4).
	Puback struct{ Ack }
	// Pubrec is stage 1 of the QoS 2 handshake (MQTT-5.0 §3.5).
	Pubrec struct{ Ack }
	// Pubrel is stage 2 of the QoS 2 handshake (MQTT-5.0 §3.6).
	Pubrel struct{ Ack }
	// Pubcomp is stage 3 of the QoS 2 handshake (MQTT-5.0 §3.7).
	Pubcomp struct{ Ack }
)

// Ack holds the fields common to PUBACK, PUBREC, PUBREL and PUBCOMP.
type Ack struct {
	PacketID   uint16
	ReasonCode ReasonCode
	Properties *Properties
}

func (*Puback) Type() Type  { return PUBACK }
func (*Pubrec) Type() Type  { return PUBREC }
func (*Pubrel) Type() Type  { return PUBREL }
func (*Pubcomp) Type() Type { return PUBCOMP }

func (p *Puback) encode() (byte, []byte, error)  { return p.Ack.encode(PUBACK) }
func (p *Pubrec) encode() (byte, []byte, error)  { return p.Ack.encode(PUBREC) }
func (p *Pubrel) encode() (byte, []byte, error)  { return p.Ack.encode(PUBREL) }
func (p *Pubcomp) encode() (byte, []byte, error) { return p.Ack.encode(PUBCOMP) }

func decodeAck(t Type, r *reader) (Packet, error) {
	var a Ack
	id, err := r.uint16()
	if err != nil {
		return nil, err
	}
	a.PacketID = id

	// "If the Remaining Length is 2, then there is no Reason Code and the
	// value of 0x00 (Success) is used" (MQTT-5.0 §3.4.2.1). "If the Remaining
	// Length is less than 4 there is no Property Length and the value of 0 is
	// used" (MQTT-5.0 §3.4.2.2.1).
	if r.empty() {
		a.ReasonCode = Success
		a.Properties = &Properties{}
	} else {
		code, err := r.byte()
		if err != nil {
			return nil, err
		}
		a.ReasonCode = ReasonCode(code)
		if r.empty() {
			a.Properties = &Properties{}
		} else if a.Properties, err = readProperties(r, t); err != nil {
			return nil, err
		}
	}
	if !r.empty() {
		return nil, malformed("%s has %d trailing bytes", t, r.remaining())
	}

	switch t {
	case PUBACK:
		return &Puback{a}, nil
	case PUBREC:
		return &Pubrec{a}, nil
	case PUBREL:
		return &Pubrel{a}, nil
	default:
		return &Pubcomp{a}, nil
	}
}

// encode emits the short forms the spec permits: a bare Packet Identifier when
// the reason is Success with no properties, and Packet Identifier plus Reason
// Code when there are no properties. Some v3-era clients only accept the
// two-byte form, and it is what every v5 client expects for a plain success.
func (a *Ack) encode(t Type) (byte, []byte, error) {
	var w writer
	w.uint16(a.PacketID)

	var pw writer
	if err := a.Properties.encode(&pw, t); err != nil {
		return 0, nil, err
	}
	noProps := len(pw.buf) == 1 && pw.buf[0] == 0

	if a.ReasonCode == Success && noProps {
		return reservedFlags[t], w.buf, nil
	}
	w.byte(byte(a.ReasonCode))
	if noProps {
		return reservedFlags[t], w.buf, nil
	}
	w.bytes(pw.buf)
	return reservedFlags[t], w.buf, nil
}
