package packet

// Connack is the CONNACK packet (MQTT-5.0 §3.2).
type Connack struct {
	// SessionPresent tells the client whether the server resumed stored
	// session state. It MUST be false whenever ReasonCode is an error
	// [MQTT-3.2.2-6].
	SessionPresent bool
	ReasonCode     ReasonCode
	Properties     *Properties
}

func (*Connack) Type() Type { return CONNACK }

func decodeConnack(r *reader) (*Connack, error) {
	ackFlags, err := r.byte()
	if err != nil {
		return nil, err
	}
	if ackFlags&0xFE != 0 {
		return nil, malformed("CONNACK reserved acknowledge flags are set [MQTT-3.2.2-1]")
	}
	code, err := r.byte()
	if err != nil {
		return nil, err
	}
	props, err := readProperties(r, CONNACK)
	if err != nil {
		return nil, err
	}
	if !r.empty() {
		return nil, malformed("CONNACK has %d trailing bytes", r.remaining())
	}
	return &Connack{
		SessionPresent: ackFlags&0x01 != 0,
		ReasonCode:     ReasonCode(code),
		Properties:     props,
	}, nil
}

func (c *Connack) encode() (byte, []byte, error) {
	var w writer
	var ackFlags byte
	// A non-zero Reason Code forces Session Present to 0 [MQTT-3.2.2-6].
	if c.SessionPresent && !c.ReasonCode.IsError() {
		ackFlags = 0x01
	}
	w.byte(ackFlags)
	w.byte(byte(c.ReasonCode))
	if err := c.Properties.encode(&w, CONNACK); err != nil {
		return 0, nil, err
	}
	return 0, w.buf, nil
}
