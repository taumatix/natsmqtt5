package packet

// Suback is the SUBACK packet (MQTT-5.0 §3.9). It carries one Reason Code per
// Topic Filter in the SUBSCRIBE it answers [MQTT-3.8.4-6].
type Suback struct {
	PacketID    uint16
	Properties  *Properties
	ReasonCodes []ReasonCode
}

func (*Suback) Type() Type { return SUBACK }

// Unsuback is the UNSUBACK packet (MQTT-5.0 §3.11). It carries one Reason Code
// per Topic Filter in the UNSUBSCRIBE it answers [MQTT-3.11.3-1].
type Unsuback struct {
	PacketID    uint16
	Properties  *Properties
	ReasonCodes []ReasonCode
}

func (*Unsuback) Type() Type { return UNSUBACK }

func decodeSuback(r *reader) (*Suback, error) {
	id, props, codes, err := decodeReasonCodeList(r, SUBACK)
	if err != nil {
		return nil, err
	}
	return &Suback{PacketID: id, Properties: props, ReasonCodes: codes}, nil
}

func decodeUnsuback(r *reader) (*Unsuback, error) {
	id, props, codes, err := decodeReasonCodeList(r, UNSUBACK)
	if err != nil {
		return nil, err
	}
	return &Unsuback{PacketID: id, Properties: props, ReasonCodes: codes}, nil
}

func decodeReasonCodeList(r *reader, t Type) (uint16, *Properties, []ReasonCode, error) {
	id, err := r.uint16()
	if err != nil {
		return 0, nil, nil, err
	}
	props, err := readProperties(r, t)
	if err != nil {
		return 0, nil, nil, err
	}
	var codes []ReasonCode
	for !r.empty() {
		b, err := r.byte()
		if err != nil {
			return 0, nil, nil, err
		}
		codes = append(codes, ReasonCode(b))
	}
	if len(codes) == 0 {
		return 0, nil, nil, protocolError("%s carries no Reason Codes", t)
	}
	return id, props, codes, nil
}

func (s *Suback) encode() (byte, []byte, error) {
	return encodeReasonCodeList(SUBACK, s.PacketID, s.Properties, s.ReasonCodes)
}

func (u *Unsuback) encode() (byte, []byte, error) {
	return encodeReasonCodeList(UNSUBACK, u.PacketID, u.Properties, u.ReasonCodes)
}

func encodeReasonCodeList(t Type, id uint16, props *Properties, codes []ReasonCode) (byte, []byte, error) {
	if len(codes) == 0 {
		return 0, nil, protocolError("%s carries no Reason Codes", t)
	}
	var w writer
	w.uint16(id)
	if err := props.encode(&w, t); err != nil {
		return 0, nil, err
	}
	for _, c := range codes {
		w.byte(byte(c))
	}
	return 0, w.buf, nil
}
