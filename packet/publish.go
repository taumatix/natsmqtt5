package packet

// Publish is the PUBLISH packet (MQTT-5.0 §3.3).
type Publish struct {
	// Dup marks a retransmission. It MUST be false for QoS 0 [MQTT-3.3.1-2],
	// and is set solely by whether this send is a retransmission — it is never
	// copied from the incoming packet [MQTT-3.3.1-3].
	Dup bool
	QoS QoS
	// Retain asks the server to store this message as the topic's retained
	// message (MQTT-5.0 §3.3.1.3).
	Retain bool
	// Topic may be empty only when Properties.TopicAlias is set
	// (MQTT-5.0 §3.3.2.1).
	Topic string
	// PacketID is present for QoS 1 and QoS 2 only (MQTT-5.0 §3.3.2.2).
	PacketID   uint16
	Properties *Properties
	Payload    []byte
}

func (*Publish) Type() Type { return PUBLISH }

func decodePublish(flags byte, r *reader) (*Publish, error) {
	p := &Publish{
		Dup:    flags&0x08 != 0,
		QoS:    QoS(flags >> 1 & 0x03),
		Retain: flags&0x01 != 0,
	}
	if !p.QoS.Valid() {
		return nil, malformed("PUBLISH has both QoS bits set [MQTT-3.3.1-4]")
	}
	if p.QoS == QoS0 && p.Dup {
		return nil, malformed("PUBLISH has DUP set at QoS 0 [MQTT-3.3.1-2]")
	}

	var err error
	if p.Topic, err = r.string(); err != nil {
		return nil, err
	}
	if p.QoS > QoS0 {
		if p.PacketID, err = r.uint16(); err != nil {
			return nil, err
		}
		if p.PacketID == 0 {
			return nil, protocolError("PUBLISH at %s has Packet Identifier 0", p.QoS)
		}
	}
	if p.Properties, err = readProperties(r, PUBLISH); err != nil {
		return nil, err
	}
	if p.Topic == "" && p.Properties.TopicAlias == nil {
		return nil, protocolError("PUBLISH has a zero-length Topic Name and no Topic Alias (MQTT-5.0 §3.3.2.1)")
	}
	p.Payload = append([]byte(nil), r.buf[r.pos:]...)
	r.pos = len(r.buf)
	return p, nil
}

func (p *Publish) encode() (byte, []byte, error) {
	if !p.QoS.Valid() {
		return 0, nil, malformed("PUBLISH QoS is %d [MQTT-3.3.1-4]", byte(p.QoS))
	}
	if err := checkStringLen("Topic Name", p.Topic); err != nil {
		return 0, nil, err
	}

	var flags byte
	if p.Dup && p.QoS != QoS0 {
		flags |= 0x08
	}
	flags |= byte(p.QoS) << 1
	if p.Retain {
		flags |= 0x01
	}

	var w writer
	w.string(p.Topic)
	if p.QoS > QoS0 {
		w.uint16(p.PacketID)
	}
	if err := p.Properties.encode(&w, PUBLISH); err != nil {
		return 0, nil, err
	}
	w.bytes(p.Payload)
	return flags, w.buf, nil
}
