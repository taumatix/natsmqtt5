package packet

// ProtocolVersion5 is the value of the CONNECT Protocol Version field for
// MQTT v5.0 (MQTT-5.0 §3.1.2.2).
const ProtocolVersion5 = 5

// protocolName is the Protocol Name every MQTT CONNECT carries
// [MQTT-3.1.2-1].
const protocolName = "MQTT"

// Will is the message the server publishes on behalf of a client whose
// connection ends without a clean DISCONNECT (MQTT-5.0 §3.1.2.5).
type Will struct {
	Topic      string
	Payload    []byte
	QoS        QoS
	Retain     bool
	Properties *Properties
}

// Connect is the CONNECT packet (MQTT-5.0 §3.1).
type Connect struct {
	ClientID string
	// CleanStart discards any existing session and starts a new one
	// (MQTT-5.0 §3.1.2.4).
	CleanStart bool
	// KeepAlive is the maximum seconds the client may stay silent. The server
	// disconnects after 1.5x this interval [MQTT-3.1.2-22]. Zero disables it.
	KeepAlive uint16
	// Will, if non-nil, is stored with the session and published when the
	// connection ends abnormally.
	Will *Will
	// Username is sent only when non-empty.
	Username string
	// Password is sent only when non-nil. v5 permits a password with no
	// username (MQTT-5.0 §3.1.2.9).
	Password   []byte
	Properties *Properties
	// ProtocolVersion is the byte the client sent. It is always
	// ProtocolVersion5 for a packet this package decoded, since a CONNECT
	// declaring another version is rejected before the rest is parsed.
	ProtocolVersion byte
}

func (*Connect) Type() Type { return CONNECT }

// ErrUnsupportedProtocolVersion reports a CONNECT whose Protocol Name is
// "MQTT" but whose Protocol Version is not 5. The server answers it with a
// CONNACK carrying UnsupportedProtocolVersion (0x84) rather than closing
// silently, so a v3.1.1 client learns to fall back [MQTT-3.1.2-2].
type ErrUnsupportedProtocolVersion struct {
	Version byte
}

func (e *ErrUnsupportedProtocolVersion) Error() string {
	return "mqtt: protocol version " + string('0'+rune(e.Version%10)) + " is not MQTT v5.0"
}

func decodeConnect(r *reader) (*Connect, error) {
	name, err := r.string()
	if err != nil {
		return nil, err
	}
	if name != protocolName {
		return nil, malformed("Protocol Name is %q, must be %q [MQTT-3.1.2-1]", name, protocolName)
	}
	version, err := r.byte()
	if err != nil {
		return nil, err
	}
	if version != ProtocolVersion5 {
		return nil, &ErrUnsupportedProtocolVersion{Version: version}
	}

	flags, err := r.byte()
	if err != nil {
		return nil, err
	}
	if flags&0x01 != 0 {
		return nil, malformed("CONNECT reserved flag is set [MQTT-3.1.2-3]")
	}
	var (
		cleanStart  = flags&0x02 != 0
		willFlag    = flags&0x04 != 0
		willQoS     = QoS(flags >> 3 & 0x03)
		willRetain  = flags&0x20 != 0
		hasPassword = flags&0x40 != 0
		hasUsername = flags&0x80 != 0
	)
	if !willFlag {
		if willQoS != 0 {
			return nil, malformed("Will QoS is %d with Will Flag 0 [MQTT-3.1.2-11]", willQoS)
		}
		if willRetain {
			return nil, malformed("Will Retain is set with Will Flag 0 [MQTT-3.1.2-13]")
		}
	} else if !willQoS.Valid() {
		return nil, malformed("Will QoS is 3 [MQTT-3.1.2-12]")
	}

	keepAlive, err := r.uint16()
	if err != nil {
		return nil, err
	}
	props, err := readProperties(r, CONNECT)
	if err != nil {
		return nil, err
	}
	if len(props.AuthenticationData) > 0 && props.AuthenticationMethod == "" {
		return nil, protocolError("Authentication Data without an Authentication Method (MQTT-5.0 §3.1.2.11.10)")
	}

	c := &Connect{
		CleanStart:      cleanStart,
		KeepAlive:       keepAlive,
		Properties:      props,
		ProtocolVersion: version,
	}
	// The payload fields appear in a fixed order [MQTT-3.1.3-1].
	if c.ClientID, err = r.string(); err != nil {
		return nil, err
	}
	if willFlag {
		will := &Will{QoS: willQoS, Retain: willRetain}
		if will.Properties, err = readProperties(r, willContext); err != nil {
			return nil, err
		}
		if will.Topic, err = r.string(); err != nil {
			return nil, err
		}
		payload, err := r.binary()
		if err != nil {
			return nil, err
		}
		will.Payload = append([]byte(nil), payload...)
		c.Will = will
	}
	if hasUsername {
		if c.Username, err = r.string(); err != nil {
			return nil, err
		}
	}
	if hasPassword {
		password, err := r.binary()
		if err != nil {
			return nil, err
		}
		c.Password = append([]byte(nil), password...)
	}
	if !r.empty() {
		return nil, malformed("CONNECT has %d trailing bytes", r.remaining())
	}
	return c, nil
}

func (c *Connect) encode() (byte, []byte, error) {
	if err := checkStringLen("Client Identifier", c.ClientID); err != nil {
		return 0, nil, err
	}

	var flags byte
	if c.CleanStart {
		flags |= 0x02
	}
	if c.Will != nil {
		if !c.Will.QoS.Valid() {
			return 0, nil, malformed("Will QoS is %d [MQTT-3.1.2-12]", c.Will.QoS)
		}
		flags |= 0x04 | byte(c.Will.QoS)<<3
		if c.Will.Retain {
			flags |= 0x20
		}
	}
	if c.Username != "" {
		flags |= 0x80
	}
	if c.Password != nil {
		flags |= 0x40
	}

	var w writer
	w.string(protocolName)
	w.byte(ProtocolVersion5)
	w.byte(flags)
	w.uint16(c.KeepAlive)
	if err := c.Properties.encode(&w, CONNECT); err != nil {
		return 0, nil, err
	}
	w.string(c.ClientID)
	if c.Will != nil {
		if err := checkStringLen("Will Topic", c.Will.Topic); err != nil {
			return 0, nil, err
		}
		if err := checkBinaryLen("Will Payload", c.Will.Payload); err != nil {
			return 0, nil, err
		}
		if err := c.Will.Properties.encode(&w, willContext); err != nil {
			return 0, nil, err
		}
		w.string(c.Will.Topic)
		w.binary(c.Will.Payload)
	}
	if c.Username != "" {
		if err := checkStringLen("User Name", c.Username); err != nil {
			return 0, nil, err
		}
		w.string(c.Username)
	}
	if c.Password != nil {
		if err := checkBinaryLen("Password", c.Password); err != nil {
			return 0, nil, err
		}
		w.binary(c.Password)
	}
	return 0, w.buf, nil
}
