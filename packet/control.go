package packet

// Pingreq is the PINGREQ packet (MQTT-5.0 §3.12). It has no variable header
// and no payload.
type Pingreq struct{}

func (*Pingreq) Type() Type                    { return PINGREQ }
func (*Pingreq) encode() (byte, []byte, error) { return 0, nil, nil }

// Pingresp is the PINGRESP packet (MQTT-5.0 §3.13). It has no variable header
// and no payload.
type Pingresp struct{}

func (*Pingresp) Type() Type                    { return PINGRESP }
func (*Pingresp) encode() (byte, []byte, error) { return 0, nil, nil }

// Disconnect is the DISCONNECT packet (MQTT-5.0 §3.14).
type Disconnect struct {
	ReasonCode ReasonCode
	Properties *Properties
}

func (*Disconnect) Type() Type { return DISCONNECT }

func decodeDisconnect(r *reader) (*Disconnect, error) {
	d := &Disconnect{Properties: &Properties{}}
	// "If the Remaining Length is less than 1 the value of 0x00 (Normal
	// disconnection) is used" (MQTT-5.0 §3.14.2.1).
	if r.empty() {
		return d, nil
	}
	code, err := r.byte()
	if err != nil {
		return nil, err
	}
	d.ReasonCode = ReasonCode(code)
	if r.empty() {
		return d, nil
	}
	if d.Properties, err = readProperties(r, DISCONNECT); err != nil {
		return nil, err
	}
	if !r.empty() {
		return nil, malformed("DISCONNECT has %d trailing bytes", r.remaining())
	}
	return d, nil
}

func (d *Disconnect) encode() (byte, []byte, error) {
	var pw writer
	if err := d.Properties.encode(&pw, DISCONNECT); err != nil {
		return 0, nil, err
	}
	noProps := len(pw.buf) == 1 && pw.buf[0] == 0
	if d.ReasonCode == NormalDisconnection && noProps {
		return 0, nil, nil
	}
	var w writer
	w.byte(byte(d.ReasonCode))
	if noProps {
		return 0, w.buf, nil
	}
	w.bytes(pw.buf)
	return 0, w.buf, nil
}

// Auth is the AUTH packet used for extended authentication
// (MQTT-5.0 §3.15, §4.12).
type Auth struct {
	ReasonCode ReasonCode
	Properties *Properties
}

func (*Auth) Type() Type { return AUTH }

func decodeAuth(r *reader) (*Auth, error) {
	a := &Auth{Properties: &Properties{}}
	// "If the Remaining Length is 0, the value of 0x00 (Success) is used"
	// (MQTT-5.0 §3.15.2.1).
	if r.empty() {
		return a, nil
	}
	code, err := r.byte()
	if err != nil {
		return nil, err
	}
	a.ReasonCode = ReasonCode(code)
	if r.empty() {
		return a, nil
	}
	if a.Properties, err = readProperties(r, AUTH); err != nil {
		return nil, err
	}
	if !r.empty() {
		return nil, malformed("AUTH has %d trailing bytes", r.remaining())
	}
	return a, nil
}

func (a *Auth) encode() (byte, []byte, error) {
	var pw writer
	if err := a.Properties.encode(&pw, AUTH); err != nil {
		return 0, nil, err
	}
	noProps := len(pw.buf) == 1 && pw.buf[0] == 0
	if a.ReasonCode == Success && noProps {
		return 0, nil, nil
	}
	var w writer
	w.byte(byte(a.ReasonCode))
	if noProps {
		return 0, w.buf, nil
	}
	w.bytes(pw.buf)
	return 0, w.buf, nil
}
