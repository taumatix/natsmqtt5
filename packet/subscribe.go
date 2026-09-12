package packet

// RetainHandling controls whether retained messages are delivered when a
// subscription is established (MQTT-5.0 §3.8.3.1, bits 4-5).
type RetainHandling byte

const (
	// RetainSendAlways delivers matching retained messages on every SUBSCRIBE.
	RetainSendAlways RetainHandling = 0
	// RetainSendOnNew delivers them only if the subscription did not already
	// exist.
	RetainSendOnNew RetainHandling = 1
	// RetainSendNever never delivers retained messages at subscribe time.
	RetainSendNever RetainHandling = 2
)

// Subscription is one Topic Filter and its Subscription Options
// (MQTT-5.0 §3.8.3.1).
type Subscription struct {
	// Filter is the Topic Filter, which may use the + and # wildcards and may
	// be a $share/{name}/{filter} shared subscription (MQTT-5.0 §4.7, §4.8.2).
	Filter string
	// QoS is the maximum QoS the server may use when delivering to this
	// subscription.
	QoS QoS
	// NoLocal suppresses delivery back to the publishing connection. It is a
	// Protocol Error on a shared subscription [MQTT-3.8.3-4].
	NoLocal bool
	// RetainAsPublished forwards the RETAIN flag as published rather than
	// clearing it [MQTT-3.3.1-12, MQTT-3.3.1-13].
	RetainAsPublished bool
	RetainHandling    RetainHandling
}

// Subscribe is the SUBSCRIBE packet (MQTT-5.0 §3.8).
type Subscribe struct {
	PacketID      uint16
	Properties    *Properties
	Subscriptions []Subscription
}

func (*Subscribe) Type() Type { return SUBSCRIBE }

func decodeSubscribe(r *reader) (*Subscribe, error) {
	s := &Subscribe{}
	var err error
	if s.PacketID, err = r.uint16(); err != nil {
		return nil, err
	}
	if s.PacketID == 0 {
		return nil, protocolError("SUBSCRIBE has Packet Identifier 0")
	}
	if s.Properties, err = readProperties(r, SUBSCRIBE); err != nil {
		return nil, err
	}
	for !r.empty() {
		filter, err := r.string()
		if err != nil {
			return nil, err
		}
		opts, err := r.byte()
		if err != nil {
			return nil, err
		}
		if opts&0xC0 != 0 {
			return nil, malformed("Subscription Options reserved bits are set [MQTT-3.8.3-5]")
		}
		sub := Subscription{
			Filter:            filter,
			QoS:               QoS(opts & 0x03),
			NoLocal:           opts&0x04 != 0,
			RetainAsPublished: opts&0x08 != 0,
			RetainHandling:    RetainHandling(opts >> 4 & 0x03),
		}
		if !sub.QoS.Valid() {
			return nil, protocolError("Subscription Options Maximum QoS is 3 (MQTT-5.0 §3.8.3.1)")
		}
		if sub.RetainHandling > RetainSendNever {
			return nil, protocolError("Retain Handling is 3 (MQTT-5.0 §3.8.3.1)")
		}
		s.Subscriptions = append(s.Subscriptions, sub)
	}
	// "The Payload MUST contain at least one Topic Filter and Subscription
	// Options pair" [MQTT-3.8.3-2].
	if len(s.Subscriptions) == 0 {
		return nil, protocolError("SUBSCRIBE has no Topic Filters [MQTT-3.8.3-2]")
	}
	return s, nil
}

func (s *Subscribe) encode() (byte, []byte, error) {
	if len(s.Subscriptions) == 0 {
		return 0, nil, protocolError("SUBSCRIBE has no Topic Filters [MQTT-3.8.3-2]")
	}
	var w writer
	w.uint16(s.PacketID)
	if err := s.Properties.encode(&w, SUBSCRIBE); err != nil {
		return 0, nil, err
	}
	for _, sub := range s.Subscriptions {
		if err := checkStringLen("Topic Filter", sub.Filter); err != nil {
			return 0, nil, err
		}
		if !sub.QoS.Valid() {
			return 0, nil, protocolError("subscription to %q has QoS %d", sub.Filter, byte(sub.QoS))
		}
		if sub.RetainHandling > RetainSendNever {
			return 0, nil, protocolError("subscription to %q has Retain Handling %d", sub.Filter, byte(sub.RetainHandling))
		}
		w.string(sub.Filter)
		opts := byte(sub.QoS) | byte(sub.RetainHandling)<<4
		if sub.NoLocal {
			opts |= 0x04
		}
		if sub.RetainAsPublished {
			opts |= 0x08
		}
		w.byte(opts)
	}
	return reservedFlags[SUBSCRIBE], w.buf, nil
}

// Unsubscribe is the UNSUBSCRIBE packet (MQTT-5.0 §3.10).
type Unsubscribe struct {
	PacketID   uint16
	Properties *Properties
	Filters    []string
}

func (*Unsubscribe) Type() Type { return UNSUBSCRIBE }

func decodeUnsubscribe(r *reader) (*Unsubscribe, error) {
	u := &Unsubscribe{}
	var err error
	if u.PacketID, err = r.uint16(); err != nil {
		return nil, err
	}
	if u.PacketID == 0 {
		return nil, protocolError("UNSUBSCRIBE has Packet Identifier 0")
	}
	if u.Properties, err = readProperties(r, UNSUBSCRIBE); err != nil {
		return nil, err
	}
	for !r.empty() {
		filter, err := r.string()
		if err != nil {
			return nil, err
		}
		u.Filters = append(u.Filters, filter)
	}
	// "The Payload of an UNSUBSCRIBE packet MUST contain at least one Topic
	// Filter" [MQTT-3.10.3-2].
	if len(u.Filters) == 0 {
		return nil, protocolError("UNSUBSCRIBE has no Topic Filters [MQTT-3.10.3-2]")
	}
	return u, nil
}

func (u *Unsubscribe) encode() (byte, []byte, error) {
	if len(u.Filters) == 0 {
		return 0, nil, protocolError("UNSUBSCRIBE has no Topic Filters [MQTT-3.10.3-2]")
	}
	var w writer
	w.uint16(u.PacketID)
	if err := u.Properties.encode(&w, UNSUBSCRIBE); err != nil {
		return 0, nil, err
	}
	for _, f := range u.Filters {
		if err := checkStringLen("Topic Filter", f); err != nil {
			return 0, nil, err
		}
		w.string(f)
	}
	return reservedFlags[UNSUBSCRIBE], w.buf, nil
}
