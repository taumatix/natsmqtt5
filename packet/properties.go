package packet

// propID is an MQTT property identifier (MQTT-5.0 §2.2.2.2, Table 2-4).
// Every identifier defined by v5.0 is one byte long, but the wire format is a
// Variable Byte Integer, so the decoder reads it as one.
type propID int

const (
	propPayloadFormat      propID = 0x01
	propMessageExpiry      propID = 0x02
	propContentType        propID = 0x03
	propResponseTopic      propID = 0x08
	propCorrelationData    propID = 0x09
	propSubscriptionID     propID = 0x0B
	propSessionExpiry      propID = 0x11
	propAssignedClientID   propID = 0x12
	propServerKeepAlive    propID = 0x13
	propAuthMethod         propID = 0x15
	propAuthData           propID = 0x16
	propRequestProblemInfo propID = 0x17
	propWillDelayInterval  propID = 0x18
	propRequestResponse    propID = 0x19
	propResponseInfo       propID = 0x1A
	propServerReference    propID = 0x1C
	propReasonString       propID = 0x1F
	propReceiveMaximum     propID = 0x21
	propTopicAliasMaximum  propID = 0x22
	propTopicAlias         propID = 0x23
	propMaximumQoS         propID = 0x24
	propRetainAvailable    propID = 0x25
	propUser               propID = 0x26
	propMaximumPacketSize  propID = 0x27
	propWildcardSubAvail   propID = 0x28
	propSubIDAvail         propID = 0x29
	propSharedSubAvail     propID = 0x2A
)

// willContext is the pseudo packet type used to validate the Will Properties
// field, which lives in the CONNECT payload rather than a variable header
// (MQTT-5.0 §3.1.3.2). Packet types occupy 1..15, so 0 is free.
const willContext Type = 0

// propContexts records, for each property, the packet types (and the Will
// Properties field) it may appear in. Transcribed from MQTT-5.0 Table 2-4;
// a property in any other context is a Malformed Packet (MQTT-5.0 §2.2.2.2).
var propContexts = map[propID][]Type{
	propPayloadFormat:      {PUBLISH, willContext},
	propMessageExpiry:      {PUBLISH, willContext},
	propContentType:        {PUBLISH, willContext},
	propResponseTopic:      {PUBLISH, willContext},
	propCorrelationData:    {PUBLISH, willContext},
	propSubscriptionID:     {PUBLISH, SUBSCRIBE},
	propSessionExpiry:      {CONNECT, CONNACK, DISCONNECT},
	propAssignedClientID:   {CONNACK},
	propServerKeepAlive:    {CONNACK},
	propAuthMethod:         {CONNECT, CONNACK, AUTH},
	propAuthData:           {CONNECT, CONNACK, AUTH},
	propRequestProblemInfo: {CONNECT},
	propWillDelayInterval:  {willContext},
	propRequestResponse:    {CONNECT},
	propResponseInfo:       {CONNACK},
	propServerReference:    {CONNACK, DISCONNECT},
	propReasonString:       {CONNACK, PUBACK, PUBREC, PUBREL, PUBCOMP, SUBACK, UNSUBACK, DISCONNECT, AUTH},
	propReceiveMaximum:     {CONNECT, CONNACK},
	propTopicAliasMaximum:  {CONNECT, CONNACK},
	propTopicAlias:         {PUBLISH},
	propMaximumQoS:         {CONNACK},
	propRetainAvailable:    {CONNACK},
	propUser: {CONNECT, CONNACK, PUBLISH, willContext, PUBACK, PUBREC, PUBREL,
		PUBCOMP, SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK, DISCONNECT, AUTH},
	propMaximumPacketSize: {CONNECT, CONNACK},
	propWildcardSubAvail:  {CONNACK},
	propSubIDAvail:        {CONNACK},
	propSharedSubAvail:    {CONNACK},
}

func (p propID) allowedIn(t Type) bool {
	for _, c := range propContexts[p] {
		if c == t {
			return true
		}
	}
	return false
}

// UserProperty is a name-value pair carried in a packet's properties
// (MQTT-5.0 §2.2.2.2, identifier 0x26). Order is significant and is preserved
// end to end: the server MUST forward them unaltered and in order
// [MQTT-3.3.2-17, MQTT-3.3.2-18].
type UserProperty struct {
	Key   string
	Value string
}

// Properties carries every MQTT v5 property. One type covers all packets;
// which fields are legal depends on the carrying packet, and both the decoder
// and the encoder enforce that against MQTT-5.0 Table 2-4.
//
// Optional scalars are pointers so that "absent" is distinguishable from
// "present and zero" — the distinction is load-bearing for Session Expiry
// Interval, Maximum QoS and Retain Available. Optional strings and byte slices
// use their zero value for absent, since MQTT assigns no meaning to an empty
// Content Type or Reason String.
type Properties struct {
	PayloadFormat           *byte
	MessageExpiryInterval   *uint32
	ContentType             string
	ResponseTopic           string
	CorrelationData         []byte
	SubscriptionIdentifiers []int
	SessionExpiryInterval   *uint32
	AssignedClientID        string
	ServerKeepAlive         *uint16
	AuthenticationMethod    string
	AuthenticationData      []byte
	RequestProblemInfo      *byte
	WillDelayInterval       *uint32
	RequestResponseInfo     *byte
	ResponseInformation     string
	ServerReference         string
	ReasonString            string
	ReceiveMaximum          *uint16
	TopicAliasMaximum       *uint16
	TopicAlias              *uint16
	MaximumQoS              *byte
	RetainAvailable         *byte
	User                    []UserProperty
	MaximumPacketSize       *uint32
	WildcardSubAvailable    *byte
	SubIDAvailable          *byte
	SharedSubAvailable      *byte
}

// Byte, Uint16 and Uint32 build the pointers Properties uses for optional
// scalars, so callers can write packet.Byte(1) instead of taking the address
// of a temporary.
func Byte(v byte) *byte       { return &v }
func Uint16(v uint16) *uint16 { return &v }
func Uint32(v uint32) *uint32 { return &v }

// readProperties decodes a Property Length followed by that many bytes of
// properties (MQTT-5.0 §2.2.2). ctx is the packet type the properties belong
// to, or willContext for the CONNECT Will Properties field.
func readProperties(r *reader, ctx Type) (*Properties, error) {
	n, err := r.varByteInt()
	if err != nil {
		return nil, err
	}
	if n > r.remaining() {
		return nil, malformed("Property Length %d exceeds the %d bytes remaining", n, r.remaining())
	}
	if n == 0 {
		return &Properties{}, nil
	}

	pr := &reader{buf: r.buf[r.pos : r.pos+n]}
	r.pos += n

	p := &Properties{}
	seen := make(map[propID]bool, 8)
	for !pr.empty() {
		raw, err := pr.varByteInt()
		if err != nil {
			return nil, err
		}
		id := propID(raw)
		if !id.allowedIn(ctx) {
			return nil, malformed("property 0x%02X is not valid in %s (MQTT-5.0 Table 2-4)", raw, propContextName(ctx))
		}
		// Every property except User Property and, in a PUBLISH, Subscription
		// Identifier, is a Protocol Error to repeat.
		repeatable := id == propUser || (id == propSubscriptionID && ctx == PUBLISH)
		if seen[id] && !repeatable {
			return nil, protocolError("property 0x%02X appears more than once", raw)
		}
		seen[id] = true

		if err := readProperty(pr, p, id); err != nil {
			return nil, err
		}
	}
	return p, nil
}

func propContextName(ctx Type) string {
	if ctx == willContext {
		return "Will Properties"
	}
	return ctx.String()
}

func readProperty(pr *reader, p *Properties, id propID) error {
	switch id {
	case propPayloadFormat:
		v, err := pr.byte()
		if err != nil {
			return err
		}
		if v > 1 {
			return protocolError("Payload Format Indicator is %d, must be 0 or 1", v)
		}
		p.PayloadFormat = &v
	case propMessageExpiry:
		v, err := pr.uint32()
		if err != nil {
			return err
		}
		p.MessageExpiryInterval = &v
	case propContentType:
		v, err := pr.string()
		if err != nil {
			return err
		}
		p.ContentType = v
	case propResponseTopic:
		v, err := pr.string()
		if err != nil {
			return err
		}
		p.ResponseTopic = v
	case propCorrelationData:
		v, err := pr.binary()
		if err != nil {
			return err
		}
		p.CorrelationData = append([]byte(nil), v...)
	case propSubscriptionID:
		v, err := pr.varByteInt()
		if err != nil {
			return err
		}
		if v == 0 {
			return protocolError("Subscription Identifier is 0 (MQTT-5.0 §3.3.2.3.8)")
		}
		p.SubscriptionIdentifiers = append(p.SubscriptionIdentifiers, v)
	case propSessionExpiry:
		v, err := pr.uint32()
		if err != nil {
			return err
		}
		p.SessionExpiryInterval = &v
	case propAssignedClientID:
		v, err := pr.string()
		if err != nil {
			return err
		}
		p.AssignedClientID = v
	case propServerKeepAlive:
		v, err := pr.uint16()
		if err != nil {
			return err
		}
		p.ServerKeepAlive = &v
	case propAuthMethod:
		v, err := pr.string()
		if err != nil {
			return err
		}
		p.AuthenticationMethod = v
	case propAuthData:
		v, err := pr.binary()
		if err != nil {
			return err
		}
		p.AuthenticationData = append([]byte(nil), v...)
	case propRequestProblemInfo:
		v, err := pr.byte()
		if err != nil {
			return err
		}
		if v > 1 {
			return protocolError("Request Problem Information is %d, must be 0 or 1", v)
		}
		p.RequestProblemInfo = &v
	case propWillDelayInterval:
		v, err := pr.uint32()
		if err != nil {
			return err
		}
		p.WillDelayInterval = &v
	case propRequestResponse:
		v, err := pr.byte()
		if err != nil {
			return err
		}
		if v > 1 {
			return protocolError("Request Response Information is %d, must be 0 or 1", v)
		}
		p.RequestResponseInfo = &v
	case propResponseInfo:
		v, err := pr.string()
		if err != nil {
			return err
		}
		p.ResponseInformation = v
	case propServerReference:
		v, err := pr.string()
		if err != nil {
			return err
		}
		p.ServerReference = v
	case propReasonString:
		v, err := pr.string()
		if err != nil {
			return err
		}
		p.ReasonString = v
	case propReceiveMaximum:
		v, err := pr.uint16()
		if err != nil {
			return err
		}
		if v == 0 {
			return protocolError("Receive Maximum is 0 (MQTT-5.0 §3.1.2.11.3)")
		}
		p.ReceiveMaximum = &v
	case propTopicAliasMaximum:
		v, err := pr.uint16()
		if err != nil {
			return err
		}
		p.TopicAliasMaximum = &v
	case propTopicAlias:
		v, err := pr.uint16()
		if err != nil {
			return err
		}
		if v == 0 {
			return protocolError("Topic Alias is 0 [MQTT-3.3.2-8]")
		}
		p.TopicAlias = &v
	case propMaximumQoS:
		v, err := pr.byte()
		if err != nil {
			return err
		}
		if v > 1 {
			return protocolError("Maximum QoS is %d, must be 0 or 1 (MQTT-5.0 §3.2.2.3.4)", v)
		}
		p.MaximumQoS = &v
	case propRetainAvailable:
		v, err := pr.byte()
		if err != nil {
			return err
		}
		if v > 1 {
			return protocolError("Retain Available is %d, must be 0 or 1", v)
		}
		p.RetainAvailable = &v
	case propUser:
		k, v, err := pr.stringPair()
		if err != nil {
			return err
		}
		p.User = append(p.User, UserProperty{Key: k, Value: v})
	case propMaximumPacketSize:
		v, err := pr.uint32()
		if err != nil {
			return err
		}
		if v == 0 {
			return protocolError("Maximum Packet Size is 0 (MQTT-5.0 §3.1.2.11.4)")
		}
		p.MaximumPacketSize = &v
	case propWildcardSubAvail:
		v, err := pr.byte()
		if err != nil {
			return err
		}
		if v > 1 {
			return protocolError("Wildcard Subscription Available is %d, must be 0 or 1", v)
		}
		p.WildcardSubAvailable = &v
	case propSubIDAvail:
		v, err := pr.byte()
		if err != nil {
			return err
		}
		if v > 1 {
			return protocolError("Subscription Identifier Available is %d, must be 0 or 1", v)
		}
		p.SubIDAvailable = &v
	case propSharedSubAvail:
		v, err := pr.byte()
		if err != nil {
			return err
		}
		if v > 1 {
			return protocolError("Shared Subscription Available is %d, must be 0 or 1", v)
		}
		p.SharedSubAvailable = &v
	default:
		// Unreachable: allowedIn rejects unknown identifiers, which have no
		// context list, before we get here.
		return malformed("unknown property identifier 0x%02X", int(id))
	}
	return nil
}

// encode appends the Property Length and the properties themselves. Fields not
// legal in ctx are dropped rather than emitted, so that a Properties value
// reused across packet types cannot produce a packet the peer must reject.
func (p *Properties) encode(w *writer, ctx Type) error {
	if p == nil {
		w.varByteInt(0)
		return nil
	}

	var pw writer
	put := func(id propID, emit func()) {
		if id.allowedIn(ctx) {
			emit()
		}
	}

	if p.PayloadFormat != nil {
		put(propPayloadFormat, func() { pw.byte(byte(propPayloadFormat)); pw.byte(*p.PayloadFormat) })
	}
	if p.MessageExpiryInterval != nil {
		put(propMessageExpiry, func() { pw.byte(byte(propMessageExpiry)); pw.uint32(*p.MessageExpiryInterval) })
	}
	if p.ContentType != "" {
		if err := checkStringLen("Content Type", p.ContentType); err != nil {
			return err
		}
		put(propContentType, func() { pw.byte(byte(propContentType)); pw.string(p.ContentType) })
	}
	if p.ResponseTopic != "" {
		if err := checkStringLen("Response Topic", p.ResponseTopic); err != nil {
			return err
		}
		put(propResponseTopic, func() { pw.byte(byte(propResponseTopic)); pw.string(p.ResponseTopic) })
	}
	if len(p.CorrelationData) > 0 {
		if err := checkBinaryLen("Correlation Data", p.CorrelationData); err != nil {
			return err
		}
		put(propCorrelationData, func() { pw.byte(byte(propCorrelationData)); pw.binary(p.CorrelationData) })
	}
	for _, id := range p.SubscriptionIdentifiers {
		if id < 1 || id > MaxVarByteInt {
			return protocolError("Subscription Identifier %d is outside 1..%d", id, MaxVarByteInt)
		}
		put(propSubscriptionID, func() { pw.byte(byte(propSubscriptionID)); pw.varByteInt(id) })
	}
	if p.SessionExpiryInterval != nil {
		put(propSessionExpiry, func() { pw.byte(byte(propSessionExpiry)); pw.uint32(*p.SessionExpiryInterval) })
	}
	if p.AssignedClientID != "" {
		if err := checkStringLen("Assigned Client Identifier", p.AssignedClientID); err != nil {
			return err
		}
		put(propAssignedClientID, func() { pw.byte(byte(propAssignedClientID)); pw.string(p.AssignedClientID) })
	}
	if p.ServerKeepAlive != nil {
		put(propServerKeepAlive, func() { pw.byte(byte(propServerKeepAlive)); pw.uint16(*p.ServerKeepAlive) })
	}
	if p.AuthenticationMethod != "" {
		if err := checkStringLen("Authentication Method", p.AuthenticationMethod); err != nil {
			return err
		}
		put(propAuthMethod, func() { pw.byte(byte(propAuthMethod)); pw.string(p.AuthenticationMethod) })
	}
	if len(p.AuthenticationData) > 0 {
		if err := checkBinaryLen("Authentication Data", p.AuthenticationData); err != nil {
			return err
		}
		put(propAuthData, func() { pw.byte(byte(propAuthData)); pw.binary(p.AuthenticationData) })
	}
	if p.RequestProblemInfo != nil {
		put(propRequestProblemInfo, func() { pw.byte(byte(propRequestProblemInfo)); pw.byte(*p.RequestProblemInfo) })
	}
	if p.WillDelayInterval != nil {
		put(propWillDelayInterval, func() { pw.byte(byte(propWillDelayInterval)); pw.uint32(*p.WillDelayInterval) })
	}
	if p.RequestResponseInfo != nil {
		put(propRequestResponse, func() { pw.byte(byte(propRequestResponse)); pw.byte(*p.RequestResponseInfo) })
	}
	if p.ResponseInformation != "" {
		if err := checkStringLen("Response Information", p.ResponseInformation); err != nil {
			return err
		}
		put(propResponseInfo, func() { pw.byte(byte(propResponseInfo)); pw.string(p.ResponseInformation) })
	}
	if p.ServerReference != "" {
		if err := checkStringLen("Server Reference", p.ServerReference); err != nil {
			return err
		}
		put(propServerReference, func() { pw.byte(byte(propServerReference)); pw.string(p.ServerReference) })
	}
	if p.ReasonString != "" {
		if err := checkStringLen("Reason String", p.ReasonString); err != nil {
			return err
		}
		put(propReasonString, func() { pw.byte(byte(propReasonString)); pw.string(p.ReasonString) })
	}
	if p.ReceiveMaximum != nil {
		put(propReceiveMaximum, func() { pw.byte(byte(propReceiveMaximum)); pw.uint16(*p.ReceiveMaximum) })
	}
	if p.TopicAliasMaximum != nil {
		put(propTopicAliasMaximum, func() { pw.byte(byte(propTopicAliasMaximum)); pw.uint16(*p.TopicAliasMaximum) })
	}
	if p.TopicAlias != nil {
		put(propTopicAlias, func() { pw.byte(byte(propTopicAlias)); pw.uint16(*p.TopicAlias) })
	}
	if p.MaximumQoS != nil {
		put(propMaximumQoS, func() { pw.byte(byte(propMaximumQoS)); pw.byte(*p.MaximumQoS) })
	}
	if p.RetainAvailable != nil {
		put(propRetainAvailable, func() { pw.byte(byte(propRetainAvailable)); pw.byte(*p.RetainAvailable) })
	}
	for _, up := range p.User {
		if err := checkStringLen("User Property name", up.Key); err != nil {
			return err
		}
		if err := checkStringLen("User Property value", up.Value); err != nil {
			return err
		}
		put(propUser, func() { pw.byte(byte(propUser)); pw.stringPair(up.Key, up.Value) })
	}
	if p.MaximumPacketSize != nil {
		put(propMaximumPacketSize, func() { pw.byte(byte(propMaximumPacketSize)); pw.uint32(*p.MaximumPacketSize) })
	}
	if p.WildcardSubAvailable != nil {
		put(propWildcardSubAvail, func() { pw.byte(byte(propWildcardSubAvail)); pw.byte(*p.WildcardSubAvailable) })
	}
	if p.SubIDAvailable != nil {
		put(propSubIDAvail, func() { pw.byte(byte(propSubIDAvail)); pw.byte(*p.SubIDAvailable) })
	}
	if p.SharedSubAvailable != nil {
		put(propSharedSubAvail, func() { pw.byte(byte(propSharedSubAvail)); pw.byte(*p.SharedSubAvailable) })
	}

	w.varByteInt(len(pw.buf))
	w.bytes(pw.buf)
	return nil
}
