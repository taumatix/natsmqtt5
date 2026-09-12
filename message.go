package natsmqtt5

import (
	"strconv"
	"strings"

	"github.com/nats-io/nats.go"

	"github.com/taumatix/natsmqtt5/packet"
)

// MQTT metadata rides on NATS headers so that the NATS subject stays exactly
// what topic.NameToSubject produced and a NATS-native subscriber can read the
// payload without knowing anything about MQTT.
//
// The header names are part of the wire contract between brokers sharing a
// NATS server, so they are frozen: adding a header is fine, renaming one is
// not.
const (
	hdrQoS             = "Mqtt5-Qos"
	hdrRetain          = "Mqtt5-Retain"
	hdrPayloadFormat   = "Mqtt5-Payload-Format"
	hdrMessageExpiry   = "Mqtt5-Message-Expiry"
	hdrContentType     = "Mqtt5-Content-Type"
	hdrResponseTopic   = "Mqtt5-Response-Topic"
	hdrCorrelationData = "Mqtt5-Correlation-Data"
	hdrUserProperty    = "Mqtt5-User"
	// hdrOrigin carries the publishing Client Identifier, which the No Local
	// subscription option needs [MQTT-3.8.3-3].
	hdrOrigin = "Mqtt5-Origin"
)

// Header values must survive the NATS protocol's line-oriented framing, and
// MQTT strings and Correlation Data may contain any bytes. escapeHeader
// percent-encodes only what would break that framing, so ordinary values such
// as "application/json" stay readable to someone running `nats sub`.
func escapeHeader(s string) string {
	if !needsEscape(s) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s) + 8)
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if unsafeHeaderByte(ch) {
			const hex = "0123456789ABCDEF"
			b.WriteByte('%')
			b.WriteByte(hex[ch>>4])
			b.WriteByte(hex[ch&0x0F])
			continue
		}
		b.WriteByte(ch)
	}
	return b.String()
}

func needsEscape(s string) bool {
	for i := 0; i < len(s); i++ {
		if unsafeHeaderByte(s[i]) {
			return true
		}
	}
	return false
}

// unsafeHeaderByte reports the bytes escapeHeader must encode: the escape
// character itself, and anything that would terminate or corrupt a header
// line.
func unsafeHeaderByte(ch byte) bool {
	return ch == '%' || ch < 0x20 || ch == 0x7F
}

func unescapeHeader(s string) string {
	if !strings.ContainsRune(s, '%') {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '%' && i+2 < len(s) {
			hi, okHi := hexVal(s[i+1])
			lo, okLo := hexVal(s[i+2])
			if okHi && okLo {
				b.WriteByte(hi<<4 | lo)
				i += 2
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

func hexVal(ch byte) (byte, bool) {
	switch {
	case ch >= '0' && ch <= '9':
		return ch - '0', true
	case ch >= 'A' && ch <= 'F':
		return ch - 'A' + 10, true
	case ch >= 'a' && ch <= 'f':
		return ch - 'a' + 10, true
	}
	return 0, false
}

// toNATS builds the NATS message carrying an MQTT application message. The
// properties the spec requires the server to forward unaltered — Payload
// Format Indicator, Content Type, Response Topic, Correlation Data and every
// User Property, in order — all travel as headers
// [MQTT-3.3.2-4, MQTT-3.3.2-15, MQTT-3.3.2-16, MQTT-3.3.2-17, MQTT-3.3.2-20].
func toNATS(subject, originClientID string, p *packet.Publish) *nats.Msg {
	msg := nats.NewMsg(subject)
	msg.Data = p.Payload

	msg.Header.Set(hdrQoS, strconv.Itoa(int(p.QoS)))
	if p.Retain {
		msg.Header.Set(hdrRetain, "1")
	}
	if originClientID != "" {
		msg.Header.Set(hdrOrigin, escapeHeader(originClientID))
	}

	props := p.Properties
	if props == nil {
		return msg
	}
	if props.PayloadFormat != nil {
		msg.Header.Set(hdrPayloadFormat, strconv.Itoa(int(*props.PayloadFormat)))
	}
	if props.MessageExpiryInterval != nil {
		msg.Header.Set(hdrMessageExpiry, strconv.FormatUint(uint64(*props.MessageExpiryInterval), 10))
	}
	if props.ContentType != "" {
		msg.Header.Set(hdrContentType, escapeHeader(props.ContentType))
	}
	if props.ResponseTopic != "" {
		msg.Header.Set(hdrResponseTopic, escapeHeader(props.ResponseTopic))
	}
	if len(props.CorrelationData) > 0 {
		msg.Header.Set(hdrCorrelationData, escapeHeader(string(props.CorrelationData)))
	}
	// User Properties keep their order and may repeat a name, so they go into
	// one header key as an ordered list of escaped "name value" pairs.
	for _, up := range props.User {
		msg.Header.Add(hdrUserProperty, escapeHeader(up.Key)+" "+escapeHeader(up.Value))
	}
	return msg
}

// fromNATS reverses toNATS. A message with no MQTT headers — one published by
// a NATS-native client straight onto the subject — is delivered as a QoS 0
// message with no properties, which is what makes the broker usable as a
// bridge in both directions.
func fromNATS(msg *nats.Msg) (qos packet.QoS, retain bool, origin string, props *packet.Properties) {
	props = &packet.Properties{}
	if msg.Header == nil {
		return packet.QoS0, false, "", props
	}

	if v := msg.Header.Get(hdrQoS); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 && n <= 2 {
			qos = packet.QoS(n)
		}
	}
	retain = msg.Header.Get(hdrRetain) == "1"
	origin = unescapeHeader(msg.Header.Get(hdrOrigin))

	if v := msg.Header.Get(hdrPayloadFormat); v != "" {
		if n, err := strconv.Atoi(v); err == nil && (n == 0 || n == 1) {
			props.PayloadFormat = packet.Byte(byte(n))
		}
	}
	if v := msg.Header.Get(hdrMessageExpiry); v != "" {
		if n, err := strconv.ParseUint(v, 10, 32); err == nil {
			props.MessageExpiryInterval = packet.Uint32(uint32(n))
		}
	}
	props.ContentType = unescapeHeader(msg.Header.Get(hdrContentType))
	props.ResponseTopic = unescapeHeader(msg.Header.Get(hdrResponseTopic))
	if v := msg.Header.Get(hdrCorrelationData); v != "" {
		props.CorrelationData = []byte(unescapeHeader(v))
	}
	for _, raw := range msg.Header.Values(hdrUserProperty) {
		key, value, found := strings.Cut(raw, " ")
		if !found {
			continue
		}
		props.User = append(props.User, packet.UserProperty{
			Key:   unescapeHeader(key),
			Value: unescapeHeader(value),
		})
	}
	return qos, retain, origin, props
}
