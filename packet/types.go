package packet

import "fmt"

// Type is an MQTT Control Packet type, the high nibble of the fixed header's
// first byte (MQTT-5.0 §2.1.2, Table 2-1).
type Type byte

// The fifteen control packet types. Value 0 is forbidden.
const (
	CONNECT     Type = 1
	CONNACK     Type = 2
	PUBLISH     Type = 3
	PUBACK      Type = 4
	PUBREC      Type = 5
	PUBREL      Type = 6
	PUBCOMP     Type = 7
	SUBSCRIBE   Type = 8
	SUBACK      Type = 9
	UNSUBSCRIBE Type = 10
	UNSUBACK    Type = 11
	PINGREQ     Type = 12
	PINGRESP    Type = 13
	DISCONNECT  Type = 14
	AUTH        Type = 15
)

var typeNames = [16]string{
	0: "RESERVED", CONNECT: "CONNECT", CONNACK: "CONNACK", PUBLISH: "PUBLISH",
	PUBACK: "PUBACK", PUBREC: "PUBREC", PUBREL: "PUBREL", PUBCOMP: "PUBCOMP",
	SUBSCRIBE: "SUBSCRIBE", SUBACK: "SUBACK", UNSUBSCRIBE: "UNSUBSCRIBE",
	UNSUBACK: "UNSUBACK", PINGREQ: "PINGREQ", PINGRESP: "PINGRESP",
	DISCONNECT: "DISCONNECT", AUTH: "AUTH",
}

func (t Type) String() string {
	if t > 15 {
		return fmt.Sprintf("Type(%d)", byte(t))
	}
	return typeNames[t]
}

// reservedFlags is the value the low nibble of byte 1 MUST carry for each
// packet type (MQTT-5.0 §2.1.3, Table 2-2). PUBLISH is excluded: its low
// nibble holds DUP, QoS and RETAIN.
var reservedFlags = [16]byte{
	CONNECT: 0, CONNACK: 0, PUBACK: 0, PUBREC: 0, PUBREL: 0x02, PUBCOMP: 0,
	SUBSCRIBE: 0x02, SUBACK: 0, UNSUBSCRIBE: 0x02, UNSUBACK: 0,
	PINGREQ: 0, PINGRESP: 0, DISCONNECT: 0, AUTH: 0,
}

// QoS is an MQTT Quality of Service level (MQTT-5.0 §4.3).
type QoS byte

// The three delivery guarantees. Value 3 is reserved and MUST NOT be used
// [MQTT-3.3.1-4].
const (
	QoS0 QoS = 0 // at most once
	QoS1 QoS = 1 // at least once
	QoS2 QoS = 2 // exactly once
)

func (q QoS) String() string {
	if q > 2 {
		return fmt.Sprintf("QoS(%d)", byte(q))
	}
	return [...]string{"QoS0", "QoS1", "QoS2"}[q]
}

// Valid reports whether q is one of the three defined levels.
func (q QoS) Valid() bool { return q <= 2 }

// ReasonCode is the one-byte result of an operation (MQTT-5.0 §2.4).
// Values below 0x80 indicate success; 0x80 and above indicate failure.
type ReasonCode byte

// Reason codes from MQTT-5.0 Table 2-6. Several share a value because their
// meaning depends on the packet carrying them: 0x00 is Success everywhere
// except DISCONNECT (Normal disconnection) and SUBACK (Granted QoS 0).
const (
	Success                             ReasonCode = 0x00
	NormalDisconnection                 ReasonCode = 0x00
	GrantedQoS0                         ReasonCode = 0x00
	GrantedQoS1                         ReasonCode = 0x01
	GrantedQoS2                         ReasonCode = 0x02
	DisconnectWithWillMessage           ReasonCode = 0x04
	NoMatchingSubscribers               ReasonCode = 0x10
	NoSubscriptionExisted               ReasonCode = 0x11
	ContinueAuthentication              ReasonCode = 0x18
	ReAuthenticate                      ReasonCode = 0x19
	UnspecifiedError                    ReasonCode = 0x80
	MalformedPacket                     ReasonCode = 0x81
	ProtocolError                       ReasonCode = 0x82
	ImplementationSpecificError         ReasonCode = 0x83
	UnsupportedProtocolVersion          ReasonCode = 0x84
	ClientIdentifierNotValid            ReasonCode = 0x85
	BadUserNameOrPassword               ReasonCode = 0x86
	NotAuthorized                       ReasonCode = 0x87
	ServerUnavailable                   ReasonCode = 0x88
	ServerBusy                          ReasonCode = 0x89
	Banned                              ReasonCode = 0x8A
	ServerShuttingDown                  ReasonCode = 0x8B
	BadAuthenticationMethod             ReasonCode = 0x8C
	KeepAliveTimeout                    ReasonCode = 0x8D
	SessionTakenOver                    ReasonCode = 0x8E
	TopicFilterInvalid                  ReasonCode = 0x8F
	TopicNameInvalid                    ReasonCode = 0x90
	PacketIdentifierInUse               ReasonCode = 0x91
	PacketIdentifierNotFound            ReasonCode = 0x92
	ReceiveMaximumExceeded              ReasonCode = 0x93
	TopicAliasInvalid                   ReasonCode = 0x94
	PacketTooLarge                      ReasonCode = 0x95
	MessageRateTooHigh                  ReasonCode = 0x96
	QuotaExceeded                       ReasonCode = 0x97
	AdministrativeAction                ReasonCode = 0x98
	PayloadFormatInvalid                ReasonCode = 0x99
	RetainNotSupported                  ReasonCode = 0x9A
	QoSNotSupported                     ReasonCode = 0x9B
	UseAnotherServer                    ReasonCode = 0x9C
	ServerMoved                         ReasonCode = 0x9D
	SharedSubscriptionsNotSupported     ReasonCode = 0x9E
	ConnectionRateExceeded              ReasonCode = 0x9F
	MaximumConnectTime                  ReasonCode = 0xA0
	SubscriptionIdentifiersNotSupported ReasonCode = 0xA1
	WildcardSubscriptionsNotSupported   ReasonCode = 0xA2
)

// IsError reports whether the code signals failure, i.e. is 0x80 or greater
// (MQTT-5.0 §2.4).
func (r ReasonCode) IsError() bool { return r >= 0x80 }

var reasonNames = map[ReasonCode]string{
	0x00: "Success", 0x01: "Granted QoS 1", 0x02: "Granted QoS 2",
	0x04: "Disconnect with Will Message", 0x10: "No matching subscribers",
	0x11: "No subscription existed", 0x18: "Continue authentication",
	0x19: "Re-authenticate", 0x80: "Unspecified error", 0x81: "Malformed Packet",
	0x82: "Protocol Error", 0x83: "Implementation specific error",
	0x84: "Unsupported Protocol Version", 0x85: "Client Identifier not valid",
	0x86: "Bad User Name or Password", 0x87: "Not authorized",
	0x88: "Server unavailable", 0x89: "Server busy", 0x8A: "Banned",
	0x8B: "Server shutting down", 0x8C: "Bad authentication method",
	0x8D: "Keep Alive timeout", 0x8E: "Session taken over",
	0x8F: "Topic Filter invalid", 0x90: "Topic Name invalid",
	0x91: "Packet Identifier in use", 0x92: "Packet Identifier not found",
	0x93: "Receive Maximum exceeded", 0x94: "Topic Alias invalid",
	0x95: "Packet too large", 0x96: "Message rate too high",
	0x97: "Quota exceeded", 0x98: "Administrative action",
	0x99: "Payload format invalid", 0x9A: "Retain not supported",
	0x9B: "QoS not supported", 0x9C: "Use another server", 0x9D: "Server moved",
	0x9E: "Shared Subscriptions not supported", 0x9F: "Connection rate exceeded",
	0xA0: "Maximum connect time", 0xA1: "Subscription Identifiers not supported",
	0xA2: "Wildcard Subscriptions not supported",
}

func (r ReasonCode) String() string {
	if name, ok := reasonNames[r]; ok {
		return name
	}
	return fmt.Sprintf("ReasonCode(0x%02X)", byte(r))
}
