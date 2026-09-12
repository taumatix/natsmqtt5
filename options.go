// Package natsmqtt5 is an MQTT version 5.0 broker that uses an existing NATS
// server as its messaging core.
//
// The broker is a NATS client, not a fork of nats-server. Point it at a NATS
// URL you already run and it speaks MQTT v5 on a TCP port, translating topics
// to NATS subjects, fan-out to NATS subscriptions, shared subscriptions to
// NATS queue groups and retained messages to a JetStream stream. Two brokers
// against the same NATS server see each other's traffic, so an MQTT client on
// one reaches a subscriber on the other.
//
// The subject mapping is byte-for-byte the one nats-server uses for its own
// MQTT 3.1.1 support, so topics land where a NATS-native subscriber expects.
//
// Minimal use:
//
//	b, err := natsmqtt5.New(natsmqtt5.Options{NATSURL: "nats://localhost:4222"})
//	if err != nil {
//		return err
//	}
//	defer b.Close()
//	return b.Serve(ctx)
//
// Section numbers in the documentation refer to the OASIS MQTT v5.0 Standard
// of 07 March 2019.
package natsmqtt5

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/taumatix/natsmqtt5/packet"
)

// Defaults applied to the zero value of the corresponding Options field.
const (
	DefaultListen        = ":1883"
	DefaultSubjectPrefix = "mqtt"
	DefaultStreamPrefix  = "MQTT5"
	// DefaultReceiveMaximum bounds how many QoS 1 and QoS 2 publications a
	// client may have in flight towards the broker (MQTT-5.0 §3.2.2.3.3).
	DefaultReceiveMaximum = 1024
	// DefaultTopicAliasMaximum is the highest Topic Alias the broker accepts
	// from a client (MQTT-5.0 §3.1.2.11.5).
	DefaultTopicAliasMaximum = 64
	// DefaultMaximumPacketSize caps an inbound packet at 64 MiB
	// (MQTT-5.0 §3.2.2.3.6).
	DefaultMaximumPacketSize = 64 << 20
	// DefaultConnectTimeout bounds how long a new connection may stay silent
	// before sending its CONNECT (MQTT-5.0 §3.1.4, step 1).
	DefaultConnectTimeout = 10 * time.Second
	// DefaultMaxSessionExpiry caps the Session Expiry Interval a client may
	// request. The broker returns the capped value in CONNACK, as
	// MQTT-5.0 §3.2.2.3.2 provides for.
	DefaultMaxSessionExpiry = time.Hour
)

// Options configures a Broker. The zero value is usable: it connects to
// nats.DefaultURL and listens on :1883.
//
// Fields are only ever added to this struct, never removed or repurposed, so
// a composite literal with field names keeps compiling across versions.
type Options struct {
	// NATSURL is the NATS server to use. Defaults to nats.DefaultURL. Ignored
	// when Conn is set.
	NATSURL string
	// NATSOptions are passed to nats.Connect for credentials, TLS and so on.
	// Ignored when Conn is set.
	NATSOptions []nats.Option
	// Conn is an already-established NATS connection to use instead of
	// dialling. The broker does not close a connection it did not open.
	Conn *nats.Conn

	// Listen is the TCP address to accept MQTT connections on. Defaults to
	// DefaultListen. Ignored when Listener is set.
	Listen string
	// Listener is an already-open listener to accept on instead of binding
	// Listen. The broker closes it on shutdown.
	Listener net.Listener
	// TLSConfig, when non-nil, wraps the listener in TLS.
	TLSConfig *tls.Config

	// SubjectPrefix namespaces every NATS subject the broker publishes MQTT
	// traffic on, keeping it clear of NATS system subjects. Defaults to
	// DefaultSubjectPrefix. Set it to the same value on every broker that
	// should share traffic.
	SubjectPrefix string
	// StreamPrefix names the JetStream assets the broker creates. Defaults to
	// DefaultStreamPrefix. Brokers sharing a SubjectPrefix must share this.
	StreamPrefix string
	// DisableRetained turns off retained-message support, so no JetStream
	// assets are created and CONNACK advertises Retain Available = 0
	// (MQTT-5.0 §3.2.2.3.5). Use it against a NATS server without JetStream.
	DisableRetained bool
	// RetainedStorage selects file or memory storage for the retained-message
	// stream. Defaults to file storage.
	RetainedStorage jetstream.StorageType
	// RetainedReplicas is the JetStream replica count for the retained-message
	// stream. Defaults to 1.
	RetainedReplicas int

	// Authenticator, when non-nil, decides whether a CONNECT is accepted. When
	// nil every connection is accepted, which is only appropriate on a trusted
	// network.
	Authenticator Authenticator
	// Authorizer, when non-nil, decides whether a session may publish to or
	// subscribe to a topic. When nil everything is permitted.
	Authorizer Authorizer

	// ReceiveMaximum is advertised in CONNACK and bounds the client's in-flight
	// QoS 1 and QoS 2 publications. Defaults to DefaultReceiveMaximum.
	ReceiveMaximum uint16
	// MaximumQoS is the highest QoS the broker accepts in a PUBLISH and grants
	// on a SUBSCRIBE, advertised in CONNACK [MQTT-3.2.2-9]. Values above 2 are
	// clamped. Leave it nil for the default of 2; it is a pointer so that
	// Ptr(uint8(0)) can express a broker that only does QoS 0, which the zero
	// value would otherwise make unreachable.
	MaximumQoS *uint8
	// TopicAliasMaximum is advertised in CONNACK. Zero disables inbound topic
	// aliases; leave it unset for DefaultTopicAliasMaximum.
	TopicAliasMaximum *uint16
	// MaximumPacketSize caps an inbound packet. Defaults to
	// DefaultMaximumPacketSize.
	MaximumPacketSize uint32
	// ServerKeepAlive, when non-zero, overrides the client's requested Keep
	// Alive and is returned in CONNACK [MQTT-3.1.2-21].
	ServerKeepAlive uint16
	// ConnectTimeout bounds the wait for a CONNECT on a new connection.
	// Defaults to DefaultConnectTimeout.
	ConnectTimeout time.Duration
	// MaxSessionExpiry caps a requested Session Expiry Interval. Defaults to
	// DefaultMaxSessionExpiry.
	MaxSessionExpiry time.Duration

	// Logger receives broker events. Defaults to slog.Default().
	Logger *slog.Logger
}

// ErrInvalidOptions reports an Options value the broker cannot run with.
var ErrInvalidOptions = errors.New("natsmqtt5: invalid options")

// Ptr returns a pointer to v. The optional Options fields are pointers so that
// "not set" is distinguishable from a meaningful zero, and this saves callers
// declaring a variable to take the address of.
//
//	natsmqtt5.Options{MaximumQoS: natsmqtt5.Ptr(uint8(1))}
func Ptr[T any](v T) *T { return &v }

// resolved is Options with defaults filled in and invariants checked, so the
// rest of the broker never re-derives a default or re-validates a field.
type resolved struct {
	Options
	maxQoS            packet.QoS
	topicAliasMax     uint16
	connectTimeout    time.Duration
	maxSessionExpiry  time.Duration
	maximumPacketSize uint32
	receiveMaximum    uint16
	logger            *slog.Logger
}

func (o Options) resolve() (*resolved, error) {
	r := &resolved{Options: o}

	if r.Listen == "" {
		r.Listen = DefaultListen
	}
	if r.SubjectPrefix == "" {
		r.SubjectPrefix = DefaultSubjectPrefix
	}
	if r.StreamPrefix == "" {
		r.StreamPrefix = DefaultStreamPrefix
	}
	if r.NATSURL == "" {
		r.NATSURL = nats.DefaultURL
	}
	if r.RetainedReplicas == 0 {
		r.RetainedReplicas = 1
	}

	r.receiveMaximum = o.ReceiveMaximum
	if r.receiveMaximum == 0 {
		r.receiveMaximum = DefaultReceiveMaximum
	}
	r.maximumPacketSize = o.MaximumPacketSize
	if r.maximumPacketSize == 0 {
		r.maximumPacketSize = DefaultMaximumPacketSize
	}
	r.connectTimeout = o.ConnectTimeout
	if r.connectTimeout == 0 {
		r.connectTimeout = DefaultConnectTimeout
	}
	r.maxSessionExpiry = o.MaxSessionExpiry
	if r.maxSessionExpiry == 0 {
		r.maxSessionExpiry = DefaultMaxSessionExpiry
	}

	r.maxQoS = packet.QoS2
	if o.MaximumQoS != nil && *o.MaximumQoS < 2 {
		r.maxQoS = packet.QoS(*o.MaximumQoS)
	}

	r.topicAliasMax = DefaultTopicAliasMaximum
	if o.TopicAliasMaximum != nil {
		r.topicAliasMax = *o.TopicAliasMaximum
	}

	r.logger = o.Logger
	if r.logger == nil {
		r.logger = slog.Default()
	}

	// A subject prefix has to be a single well-formed NATS token, or every
	// subject derived from it is malformed.
	if strings.ContainsAny(r.SubjectPrefix, " \t\r\n*>") {
		return nil, fmt.Errorf("%w: SubjectPrefix %q contains a character that is not valid in a NATS subject",
			ErrInvalidOptions, r.SubjectPrefix)
	}
	if strings.ContainsAny(r.StreamPrefix, " \t\r\n.*>") {
		return nil, fmt.Errorf("%w: StreamPrefix %q contains a character that is not valid in a JetStream stream name",
			ErrInvalidOptions, r.StreamPrefix)
	}
	return r, nil
}

// AuthRequest describes a connection attempt for an Authenticator.
type AuthRequest struct {
	// ClientID is the Client Identifier from CONNECT, before the broker
	// assigns one for an empty value.
	ClientID string
	// Username is empty when the User Name Flag was not set.
	Username string
	// Password is nil when the Password Flag was not set. MQTT v5 permits a
	// password with no username (MQTT-5.0 §3.1.2.9).
	Password []byte
	// RemoteAddr is the client's network address.
	RemoteAddr net.Addr
	// TLS is the connection's TLS state, or nil for a plaintext connection.
	// Use it for client-certificate authentication.
	TLS *tls.ConnectionState
	// Properties are the CONNECT properties, including any User Properties.
	Properties *packet.Properties
}

// AuthResult is an Authenticator's verdict.
type AuthResult struct {
	// Identity, when non-empty, names the authenticated principal. It is
	// passed to the Authorizer and included in log records; it does not change
	// the Client Identifier.
	Identity string
	// AssignClientID, when non-empty, overrides the Client Identifier for the
	// session and is returned to the client as the Assigned Client Identifier
	// [MQTT-3.1.3-7].
	AssignClientID string
}

// Authenticator decides whether a CONNECT is accepted.
//
// Returning a nil error accepts the connection. Returning an error rejects it;
// wrap or return a *ConnectError to choose the CONNACK Reason Code, otherwise
// the broker answers 0x87 (Not authorized) and does not disclose the reason.
type Authenticator interface {
	Authenticate(ctx context.Context, req *AuthRequest) (*AuthResult, error)
}

// AuthenticatorFunc adapts a function to the Authenticator interface.
type AuthenticatorFunc func(ctx context.Context, req *AuthRequest) (*AuthResult, error)

// Authenticate calls f.
func (f AuthenticatorFunc) Authenticate(ctx context.Context, req *AuthRequest) (*AuthResult, error) {
	return f(ctx, req)
}

// ConnectError carries the CONNACK Reason Code an Authenticator wants the
// broker to send. Any code of 0x80 or greater from MQTT-5.0 Table 3-1 is
// valid; common choices are BadUserNameOrPassword (0x86), NotAuthorized
// (0x87), Banned (0x8A) and ClientIdentifierNotValid (0x85).
type ConnectError struct {
	Code   packet.ReasonCode
	Reason string
}

func (e *ConnectError) Error() string {
	if e.Reason != "" {
		return fmt.Sprintf("natsmqtt5: connection refused: %s (%s)", e.Code, e.Reason)
	}
	return fmt.Sprintf("natsmqtt5: connection refused: %s", e.Code)
}

// Action is an operation an Authorizer decides on.
type Action int

const (
	// ActionPublish is a client publishing to a Topic Name.
	ActionPublish Action = iota
	// ActionSubscribe is a client subscribing to a Topic Filter.
	ActionSubscribe
)

func (a Action) String() string {
	if a == ActionPublish {
		return "publish"
	}
	return "subscribe"
}

// AuthzRequest describes an operation for an Authorizer.
type AuthzRequest struct {
	Action Action
	// ClientID is the session's Client Identifier, after any assignment.
	ClientID string
	// Identity is the AuthResult.Identity from authentication, if any.
	Identity string
	// Username is the User Name from CONNECT, if any.
	Username string
	// Topic is the Topic Name for ActionPublish or the Topic Filter, shared
	// prefix included, for ActionSubscribe.
	Topic string
	// QoS is the requested QoS.
	QoS packet.QoS
	// Retain is set for a publish with the RETAIN flag.
	Retain bool
}

// Authorizer decides whether a session may publish to or subscribe to a topic.
// A nil error permits the operation.
//
// A denied publish is answered with 0x87 (Not authorized) in the PUBACK or
// PUBREC, or dropped silently at QoS 0, as MQTT-5.0 §3.3.4 requires. A denied
// subscription is answered with 0x87 in the SUBACK for that filter.
type Authorizer interface {
	Authorize(ctx context.Context, req *AuthzRequest) error
}

// AuthorizerFunc adapts a function to the Authorizer interface.
type AuthorizerFunc func(ctx context.Context, req *AuthzRequest) error

// Authorize calls f.
func (f AuthorizerFunc) Authorize(ctx context.Context, req *AuthzRequest) error {
	return f(ctx, req)
}
