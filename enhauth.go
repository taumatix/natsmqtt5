package natsmqtt5

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/taumatix/natsmqtt5/v2/packet"
)

// Auther drives enhanced authentication, the challenge/response exchange of
// MQTT-5.0 §4.12 that a CONNECT starts by naming an Authentication Method
// (SCRAM, Kerberos, a signed token) and that AUTH packets carry on. It is set
// in [Options.Auther]. With none set a CONNECT carrying an Authentication Method
// is refused with 0x8C (Bad authentication method), as before.
//
// BeginAuth is called once per exchange, for the CONNECT (and again for every
// re-authentication a client starts, [AuthBegin.Reauth]). The exchange it
// returns holds that exchange's state; the broker calls it from the connection's
// own reader, so it is never used concurrently, and drops it when the exchange
// ends.
//
// A CONNECT that names a method is decided by the Auther alone: the
// [Authenticator], which sees only a user name and password, is not asked. The
// [Authorizer] is, with the identity the exchange returned.
//
// As with an Authenticator, the Client Identifier names a session, and nothing
// downstream stops a principal from naming another's: BeginAuth must refuse a
// CONNECT whose [AuthBegin.ClientID] the authenticated principal does not own
// (or return an [AuthStep.AssignClientID] derived from it).
type Auther interface {
	BeginAuth(ctx context.Context, req *AuthBegin) (AuthExchange, error)
}

// AuthBegin describes the start of an enhanced authentication exchange.
type AuthBegin struct {
	// Method is the Authentication Method the client named. Return
	// [ErrBadAuthMethod] from BeginAuth for one the Auther does not support.
	Method string
	// Data is the Authentication Data the client sent with it, which is the
	// first input of the exchange. It is also passed to the first Next call.
	Data []byte

	ClientID   string
	Username   string
	RemoteAddr net.Addr
	TLS        *tls.ConnectionState

	// Reauth is true for a re-authentication on an established connection
	// (§4.12.1); Identity is then what the connection authenticated as.
	Reauth   bool
	Identity string
}

// AuthExchange is one run of an authentication method.
type AuthExchange interface {
	// Next consumes the client's Authentication Data (the CONNECT's for the
	// first call, then each AUTH's) and says what happens next: a step with
	// Done false sends the step's Data to the client in an AUTH packet with
	// Reason Code 0x18 and waits for the client's reply; Done true accepts.
	//
	// A non-nil error rejects the exchange. A CONNECT is refused with the
	// [ConnectError]'s code (0x87 otherwise; [ErrBadAuthMethod] gives 0x8C); a
	// re-authentication ends in a DISCONNECT with that code, since the
	// connection can no longer be trusted [MQTT-4.12.1-2].
	Next(ctx context.Context, data []byte) (AuthStep, error)
}

// AuthStep is an AuthExchange's answer to the client's input.
type AuthStep struct {
	// Data is sent as Authentication Data: in the AUTH that continues the
	// exchange, or, when Done, in the final CONNACK (or the AUTH 0x00 that ends a
	// re-authentication), where a method puts its server-final message.
	Data []byte
	// Done accepts the client.
	Done bool
	// Identity names the authenticated principal, as [AuthResult.Identity]. A
	// re-authentication must come out as the identity the connection already
	// has, or else the Authorizer is re-run over the session's subscriptions and
	// Will for the new identity, as Broker.Reauthorize does, and those it no
	// longer allows are dropped. If that check cannot be completed the client is
	// disconnected with 0x87.
	Identity string
	// AssignClientID is [AuthResult.AssignClientID]; CONNECT only.
	AssignClientID string
}

// ErrBadAuthMethod is what an Auther returns (bare or wrapped) for an
// Authentication Method it does not support: 0x8C Bad authentication method.
var ErrBadAuthMethod = errors.New("natsmqtt5: unsupported authentication method")

// authCode maps an Auther's error onto the Reason Code the client is given.
func authCode(err error) (packet.ReasonCode, string) {
	var ce *ConnectError
	switch {
	case errors.Is(err, ErrBadAuthMethod):
		return packet.BadAuthenticationMethod, ""
	case errors.As(err, &ce) && isConnackError(ce.Code):
		return ce.Code, ce.Reason
	}
	return packet.NotAuthorized, ""
}

// authProps is the AUTH packet's properties: the connection's method, and
// the data when there is some [MQTT-4.12.0-5].
func authProps(method string, data []byte) *packet.Properties {
	return &packet.Properties{AuthenticationMethod: method, AuthenticationData: data}
}

// enhancedAuth runs the CONNECT side of the exchange, returning the identity
// and the Authentication Data the successful CONNACK carries. It has already
// answered and closed for every failure it returns.
func (c *conn) enhancedAuth(ctx context.Context, cp *packet.Connect, props *packet.Properties) (step AuthStep, err error) {
	method := props.AuthenticationMethod
	exch, err := c.broker.opts.Auther.BeginAuth(ctx, &AuthBegin{
		Method: method, Data: props.AuthenticationData,
		ClientID: cp.ClientID, Username: cp.Username,
		RemoteAddr: c.nc.RemoteAddr(), TLS: c.tlsState(),
	})
	if err != nil {
		return step, c.refuseAuth(err, cp.ClientID)
	}
	data := props.AuthenticationData
	for {
		step, err = exch.Next(ctx, data)
		if err != nil {
			return step, c.refuseAuth(err, cp.ClientID)
		}
		if step.Done {
			c.authMethod = method
			return step, nil
		}
		if err := c.write(&packet.Auth{ReasonCode: packet.ContinueAuthentication, Properties: authProps(method, step.Data)}); err != nil {
			return step, err
		}
		// A client that goes quiet mid-exchange is held no longer than one that
		// never sends its CONNECT.
		if err := c.nc.SetReadDeadline(time.Now().Add(c.broker.opts.connectTimeout)); err != nil {
			return step, err
		}
		pkt, err := packet.Read(c.r, c.broker.opts.maximumPacketSize)
		if err != nil {
			return step, c.rejectHandshake(err)
		}
		auth, ok := pkt.(*packet.Auth)
		switch {
		case !ok:
			c.refuse(packet.ProtocolError, "expected AUTH to continue authentication, got "+pkt.Type().String())
			return step, fmt.Errorf("%s during enhanced authentication", pkt.Type())
		case auth.ReasonCode != packet.ContinueAuthentication: // [MQTT-4.12.0-3]
			c.refuse(packet.ProtocolError, "an AUTH answering the server must carry Reason Code 0x18")
			return step, fmt.Errorf("AUTH with reason code %s during enhanced authentication", auth.ReasonCode)
		case auth.Properties == nil || auth.Properties.AuthenticationMethod != method: // [MQTT-4.12.0-5]
			c.refuse(packet.BadAuthenticationMethod, "the AUTH names a different Authentication Method than the CONNECT")
			return step, errors.New("authentication method changed mid-exchange")
		}
		data = auth.Properties.AuthenticationData
	}
}

func (c *conn) refuseAuth(err error, clientID string) error {
	code, reason := authCode(err)
	c.refuse(code, reason)
	return fmt.Errorf("enhanced authentication refused for %q: %w", clientID, err)
}

// handleAuth is a client-sent AUTH on an established connection: the start
// (0x19) or the continuation (0x18) of a re-authentication (§4.12.1).
func (c *conn) handleAuth(ctx context.Context, p *packet.Auth) error {
	if c.authMethod == "" {
		// The CONNECT named no method (or the broker has no Auther): nothing
		// to re-authenticate [MQTT-4.12.0-7].
		c.sendDisconnect(packet.ProtocolError, "AUTH on a connection that did not use enhanced authentication")
		return errors.New("unexpected AUTH")
	}
	if p.Properties == nil || p.Properties.AuthenticationMethod != c.authMethod { // [MQTT-4.12.1-1]
		c.stopReauth()
		c.sendDisconnect(packet.BadAuthenticationMethod, "the AUTH names a different Authentication Method than the CONNECT")
		return errors.New("authentication method changed on re-authentication")
	}
	switch {
	case p.ReasonCode == packet.ReAuthenticate && c.reauth == nil:
		_, username := c.sess.principal()
		exch, err := c.broker.opts.Auther.BeginAuth(ctx, &AuthBegin{
			Method: c.authMethod, Data: p.Properties.AuthenticationData,
			ClientID: c.sess.clientID, Username: username,
			RemoteAddr: c.nc.RemoteAddr(), TLS: c.tlsState(),
			Reauth: true, Identity: c.authIdentity,
		})
		if err != nil {
			return c.failReauth(err)
		}
		c.reauth = exch
		// A client that starts a re-authentication and leaves it open is
		// dropped like one that stalls a CONNECT.
		c.reauthTimer = time.AfterFunc(c.broker.opts.connectTimeout, func() {
			c.shutdown(packet.NotAuthorized, "re-authentication timed out")
		})
	case p.ReasonCode == packet.ContinueAuthentication && c.reauth != nil:
	default:
		c.stopReauth()
		c.sendDisconnect(packet.ProtocolError, fmt.Sprintf("AUTH with Reason Code %s out of sequence", p.ReasonCode))
		return fmt.Errorf("AUTH %s out of sequence", p.ReasonCode)
	}
	step, err := c.reauth.Next(ctx, p.Properties.AuthenticationData)
	if err != nil {
		return c.failReauth(err)
	}
	if !step.Done {
		return c.write(&packet.Auth{ReasonCode: packet.ContinueAuthentication, Properties: authProps(c.authMethod, step.Data)})
	}
	c.stopReauth()
	if step.Identity != "" && step.Identity != c.authIdentity {
		// The subscriptions and the Will were authorised for the first identity:
		// re-check them for the new one, as Broker.Reauthorize does, before the
		// client is told it succeeded. Failing closed: anything the sweep could
		// not decide would stay held on the old identity's say-so, so it
		// disconnects instead.
		_, username := c.sess.principal()
		c.sess.setPrincipal(step.Identity, username)
		c.authIdentity = step.Identity
		if _, err := c.broker.reauthorizeLocal(ctx, c.sess.clientID); err != nil {
			c.sendDisconnect(packet.NotAuthorized, "re-authentication changed the identity and its permissions could not be re-checked")
			return fmt.Errorf("re-authentication changed the identity: %w", err)
		}
	}
	return c.write(&packet.Auth{ReasonCode: packet.Success, Properties: authProps(c.authMethod, step.Data)})
}

func (c *conn) failReauth(err error) error {
	c.stopReauth()
	code, reason := authCode(err)
	if reason == "" {
		reason = "re-authentication failed"
	}
	c.sendDisconnect(code, reason) // [MQTT-4.12.1-2]
	return fmt.Errorf("re-authentication failed: %w", err)
}

func (c *conn) stopReauth() {
	c.reauth = nil
	if c.reauthTimer != nil {
		c.reauthTimer.Stop()
		c.reauthTimer = nil
	}
}
