package natsmqtt5

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nuid"

	"github.com/taumatix/natsmqtt5/packet"
	"github.com/taumatix/natsmqtt5/topic"
)

// handshake reads the CONNECT, authenticates it, sets up the session and
// answers with CONNACK. It returns the read deadline the connection should
// use: 1.5 times the negotiated Keep Alive [MQTT-3.1.2-22], or zero when Keep
// Alive is disabled.
func (c *conn) handshake(ctx context.Context) (time.Duration, error) {
	// "If the Server does not receive a CONNECT packet within a reasonable
	// amount of time after the Network Connection is established, the Server
	// SHOULD close the Network Connection" (MQTT-5.0 §3.1.4).
	if err := c.nc.SetReadDeadline(time.Now().Add(c.broker.opts.connectTimeout)); err != nil {
		return 0, err
	}
	pkt, err := packet.Read(c.r, c.broker.opts.maximumPacketSize)
	if err != nil {
		return 0, c.rejectHandshake(err)
	}
	cp, ok := pkt.(*packet.Connect)
	if !ok {
		// "the first packet sent from the Client to the Server MUST be a
		// CONNECT packet" [MQTT-3.1.0-1]; anything else closes the connection
		// with no CONNACK.
		return 0, fmt.Errorf("first packet was %s, not CONNECT", pkt.Type())
	}

	if err := c.negotiate(ctx, cp); err != nil {
		return 0, err
	}

	keepAlive := cp.KeepAlive
	if c.broker.opts.ServerKeepAlive != 0 {
		keepAlive = c.broker.opts.ServerKeepAlive
	}
	if keepAlive == 0 {
		return 0, nil
	}
	return time.Duration(keepAlive) * time.Second * 3 / 2, nil
}

// rejectHandshake maps a CONNECT that could not be read onto the CONNACK the
// spec prescribes. A client speaking MQTT 3.1.1 gets 0x84 so it can fall back
// rather than retry blindly [MQTT-3.1.2-2].
func (c *conn) rejectHandshake(err error) error {
	var verErr *packet.ErrUnsupportedProtocolVersion
	switch {
	case errors.As(err, &verErr):
		c.refuse(packet.UnsupportedProtocolVersion,
			fmt.Sprintf("this broker speaks MQTT v5.0; the CONNECT declared version %d", verErr.Version))
	case errors.Is(err, packet.ErrPacketTooLarge):
		c.refuse(packet.PacketTooLarge, "CONNECT exceeds the broker's Maximum Packet Size")
	case errors.Is(err, packet.ErrMalformed):
		c.refuse(packet.MalformedPacket, err.Error())
	case errors.Is(err, packet.ErrProtocol):
		c.refuse(packet.ProtocolError, err.Error())
	}
	return err
}

// refuse sends a CONNACK carrying an error Reason Code and closes the
// connection [MQTT-3.2.2-7].
func (c *conn) refuse(code packet.ReasonCode, reason string) {
	ack := &packet.Connack{ReasonCode: code}
	if reason != "" {
		ack.Properties = &packet.Properties{ReasonString: reason}
	}
	_ = c.write(ack)
	c.close()
}

func (c *conn) negotiate(ctx context.Context, cp *packet.Connect) error {
	props := cp.Properties
	if props == nil {
		props = &packet.Properties{}
	}

	// The broker advertises no authentication method, so a client asking for
	// enhanced authentication must be turned away with 0x8C rather than left
	// waiting for an AUTH that will never come (MQTT-5.0 §4.12).
	if props.AuthenticationMethod != "" {
		c.refuse(packet.BadAuthenticationMethod,
			"this broker does not support enhanced authentication")
		return fmt.Errorf("unsupported authentication method %q", props.AuthenticationMethod)
	}

	c.requestProblemInfo = props.RequestProblemInfo == nil || *props.RequestProblemInfo == 1
	if props.MaximumPacketSize != nil {
		c.clientMaxPacketSize = *props.MaximumPacketSize
	}
	c.clientReceiveMax = 65535
	if props.ReceiveMaximum != nil {
		c.clientReceiveMax = *props.ReceiveMaximum
	}
	c.quota = make(chan struct{}, int(c.clientReceiveMax))

	if err := c.checkWill(cp); err != nil {
		return err
	}

	clientID := cp.ClientID
	assigned := ""
	if clientID == "" {
		// "A Server MAY allow a Client to supply a ClientID that has a length
		// of zero bytes ... MUST assign a unique ClientID" [MQTT-3.1.3-6] and
		// "MUST return the Assigned Client Identifier in the CONNACK"
		// [MQTT-3.1.3-7].
		clientID = "auto-" + nuid.Next()
		assigned = clientID
	}

	identity, username := "", cp.Username
	if a := c.broker.opts.Authenticator; a != nil {
		res, err := a.Authenticate(ctx, &AuthRequest{
			ClientID:   cp.ClientID,
			Username:   cp.Username,
			Password:   cp.Password,
			RemoteAddr: c.nc.RemoteAddr(),
			TLS:        c.tlsState(),
			Properties: props,
		})
		if err != nil {
			code, reason := packet.NotAuthorized, ""
			var ce *ConnectError
			if errors.As(err, &ce) {
				code, reason = ce.Code, ce.Reason
			}
			c.refuse(code, reason)
			return fmt.Errorf("authentication refused for %q: %w", cp.ClientID, err)
		}
		if res != nil {
			identity = res.Identity
			if res.AssignClientID != "" {
				clientID = res.AssignClientID
				assigned = clientID
			}
		}
	}

	// Displace any connection already using this Client Identifier
	// [MQTT-3.1.4-3], and decide whether we may resume its session.
	sess, resumed := c.broker.takeOverSession(clientID, cp.CleanStart)
	if !resumed {
		sess = newSession(clientID)
	}
	sess.identity, sess.username = identity, username
	sess.setExpiry(c.sessionExpiry(props))

	if prev := sess.attach(c); prev != nil && prev != c {
		prev.close()
	}
	c.sess = sess
	c.broker.registerSession(sess)

	if cp.Will != nil {
		var delay time.Duration
		if cp.Will.Properties != nil && cp.Will.Properties.WillDelayInterval != nil {
			delay = time.Duration(*cp.Will.Properties.WillDelayInterval) * time.Second
		}
		sess.setWill(cp.Will, delay)
	}

	ack := &packet.Connack{
		SessionPresent: resumed,
		ReasonCode:     packet.Success,
		Properties:     c.connackProperties(assigned, sess.expiry(), props),
	}
	if err := c.write(ack); err != nil {
		return err
	}
	c.connected.Store(true)
	return nil
}

// checkWill rejects a CONNECT whose Will Message the broker cannot honour,
// which MQTT-5.0 §3.2.2.3.4 and §3.2.2.3.5 require it to do before accepting
// the connection.
func (c *conn) checkWill(cp *packet.Connect) error {
	if cp.Will == nil {
		return nil
	}
	if cp.Will.QoS > c.broker.opts.maxQoS {
		// [MQTT-3.2.2-12]
		c.refuse(packet.QoSNotSupported,
			fmt.Sprintf("Will QoS %d exceeds the broker's maximum of %d", cp.Will.QoS, c.broker.opts.maxQoS))
		return fmt.Errorf("will QoS %d not supported", cp.Will.QoS)
	}
	if cp.Will.Retain && !c.broker.retainAvailable() {
		// [MQTT-3.2.2-13]
		c.refuse(packet.RetainNotSupported, "this broker has retained messages disabled")
		return errors.New("will retain requested but retained messages are disabled")
	}
	if err := topic.ValidateName(cp.Will.Topic); err != nil {
		c.refuse(packet.TopicNameInvalid, err.Error())
		return fmt.Errorf("will topic %q: %w", cp.Will.Topic, err)
	}
	return nil
}

func (c *conn) sessionExpiry(props *packet.Properties) uint32 {
	if props.SessionExpiryInterval == nil {
		// "If the Session Expiry Interval is absent the value 0 is used"
		// (MQTT-5.0 §3.1.2.11.2).
		return 0
	}
	return c.cappedExpiry(*props.SessionExpiryInterval)
}

// connackProperties builds the server's half of the negotiation
// (MQTT-5.0 §3.2.2.3). Every limit the broker imposes is advertised, so a
// conforming client never has to discover one by being disconnected.
func (c *conn) connackProperties(assignedClientID string, expiry uint32, req *packet.Properties) *packet.Properties {
	opts := c.broker.opts
	p := &packet.Properties{
		ReceiveMaximum:    packet.Uint16(opts.receiveMaximum),
		MaximumPacketSize: packet.Uint32(opts.maximumPacketSize),
		TopicAliasMaximum: packet.Uint16(opts.topicAliasMax),
		AssignedClientID:  assignedClientID,
		// Subscription Identifiers, wildcard subscriptions and shared
		// subscriptions are all supported.
		WildcardSubAvailable: packet.Byte(1),
		SubIDAvailable:       packet.Byte(1),
		SharedSubAvailable:   packet.Byte(1),
		RetainAvailable:      packet.Byte(boolByte(c.broker.retainAvailable())),
	}
	// "If a Server does not support QoS 1 or QoS 2 PUBLISH packets it MUST
	// send a Maximum QoS in the CONNACK" [MQTT-3.2.2-9]. Absent means 2.
	if opts.maxQoS < packet.QoS2 {
		p.MaximumQoS = packet.Byte(byte(opts.maxQoS))
	}
	if opts.ServerKeepAlive != 0 {
		p.ServerKeepAlive = packet.Uint16(opts.ServerKeepAlive)
	}
	// Only state the Session Expiry Interval when it differs from what the
	// client asked for, which is what the property is for
	// (MQTT-5.0 §3.2.2.3.2).
	if req.SessionExpiryInterval != nil && *req.SessionExpiryInterval != expiry {
		p.SessionExpiryInterval = packet.Uint32(expiry)
	}
	return p
}

func boolByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}
