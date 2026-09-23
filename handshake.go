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

	if err := c.checkClientID(clientID); err != nil {
		return err
	}

	// Displace any connection already using this Client Identifier
	// [MQTT-3.1.4-3], and decide whether we may resume its session.
	sess, resumed := c.broker.takeOverSession(clientID, cp.CleanStart)

	// With persistence on, the durable record decides ownership, and it may
	// hold a session this broker has no memory of.
	var (
		stored []storedSubscription
		rec    *sessionRecord
		rev    uint64
	)
	if c.broker.persistsSessions() {
		var (
			present bool
			err     error
		)
		rec, rev, present, err = c.broker.store.claim(ctx, clientID, identity, username, cp.CleanStart)
		if err != nil {
			c.refuse(packet.ImplementationSpecificError, "the session store is unavailable")
			return fmt.Errorf("claiming the session for %q: %w", clientID, err)
		}
		// A stored session this broker has no memory of is still a session that
		// is present, and its subscriptions have to be rebuilt. One it does
		// remember already has them live.
		if sess == nil && present {
			stored = rec.Subscriptions
			resumed = true
		}
	}

	if sess == nil {
		sess = newSession(clientID)
	}
	if rec != nil {
		c.claimGen = sess.bindRecord(rec, rev)
	}

	sess.setPrincipal(identity, username)
	sess.setExpiry(c.sessionExpiry(props))

	if prev := sess.attach(c); prev != nil && prev != c {
		prev.close()
	}
	c.sess = sess
	c.broker.registerSession(sess)

	if resumed {
		if err := c.resumeSubscriptions(ctx, stored); err != nil {
			return err
		}
	}

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

// checkClientID enforces the length limit persistence imposes. MQTT-5.0
// §3.1.3.1 lets a server state which Client Identifiers it accepts and answer
// 0x85 for the rest, which is a better outcome than accepting an identifier
// whose session could never be stored.
func (c *conn) checkClientID(clientID string) error {
	if !c.broker.persistsSessions() || len(clientID) <= MaxPersistentClientIDLen {
		return nil
	}
	c.refuse(packet.ClientIdentifierNotValid,
		fmt.Sprintf("this broker stores sessions and accepts a Client Identifier of at most %d bytes",
			MaxPersistentClientIDLen))
	return fmt.Errorf("client identifier of %d bytes exceeds the %d this broker stores",
		len(clientID), MaxPersistentClientIDLen)
}

// resumeSubscriptions makes a resumed session's subscription set live again.
//
// stored is empty when the session came from this broker's memory, in which
// case its NATS subscriptions were never torn down and there is nothing to
// rebuild — only the Authorizer to re-run over the live set, and the durable
// record to bring back in line with what this broker actually holds. It is also
// empty for a durable record that holds no subscriptions, which reaches the
// same branch and finds nothing to walk.
//
// Only the in-memory branch withdraws in-flight messages, and that is safe
// because a non-empty stored implies negotiate found no session in memory: the
// session it is working on was built by newSession moments earlier and its
// in-flight set is empty. A future change that reconciles a record against a
// live session breaks that, and has to withdraw on both branches.
func (c *conn) resumeSubscriptions(ctx context.Context, stored []storedSubscription) error {
	if len(stored) == 0 {
		// Order matters: the reduced set is what gets written, so a filter this
		// connection may not have stops being carried forward. Reversing these
		// leaves the denied filter in the record, where the next broker to
		// claim the session restores it.
		c.reauthoriseLive(ctx)
		c.broker.persistSession(c)
		return nil
	}

	dropped := false
	for _, st := range stored {
		if !c.mayResume(ctx, st.Filter, st.Opts.QoS) {
			dropped = true
			continue
		}
		sub, err := c.rebuild(st)
		if err != nil {
			// Every filter in the record was validated when the client first
			// subscribed, so reaching here means the record is corrupt or NATS
			// refused the subscription. Either way the session cannot be served
			// as the CONNACK is about to promise.
			c.sess.discard()
			c.refuse(packet.ImplementationSpecificError, "the stored subscriptions could not be restored")
			return fmt.Errorf("restoring subscription %q for %q: %w", st.Filter, c.sess.clientID, err)
		}
		c.sess.putSubscription(sub)
	}

	// Same round trip as a SUBSCRIBE, for the same reason: the CONNACK is about
	// to tell the client its session is present, so the interest has to have
	// reached the NATS server before the client can publish against it.
	if err := c.flushNATS(ctx); err != nil {
		c.sess.discard()
		c.refuse(packet.ImplementationSpecificError, "the stored subscriptions could not be confirmed with NATS")
		return fmt.Errorf("confirming the restored subscriptions for %q: %w", c.sess.clientID, err)
	}
	if dropped {
		// Write the reduced set back, so a filter this connection may not have
		// stops being carried forward to the next one.
		c.broker.persistSession(c)
	}
	return nil
}

// reauthoriseLive re-runs the Authorizer over the subscriptions a session
// resumed from this broker's memory already holds, and tears down the ones this
// connection may not have.
//
// The stored branch gets this for free by rebuilding each filter; the in-memory
// branch does not, because nothing was ever torn down. Without it a permission
// narrowed between two connections would not take effect until the session
// expired: the NATS subscriptions go on delivering into whatever connection the
// session is attached to, and this is the connection they would deliver into.
//
// It runs during the handshake, before the CONNACK and before the delivery
// goroutine starts, so a denied filter stops delivering ahead of the client
// being told its session is present. One window is left open and is not closed
// here: sess.attach has already pointed the session's NATS handlers at this
// connection, so a message that reached the NATS client before the unsubscribe
// took effect is already in c.deliveries. dropQueued empties those.
func (c *conn) reauthoriseLive(ctx context.Context) {
	if c.broker.opts.Authorizer == nil {
		// Checked here as well as in mayResume, to skip the subscriptions
		// snapshot on the path every broker without an Authorizer takes.
		return
	}

	// Both sets are collected in one pass. Reading the session twice would let
	// a SUBSCRIBE from the connection this CONNECT displaced — which goes on
	// decoding the packets its socket had already buffered, see retransmit.go —
	// land between the two, and a filter that appeared only in the second read
	// would count as surviving without ever having been put past the Authorizer.
	var denied, surviving []string
	for _, sub := range c.sess.subscriptions() {
		if c.mayResume(ctx, sub.filter, sub.opts.QoS) {
			surviving = append(surviving, sub.filter)
			continue
		}
		denied = append(denied, sub.filter)
		// The same two steps handleUnsubscribe takes, in the same order.
		// Neither order closes the delivery window on its own: a message is
		// delivered against the *subscription and never against the session's
		// map, so removing first does not stop a delivery and unsubscribing
		// first does not stop one already in the NATS client's hands. That is
		// what dropQueued below is for.
		if removed, ok := c.sess.removeSubscription(sub.filter); ok {
			unsubscribeAll(removed)
		}
	}
	if len(denied) == 0 {
		return
	}

	c.dropQueued(surviving)

	// The subscription is only half of what a denied filter leaves behind: the
	// messages it already earned are still in the in-flight set, and
	// retransmission would hand them to this connection at CONNACK time
	// [MQTT-4.4.0-1].
	if taken := c.sess.withdrawInflight(denied, surviving); len(taken) > 0 {
		c.logger.Warn("withdrawing unacknowledged messages this connection may not receive",
			"client_id", c.sess.clientID, "packet_ids", taken)
	}
}

// dropQueued discards the deliveries already queued for this connection that no
// surviving filter matches.
//
// The queue is this connection's own and nothing is reading it yet —
// deliverLoop starts after the handshake returns — so draining and refilling it
// keeps the order the survivors arrived in. A message enqueued by a NATS
// dispatcher that was already inside onNATSMessage when the unsubscribe landed
// can still slip in behind them; that window is one message wide and closing it
// means checking every delivery against the session on the hot path.
func (c *conn) dropQueued(surviving []string) {
	for queued := len(c.deliveries); queued > 0; queued-- {
		select {
		case d := <-c.deliveries:
			if topic.MatchAny(surviving, d.topic) {
				c.deliveries <- d
			}
		default:
			return
		}
	}
}

// mayResume re-runs the Authorizer over one filter of a resumed session.
//
// A session is identified by its Client Identifier alone, and it can be claimed
// by any connection the Authenticator lets use that identifier — across
// brokers and across restarts, and equally within one broker's memory. Resuming
// a filter unchecked would let a narrowed permission be outlived by the
// subscription it was meant to remove.
//
// A denied filter is dropped rather than refused, because a CONNACK has no
// per-filter Reason Code to carry the refusal. The client is free to subscribe
// again, and will get an honest 0x87 in the SUBACK when it does.
func (c *conn) mayResume(ctx context.Context, filter string, qos packet.QoS) bool {
	a := c.broker.opts.Authorizer
	if a == nil {
		return true
	}
	identity, username := c.sess.principal()
	err := a.Authorize(ctx, &AuthzRequest{
		Action:   ActionSubscribe,
		Resume:   true,
		ClientID: c.sess.clientID,
		Identity: identity,
		Username: username,
		Topic:    filter,
		QoS:      qos,
	})
	if err != nil {
		c.logger.Warn("dropping a subscription this connection may not resume",
			"client_id", c.sess.clientID, "filter", filter, "error", err)
		return false
	}
	return true
}

// rebuild turns a stored subscription back into a live one. The subject and the
// share name are recomputed rather than read, so a broker whose SubjectPrefix
// differs from the one that stored the record subscribes where it now belongs.
func (c *conn) rebuild(st storedSubscription) (*subscription, error) {
	share, _, err := topic.SplitShared(st.Filter)
	if err != nil {
		return nil, err
	}
	subject, err := topic.FilterToSubject(st.Filter)
	if err != nil {
		return nil, err
	}

	sub := &subscription{
		filter:     st.Filter,
		subject:    topic.Prefix(c.broker.opts.SubjectPrefix, subject),
		share:      share,
		opts:       st.Opts,
		grantedQoS: st.GrantedQoS,
		id:         st.ID,
	}
	if err := c.bindNATS(sub); err != nil {
		return nil, err
	}
	return sub, nil
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
