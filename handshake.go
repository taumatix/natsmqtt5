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

	// Before any refusal below, so a CONNACK that refuses the CONNECT respects
	// the limit as well [MQTT-3.1.2-24].
	if props.MaximumPacketSize != nil {
		c.clientMaxPacketSize = *props.MaximumPacketSize
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
		if c.clientMaxPacketSize > 0 && !c.connackFits(&packet.Connack{
			ReasonCode: packet.Success, Properties: &packet.Properties{AssignedClientID: assigned},
		}) {
			// The Assigned Client Identifier cannot be left out [MQTT-3.2.2-16], and
			// a CONNACK carrying it would exceed the client's Maximum Packet Size
			// [MQTT-3.1.2-24]. Both cannot hold, so say so in a CONNACK that fits
			// and let the client choose a shorter identifier of its own.
			c.refuse(packet.ClientIdentifierNotValid,
				"the Client Identifier this broker would assign does not fit the Maximum Packet Size; send one")
			return fmt.Errorf("assigned client identifier does not fit the client's Maximum Packet Size of %d", c.clientMaxPacketSize)
		}
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
				if !isConnackError(code) {
					// [MQTT-3.2.2-8]: only a Table 3-1 code may reach the wire.
					c.logger.Warn("authenticator returned a reason code a CONNACK cannot carry; sending 0x80",
						"code", code)
					code = packet.UnspecifiedError
				}
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
	// Before the takeover, so a refused CONNECT cannot displace the live
	// connection on the same Client Identifier.
	if err := c.authoriseWill(ctx, cp.Will, clientID, identity, username); err != nil {
		return err
	}

	// Likewise: a policy service that cannot answer must not cost the session
	// its filters, nor displace the connection that holds it.
	if err := c.authoriseResume(ctx, clientID, identity, username, cp.CleanStart); err != nil {
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
		// restoredAway is when a session restored from the store was last
		// released, for the offline queue's replay.
		restoredAway time.Time
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
		if sess == nil && present && c.broker.queue != nil && !rec.awayWas.IsZero() {
			restoredAway = rec.awayWas
		}
	}

	if sess == nil {
		sess = newSession(clientID)
		sess.rewind = c.broker.queue.window()
		sess.holds = c.broker
		if rec != nil && (len(rec.inflightWas) > 0 || len(rec.spillWas) > 0 || len(rec.receivedQoS2Was) > 0 || len(rec.withdrawnWas) > 0) {
			c.broker.restoreSessionState(ctx, sess, rec)
		}
		if !restoredAway.IsZero() {
			sess.markAwayRestored(restoredAway, rec.awayFromSeqWas, rec.awayLateSeqWas, rec.deliveredSinceWas, rec.deliveredWas)
		}
		sess.setUnrestored(stored)
	} else if len(stored) == 0 {
		// A session in memory that an earlier connection was still restoring when
		// this one took it over: finish the job.
		stored = sess.unrestoredSubscriptions()
	}
	if rec != nil {
		c.claimGen = sess.bindRecord(rec, rev)
	}

	sess.setPrincipal(identity, username)
	sess.setExpiry(c.sessionExpiry(props))

	// A client that connects again is live: the Will of the connection it
	// replaces, or one waiting out its delay, is not published
	// [MQTT-3.1.2-8], [MQTT-3.1.3-9]. The old connection's own end then finds
	// nothing of its to take.
	cancelled := sess.cancelWills()
	if prev := sess.attach(c); prev != nil && prev != c {
		prev.close()
	}
	// The resumption cancelled a Will waiting out its delay [MQTT-3.1.3-9];
	// the stored copy goes with it, or a broker that adopted it later would
	// publish it for a client that came back.
	c.broker.wills.drop(cancelled)
	c.sess = sess
	c.broker.registerSession(sess)

	if resumed {
		if err := c.resumeSubscriptions(ctx, stored); err != nil {
			return err
		}
	}

	// What an earlier connection of this client left in the Will store is
	// settled before this one's Will is written; see willStore.settle.
	c.broker.wills.settle(ctx, clientID, resumed)

	if cp.Will != nil {
		var delay time.Duration
		if cp.Will.Properties != nil && cp.Will.Properties.WillDelayInterval != nil {
			delay = time.Duration(*cp.Will.Properties.WillDelayInterval) * time.Second
		}
		var lease *willLease
		if wills := c.broker.wills; wills != nil {
			// The record names the Will Delay Interval as it will be waited:
			// no longer than the session lasts.
			recDelay := delay
			if exp := time.Duration(sess.expiry()) * time.Second; exp < recDelay {
				recDelay = exp
			}
			lease = wills.register(ctx, clientID, cp.Will, recDelay)
		}
		c.broker.wills.drop(sess.setWill(c, cp.Will, delay, lease))
	}

	ack := &packet.Connack{
		SessionPresent: resumed,
		ReasonCode:     packet.Success,
		Properties:     c.connackProperties(assigned, sess.expiry(), props),
	}
	c.fitConnack(ack)
	return c.write(ack)
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
// Both branches withdraw the in-flight messages a denied filter earned. The
// stored branch has an in-flight set to withdraw from because negotiate
// restores the record's before this runs (restore.go); a non-empty stored still
// implies negotiate found no session in memory, so the set is exactly that.
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

	var denied, surviving []string
	for _, st := range stored {
		if c.mayResume(ctx, st.Filter, st.Opts.QoS) == resumeDenied {
			denied = append(denied, st.Filter)
			// The session was a member of a shared subscription it may no longer
			// have; the record is what made it one.
			c.broker.leaveGroup(c.sess, st.Filter)
			continue
		}
		surviving = append(surviving, st.Filter)
		if f := c.broker.resumeGate.Load(); f != nil {
			(*f)(st.Filter)
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
		// Same race as a SUBSCRIBE: mayResume asked the Authorizer, and a
		// second CONNECT on this Client Identifier can take the session over
		// while it was out.
		old, installed, _ := c.sess.installSubscription(c, sub)
		if !installed {
			unsubscribeAll(sub)
			return fmt.Errorf("restoring %q for %q: the session was taken over mid-handshake", st.Filter, c.sess.clientID)
		}
		if old != nil {
			// A record holds each filter once, so nothing should be here;
			// if something is, it is not the subscription that was authorised.
			unsubscribeAll(old)
		}
	}

	// Same round trip as a SUBSCRIBE, for the same reason: the CONNACK is about
	// to tell the client its session is present, so the interest has to have
	// reached the NATS server before the client can publish against it.
	if err := c.flushNATS(ctx); err != nil {
		c.sess.discard()
		c.refuse(packet.ImplementationSpecificError, "the stored subscriptions could not be confirmed with NATS")
		return fmt.Errorf("confirming the restored subscriptions for %q: %w", c.sess.clientID, err)
	}
	c.sess.setUnrestored(nil)
	if len(denied) > 0 {
		// The messages a denied filter earned are in the in-flight set the
		// record restored, and the retransmission after the CONNACK would hand
		// them to this connection [MQTT-4.4.0-1]; see reauthoriseLive.
		if taken := c.sess.withdrawInflight(denied, surviving); len(taken) > 0 {
			c.logger.Warn("withdrawing unacknowledged messages this connection may not receive",
				"client_id", c.sess.clientID, "packet_ids", taken)
		}
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
		if c.mayResume(ctx, sub.filter, sub.opts.QoS) != resumeDenied {
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
			c.broker.leaveGroup(c.sess, sub.filter)
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
			} else {
				c.pending.remove(d.seq)
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
func (c *conn) mayResume(ctx context.Context, filter string, qos packet.QoS) resumeVerdict {
	if c.broker.opts.RestrictDollarTopics && dollarFilter(filter) {
		// A filter stored before the option was turned on (dollar.go).
		c.logger.Warn("dropping a stored subscription to a $ topic",
			"client_id", c.sess.clientID, "filter", filter)
		return resumeDenied
	}
	a := c.broker.opts.Authorizer
	if a == nil {
		return resumeAllowed
	}
	if ok, asked := c.resumeVerdicts[resumeKey{filter, qos}]; asked {
		if !ok {
			c.logger.Warn("dropping a subscription this connection may not resume",
				"client_id", c.sess.clientID, "filter", filter)
			return resumeDenied
		}
		return resumeAllowed
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
	if errors.Is(err, ErrAuthorizerUnavailable) {
		// Only a filter that appeared after authoriseResume looked gets here: it
		// was authorised when it was subscribed, and no CONNECT is left to
		// refuse, so it stays, as Reauthorize leaves one on an outage.
		c.logger.Warn("keeping a subscription the Authorizer could not decide on at resume",
			"client_id", c.sess.clientID, "filter", filter, "error", err)
		return resumeUnknown
	}
	if err != nil {
		c.logger.Warn("dropping a subscription this connection may not resume",
			"client_id", c.sess.clientID, "filter", filter, "error", err)
		return resumeDenied
	}
	return resumeAllowed
}

// resumeVerdict is the answer to whether a resumed session may keep a filter.
// resumeUnknown means the Authorizer could not decide, which keeps the filter.
type resumeVerdict int

const (
	resumeAllowed resumeVerdict = iota
	resumeDenied
	resumeUnknown
)

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
	if wp := cp.Will.Properties; wp != nil && hasWildcard(wp.ResponseTopic) {
		// [MQTT-3.3.2-14] applies to the Will's Response Topic as it does to a
		// PUBLISH's: the Will is published later, as an ordinary PUBLISH.
		c.refuse(packet.ProtocolError,
			fmt.Sprintf("the Will's Response Topic %q contains a wildcard", wp.ResponseTopic))
		return fmt.Errorf("will response topic %q contains a wildcard", wp.ResponseTopic)
	}
	if err := topic.ValidateName(cp.Will.Topic); err != nil {
		c.refuse(packet.TopicNameInvalid, err.Error())
		return fmt.Errorf("will topic %q: %w", cp.Will.Topic, err)
	}
	if c.broker.opts.RestrictDollarTopics && dollarName(cp.Will.Topic) {
		// The Will is published as an ordinary PUBLISH later, MQTT-5.0 §4.7.2.
		c.refuse(packet.TopicNameInvalid, "a Will may not target a topic starting with $")
		return fmt.Errorf("will topic %q starts with $", cp.Will.Topic)
	}
	return nil
}

type resumeKey struct {
	filter string
	qos    packet.QoS
}

// authoriseResume asks the Authorizer about the filters a resuming session
// holds before anything is displaced or claimed, and refuses the CONNECT with
// 0x83 if it answers ErrAuthorizerUnavailable for any of them.
//
// Past this point a refusal is not free: the session is attached, the store
// claim is made, and the record would be written back without the filters
// mayResume dropped. Here the old connection and the record are untouched, so
// the client can retry the CONNECT once the policy service is back. The
// verdicts it gets are kept for mayResume, so a reconnect storm costs the
// policy service one call per filter, not two. A filter that appears between
// this check and the resume (a SUBSCRIBE from the connection being displaced)
// is not in the cache and is asked about in mayResume, where an unavailable
// answer keeps it.
func (c *conn) authoriseResume(ctx context.Context, clientID, identity, username string, cleanStart bool) error {
	a := c.broker.opts.Authorizer
	if a == nil || cleanStart {
		return nil
	}
	var filters []storedSubscription
	if sess := c.broker.peekSession(clientID); sess != nil {
		for _, sub := range sess.subscriptions() {
			filters = append(filters, storedSubscription{Filter: sub.filter, Opts: packet.Subscription{QoS: sub.opts.QoS}})
		}
		filters = append(filters, sess.unrestoredSubscriptions()...)
	}
	if c.broker.persistsSessions() {
		filters = append(filters, c.broker.store.peekSubscriptions(ctx, clientID)...)
	}
	for _, st := range filters {
		key := resumeKey{st.Filter, st.Opts.QoS}
		if _, asked := c.resumeVerdicts[key]; asked {
			continue
		}
		if c.broker.opts.RestrictDollarTopics && dollarFilter(st.Filter) {
			continue
		}
		err := a.Authorize(ctx, &AuthzRequest{
			Action:   ActionSubscribe,
			Resume:   true,
			ClientID: clientID,
			Identity: identity,
			Username: username,
			Topic:    st.Filter,
			QoS:      st.Opts.QoS,
		})
		if errors.Is(err, ErrAuthorizerUnavailable) {
			c.refuse(packet.ImplementationSpecificError, "")
			return fmt.Errorf("resuming %q: the Authorizer could not decide on %q: %w", clientID, st.Filter, err)
		}
		if c.resumeVerdicts == nil {
			c.resumeVerdicts = make(map[resumeKey]bool)
		}
		c.resumeVerdicts[key] = err == nil
	}
	return nil
}

// authoriseWill puts the Will Message past the Authorizer as a publish by the
// connecting principal, refusing the CONNECT with 0x87 if it is denied.
//
// The Will is published on the client's behalf after the client has gone, so
// nothing on the publish path can ask again, and there is no connection left
// to tell by then. A Will the broker would refuse to publish is a Will it
// should not accept — the reasoning [MQTT-3.2.2-12] and [MQTT-3.2.2-13] apply
// to a Will whose QoS or RETAIN it cannot honour.
func (c *conn) authoriseWill(ctx context.Context, will *packet.Will, clientID, identity, username string) error {
	a := c.broker.opts.Authorizer
	if will == nil || a == nil {
		return nil
	}
	err := a.Authorize(ctx, &AuthzRequest{
		Action:   ActionPublish,
		Will:     true,
		ClientID: clientID,
		Identity: identity,
		Username: username,
		Topic:    will.Topic,
		QoS:      will.QoS,
		Retain:   will.Retain,
	})
	if err != nil {
		code := packet.NotAuthorized
		if errors.Is(err, ErrAuthorizerUnavailable) {
			code = packet.ImplementationSpecificError
		}
		c.refuse(code, "")
		return fmt.Errorf("will message on %q denied for %q: %w", will.Topic, clientID, err)
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

// connackFits reports whether ack, encoded, is within the client's Maximum
// Packet Size.
func (c *conn) connackFits(ack *packet.Connack) bool {
	if c.clientMaxPacketSize == 0 {
		return true
	}
	raw, err := packet.Encode(ack)
	return err == nil && uint32(len(raw)) <= c.clientMaxPacketSize
}

// fitConnack leaves out of a success CONNACK what a client with a small Maximum
// Packet Size cannot be sent, so that it hears that it is connected instead of
// nothing at all [MQTT-3.1.2-24], [MQTT-3.1.2-25]. Only what is optional goes,
// and in the order in which leaving it out costs the client least:
//
//  1. the availability flags that state their default (all three are 1 when set
//     here; MQTT-5.0 §3.2.2.3.11 to .13 read an absent one as 1), and Retain
//     Available when it too states the default;
//  2. Topic Alias Maximum: absent means 0, the client sends no alias, and the
//     broker holds it to that (conn.topicAliasMax);
//  3. Maximum Packet Size and then Receive Maximum: absent means no limit
//     stated and 65535, which is then the limit the broker holds the client to
//     (conn.receiveMax). The default Receive Maximum is 1024 and the default
//     Maximum Packet Size 64 MiB, so a client small enough to need this is not
//     asking for what it was not told.
//
// What is never left out is a property the specification requires: the
// Assigned Client Identifier [MQTT-3.2.2-16], Maximum QoS below 2
// [MQTT-3.2.2-9], Retain Available 0 [MQTT-3.2.2-13], Server Keep Alive and
// Session Expiry Interval. A CONNACK still too large with those is discarded
// by writePacket, as any packet is.
func (c *conn) fitConnack(ack *packet.Connack) {
	c.topicAliasMax = c.broker.opts.topicAliasMax
	c.receiveMax = c.broker.opts.receiveMaximum
	if c.clientMaxPacketSize == 0 || ack.Properties == nil || c.connackFits(ack) {
		return
	}
	p := ack.Properties
	steps := []func(){
		func() {
			p.WildcardSubAvailable, p.SubIDAvailable, p.SharedSubAvailable = nil, nil, nil
			if p.RetainAvailable != nil && *p.RetainAvailable == 1 {
				p.RetainAvailable = nil
			}
		},
		func() { p.TopicAliasMaximum, c.topicAliasMax = nil, 0 },
		func() { p.MaximumPacketSize = nil },
		func() { p.ReceiveMaximum, c.receiveMax = nil, 65535 },
	}
	for _, step := range steps {
		step()
		if c.connackFits(ack) {
			return
		}
	}
}

func boolByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}

// isConnackError reports whether code is one of the refusal codes Table 3-1 of
// MQTT-5.0 lists for a CONNACK [MQTT-3.2.2-8].
func isConnackError(code packet.ReasonCode) bool {
	switch code {
	case packet.UnspecifiedError, packet.MalformedPacket, packet.ProtocolError,
		packet.ImplementationSpecificError, packet.UnsupportedProtocolVersion,
		packet.ClientIdentifierNotValid, packet.BadUserNameOrPassword,
		packet.NotAuthorized, packet.ServerUnavailable, packet.ServerBusy,
		packet.Banned, packet.BadAuthenticationMethod, packet.TopicNameInvalid,
		packet.PacketTooLarge, packet.QuotaExceeded, packet.PayloadFormatInvalid,
		packet.RetainNotSupported, packet.QoSNotSupported, packet.UseAnotherServer,
		packet.ServerMoved, packet.ConnectionRateExceeded:
		return true
	}
	return false
}
