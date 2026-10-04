package natsmqtt5

import (
	"context"
	"fmt"
)

// Reauthorize re-runs the Authorizer over everything the session for clientID
// holds: each subscription, as at a resume, and its Will Message, as at
// CONNECT. Call it when a principal's permissions change; the broker does not
// poll a policy store it knows nothing about, and does not ask the Authorizer
// on every delivery.
//
// A denied subscription is removed. It stops delivering at once, including
// messages already queued for the connection, and the messages it had in
// flight are withdrawn, so a later resume does not resend them; their
// acknowledgements are still accepted. A denied Will is discarded, including
// one already waiting out its Will Delay Interval. The connection itself is
// left alone: it is still allowed, and a DISCONNECT would publish the very Will
// being revoked. The client is not told which filters it lost; MQTT has no
// packet for that.
//
// The Authorizer sees a subscription with [AuthzRequest].Resume set and a Will
// with Will set, as it does for those checks elsewhere. As there, any error it
// returns is a denial, so an Authorizer that cannot reach its policy should not
// be asked to re-check a fleet during an outage.
//
// It returns nil when no session for clientID is held in this broker's memory:
// a session elsewhere, or only in the session store, is re-checked when it is
// next resumed. It returns ctx's error, having changed nothing further, if ctx
// ends during the sweep.
func (b *Broker) Reauthorize(ctx context.Context, clientID string) error {
	a := b.opts.Authorizer
	if a == nil {
		return nil
	}
	b.mu.Lock()
	s, ok := b.sessions[clientID]
	b.mu.Unlock()
	if !ok {
		return nil
	}
	identity, username := s.principal()

	var denied, surviving []string
	for _, sub := range s.subscriptions() {
		err := a.Authorize(ctx, &AuthzRequest{
			Action:   ActionSubscribe,
			Resume:   true,
			ClientID: clientID,
			Identity: identity,
			Username: username,
			Topic:    sub.filter,
			QoS:      sub.opts.QoS,
		})
		if ctx.Err() != nil {
			// The Authorizer's answer may only reflect the cancelled context.
			return fmt.Errorf("reauthorizing %q: %w", clientID, ctx.Err())
		}
		if err == nil {
			surviving = append(surviving, sub.filter)
			continue
		}
		b.logger.Warn("removing a subscription whose permission was revoked",
			"client_id", clientID, "filter", sub.filter, "error", err)
		denied = append(denied, sub.filter)
		if removed, ok := s.removeSubscription(sub.filter); ok {
			unsubscribeAll(removed)
		}
	}
	if len(denied) > 0 {
		if taken := s.withdrawInflight(denied, surviving); len(taken) > 0 {
			b.logger.Warn("withdrawing unacknowledged messages on revoked filters",
				"client_id", clientID, "packet_ids", taken)
		}
		if c := s.currentConn(); c != nil {
			b.persistSession(c)
		}
	}

	if will := s.currentWill(); will != nil {
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
		if ctx.Err() != nil {
			return fmt.Errorf("reauthorizing %q: %w", clientID, ctx.Err())
		}
		if err != nil {
			b.logger.Warn("discarding a Will Message whose permission was revoked",
				"client_id", clientID, "topic", will.Topic, "error", err)
			s.discardWill(will)
		}
	}
	return nil
}
