package natsmqtt5

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
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
// Any broker will do. One that does not hold the session asks the others
// sharing its NATS connection and SubjectPrefix, and the one holding it runs
// the sweep and answers. When none holds it after [ReauthorizeForwardWait] (or
// when ctx ends first), it returns nil: a session held nowhere, only in the
// session store, is re-checked when it is next resumed. It returns ctx's error,
// having changed nothing further, if ctx ends during this broker's own sweep,
// and the holder's error if its sweep failed.
func (b *Broker) Reauthorize(ctx context.Context, clientID string) error {
	if b.opts.Authorizer == nil {
		return nil
	}
	held, err := b.reauthorizeLocal(ctx, clientID)
	if err != nil || held {
		return err
	}
	return b.forwardReauthorize(ctx, clientID)
}

// reauthorizeLocal sweeps the session for clientID if this broker holds it.
func (b *Broker) reauthorizeLocal(ctx context.Context, clientID string) (held bool, err error) {
	a := b.opts.Authorizer
	if a == nil {
		return false, nil
	}
	b.mu.Lock()
	s, ok := b.sessions[clientID]
	b.mu.Unlock()
	if !ok {
		return false, nil
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
			return true, fmt.Errorf("reauthorizing %q: %w", clientID, ctx.Err())
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
			b.leaveGroup(s, sub.filter)
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
			return true, fmt.Errorf("reauthorizing %q: %w", clientID, ctx.Err())
		}
		if err != nil {
			b.logger.Warn("discarding a Will Message whose permission was revoked",
				"client_id", clientID, "topic", will.Topic, "error", err)
			b.wills.drop(s.discardWill(will))
		}
	}
	return true, nil
}

// ReauthorizeForwardWait is how long [Broker.Reauthorize] waits for another
// broker to say it holds a session this one does not. Every broker answers at
// once, so the wait is spent only when no broker holds it.
const ReauthorizeForwardWait = time.Second

// reauthorizeRemoteTimeout bounds a sweep run for another broker's caller,
// whose own context does not reach here.
const reauthorizeRemoteTimeout = 30 * time.Second

type reauthorizeRequest struct {
	ClientID string `json:"client_id"`
}

type reauthorizeReply struct {
	Held  bool   `json:"held"`
	Error string `json:"error,omitempty"`
}

// reauthorizeSubject is where the brokers sharing a SubjectPrefix listen for
// one another's Reauthorize. It lies outside the prefix, so no MQTT topic maps
// onto it, and carries the prefix, so brokers serving different namespaces on
// one NATS cluster do not sweep each other's Client Identifiers.
func (b *Broker) reauthorizeSubject() string {
	return "_NATSMQTT5.reauthorize." + b.opts.SubjectPrefix
}

// listenForReauthorize answers other brokers' Reauthorize. Every broker
// answers, held or not, so that a caller is not left waiting for the holder
// longer than it takes the holder to sweep.
func (b *Broker) listenForReauthorize() error {
	sub, err := b.nc.Subscribe(b.reauthorizeSubject(), func(m *nats.Msg) {
		var req reauthorizeRequest
		if err := json.Unmarshal(m.Data, &req); err != nil || req.ClientID == "" {
			return
		}
		// Off the NATS dispatcher: a slow Authorizer must not hold up the
		// next request.
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), reauthorizeRemoteTimeout)
			defer cancel()
			held, err := b.reauthorizeLocal(ctx, req.ClientID)
			reply := reauthorizeReply{Held: held}
			if err != nil {
				reply.Error = err.Error()
			}
			data, _ := json.Marshal(reply)
			_ = m.Respond(data)
		}()
	})
	if err != nil {
		return fmt.Errorf("natsmqtt5: listening for reauthorization requests: %w", err)
	}
	b.reauthSub = sub
	return nil
}

// forwardReauthorize asks the other brokers to sweep clientID and waits for
// the one holding it.
func (b *Broker) forwardReauthorize(ctx context.Context, clientID string) error {
	inbox := b.nc.NewRespInbox()
	sub, err := b.nc.SubscribeSync(inbox)
	if err != nil {
		return fmt.Errorf("reauthorizing %q: %w", clientID, err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	data, _ := json.Marshal(reauthorizeRequest{ClientID: clientID})
	if err := b.nc.PublishMsg(&nats.Msg{Subject: b.reauthorizeSubject(), Reply: inbox, Data: data}); err != nil {
		return fmt.Errorf("reauthorizing %q: asking the other brokers: %w", clientID, err)
	}

	wait := ReauthorizeForwardWait
	waitCtx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	for {
		m, err := sub.NextMsgWithContext(waitCtx)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				// No broker holds it, or the caller stopped waiting.
				return nil
			}
			return fmt.Errorf("reauthorizing %q: %w", clientID, err)
		}
		var reply reauthorizeReply
		if json.Unmarshal(m.Data, &reply) != nil || !reply.Held {
			continue
		}
		if reply.Error != "" {
			return fmt.Errorf("reauthorizing %q on the broker holding it: %s", clientID, reply.Error)
		}
		return nil
	}
}
