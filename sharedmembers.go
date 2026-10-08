package natsmqtt5

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/taumatix/natsmqtt5/topic"
)

// Who is subscribed to a shared subscription.
//
// "A Shared Subscription ends, and any undelivered messages associated with it
// are deleted, when there are no longer any Sessions subscribed to it"
// (MQTT-5.0 §4.8.2). The backlog consumer (shared.go) belongs to the whole group,
// across brokers, so no one broker can tell that its session was the last: the
// brokers keep a key-value entry per member session, keyed by the group and the
// Client Identifier. A session joins by writing its entry and leaves by deleting
// it; the one whose deletion leaves the group with no entry deletes the consumer,
// and the messages it holds go with it.
//
// An entry per member rather than a counter makes joining idempotent, so a
// SUBSCRIBE that replaces a subscription, a session restored on another broker
// and a retry after a failure cannot skew a count, and two last members leaving
// at once both find the group empty and both delete a consumer that is deleted
// once. The entry holds the instance that wrote it, so the end of a session
// that has since moved to another broker (or been replaced by a Clean Start)
// does not remove the entry of its successor.
//
// Two things are not ended by a count. A session that vanishes without
// unsubscribing (its broker was killed) leaves its entry behind: every broker
// rewrites the entries of the sessions it holds, and the bucket drops an entry
// nobody has rewritten for longer than any session can last, which is also how
// long the consumer survives with no member pulling (InactiveThreshold). And a
// member joining just as the last one leaves can find the consumer gone: the
// member's pull loop creates it again (conn.pull).

// shareMemberSlack is added to MaxSessionExpiry for the life of a membership
// entry that is not being rewritten. A variable so that a test need not wait it
// out.
var shareMemberSlack = time.Minute

// shareMembers is the key-value bucket of member sessions, one entry per group
// and Client Identifier.
type shareMembers struct {
	kv jetstream.KeyValue
	// ttl is how long an entry survives without being rewritten.
	ttl time.Duration
}

func newShareMembers(ctx context.Context, js jetstream.JetStream, opts *resolved) (*shareMembers, error) {
	ttl := opts.maxSessionExpiry + shareMemberSlack
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      opts.StreamPrefix + "_share_members",
		Description: "Sessions subscribed to each shared subscription, kept by natsmqtt5",
		Storage:     opts.OfflineQueueStorage,
		Replicas:    opts.OfflineQueueReplicas,
		History:     1,
		TTL:         ttl,
	})
	if err != nil {
		return nil, err
	}
	return &shareMembers{kv: kv, ttl: ttl}, nil
}

// refreshEvery is how often the sessions a broker holds rewrite their entries.
func (m *shareMembers) refreshEvery() time.Duration { return m.ttl / 3 }

// memberKey names the entry of one session in one group. Both halves are
// hashed: a consumer name carries the StreamPrefix, and a Client Identifier is
// any string, neither of which a key is free to hold.
func memberKey(group, clientID string) string {
	g := sha256.Sum256([]byte(group))
	c := sha256.Sum256([]byte(clientID))
	return hex.EncodeToString(g[:12]) + "." + hex.EncodeToString(c[:12])
}

func groupPrefix(group string) string {
	g := sha256.Sum256([]byte(group))
	return hex.EncodeToString(g[:12]) + ".*"
}

// join records that clientID's session, whose instance is the given one, is
// subscribed to group.
func (m *shareMembers) join(ctx context.Context, group, clientID, instance string) error {
	_, err := m.kv.Put(ctx, memberKey(group, clientID), []byte(instance))
	return err
}

// leave removes the session's entry and reports whether the group is left with
// no member. An entry written by another instance is not this session's to
// remove, and then the group is not reported empty.
func (m *shareMembers) leave(ctx context.Context, group, clientID, instance string) (empty bool, err error) {
	key := memberKey(group, clientID)
	entry, err := m.kv.Get(ctx, key)
	switch {
	case err == nil:
		if string(entry.Value()) != instance {
			return false, nil
		}
		if err := m.kv.Delete(ctx, key, jetstream.LastRevision(entry.Revision())); err != nil {
			// Rewritten since it was read: the session is subscribed again.
			if errors.Is(err, jetstream.ErrKeyExists) {
				return false, nil
			}
			return false, err
		}
	case errors.Is(err, jetstream.ErrKeyNotFound):
		// Expired or never written; the count below is still the answer.
	default:
		return false, err
	}
	lister, err := m.kv.ListKeysFiltered(ctx, groupPrefix(group))
	if err != nil {
		return false, err
	}
	for range lister.Keys() {
		_ = lister.Stop()
		return false, nil
	}
	return true, nil
}

// groupName is the name of the consumer behind a shared subscription, which
// names the group across brokers. ok is false for a filter that is not shared.
func (b *Broker) groupName(filter string) (name string, ok bool) {
	share, _, err := topic.SplitShared(filter)
	if err != nil || share == "" || b.queue == nil {
		return "", false
	}
	subject, err := topic.FilterToSubject(filter)
	if err != nil {
		return "", false
	}
	trimmed, ok := topic.TrimPrefix(b.queue.prefix, topic.Prefix(b.opts.SubjectPrefix, subject))
	if !ok {
		return "", false
	}
	return sharedConsumerName(b.opts.StreamPrefix, share, trimmed), true
}

// leaveGroup is called when s no longer holds filter. It takes the session out
// of the shared subscription's members, and deletes the group's backlog when
// that was the last of them. A session that still holds the filter (a SUBSCRIBE
// replaced the subscription, or another connection of the session installed
// it) is still a member and nothing is done.
func (b *Broker) leaveGroup(s *session, filter string) {
	if b.queue == nil || b.queue.members == nil {
		return
	}
	if _, held := s.subscription(filter); held {
		return
	}
	group, ok := b.groupName(filter)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), natsFlushTimeout)
	defer cancel()
	empty, err := b.queue.members.leave(ctx, group, s.clientID, s.instance)
	if err != nil {
		// Left to the entry's expiry and the consumer's inactivity threshold,
		// which end the group late rather than never.
		b.logger.Warn("could not leave a shared subscription's members",
			"client_id", s.clientID, "filter", filter, "error", err)
		return
	}
	if !empty {
		return
	}
	// The held messages of this session were handed back before this, so the
	// consumer deleted here takes them with it, as it does the undelivered ones.
	if err := b.queue.stream.DeleteConsumer(ctx, group); err != nil && !errors.Is(err, jetstream.ErrConsumerNotFound) {
		b.logger.Warn("could not delete the backlog of an ended shared subscription",
			"filter", filter, "error", err)
		return
	}
	b.logger.Debug("a shared subscription ended: no session is subscribed", "filter", filter)
}

// leaveGroups is leaveGroup for every shared subscription of a session that
// has ended.
func (b *Broker) leaveGroups(s *session, subs []*subscription) {
	for _, sub := range subs {
		if sub.share != "" {
			b.leaveGroup(s, sub.filter)
		}
	}
}

// refreshMembers rewrites the entries of the sessions this broker holds, so
// that only a session nobody serves any more lets its entry expire.
func (b *Broker) refreshMembersLoop() {
	defer b.wg.Done()
	t := time.NewTicker(b.queue.members.refreshEvery())
	defer t.Stop()
	for {
		select {
		case <-b.shutdown:
			return
		case <-t.C:
			b.refreshMembers()
		}
	}
}

func (b *Broker) refreshMembers() {
	b.mu.Lock()
	sessions := make([]*session, 0, len(b.sessions))
	for _, s := range b.sessions {
		sessions = append(sessions, s)
	}
	b.mu.Unlock()
	for _, s := range sessions {
		for _, sub := range s.subscriptions() {
			if sub.share == "" || sub.backlog == nil {
				continue
			}
			group, ok := b.groupName(sub.filter)
			if !ok {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), natsFlushTimeout)
			err := b.queue.members.join(ctx, group, s.clientID, s.instance)
			cancel()
			if err != nil {
				b.logger.Debug("could not refresh a shared subscription's member", "filter", sub.filter, "error", err)
			}
		}
	}
}
