package natsmqtt5

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/taumatix/natsmqtt5/v2/topic"
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
// long the consumer survives with no member pulling (InactiveThreshold). With
// persistent sessions a surviving broker does not wait that long: once the
// session store has reclaimed the session's record, the entry goes too
// (sweepShareMembers). And a
// member joining just as the last one leaves can find the consumer gone: the
// member's pull loop creates it again (conn.pull).

// shareMemberSlack is added to MaxSessionExpiry for the life of a membership
// entry that is not being rewritten. A variable so that a test need not wait it
// out.
var shareMemberSlack = time.Minute

// shareMemberSweepGrace is how long an entry must have gone unrewritten before
// the sweep (sweepShareMembers) may remove it because its session has no record.
// A live member's entry is rewritten every refreshEvery, and a session's record
// is written by its first CONNECT, before any SUBSCRIBE writes an entry, so an
// entry this old with no record belongs to nobody. A variable so that a test
// need not wait it out.
var shareMemberSweepGrace = time.Minute

// memberValue is what an entry holds: the instance that wrote it (the one thing
// leave compares), then the consumer's name and the Client Identifier, which the
// key only holds hashed and which the sweep needs. None contains NUL (a
// Client Identifier is an MQTT UTF-8 string, which may not). An entry written
// before this held the instance alone, and is never swept, only left or lapsed.
func memberValue(instance, group, clientID string) []byte {
	return []byte(instance + "\x00" + group + "\x00" + clientID)
}

func parseMemberValue(v []byte) (instance, group, clientID string) {
	parts := strings.SplitN(string(v), "\x00", 3)
	instance = parts[0]
	if len(parts) == 3 {
		group, clientID = parts[1], parts[2]
	}
	return instance, group, clientID
}

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

// Serialising a group's last leave against a join.
//
// A joiner records its entry, makes sure the consumer exists and answers its
// SUBACK; a last leaver deletes its entry, finds the group empty and deletes the
// consumer. JetStream has no conditional consumer delete, so a joiner whose
// consumer was made before the leaver's delete lost it after its SUBACK, and the
// consumer its puller made again started at the next message: what was
// published in between was in no backlog. Both sides therefore take a short
// lock on the group (a create-only key, which the stream orders for every
// broker) around what must not interleave: the joiner around "make sure the
// consumer exists", the leaver around "is the group still empty? delete the
// consumer". Whichever comes second sees the other's result: a joiner after the
// delete makes a consumer that begins before its SUBACK; a leaver after the join
// finds the joiner's entry and keeps the consumer. A lock a broker died holding
// is taken over after shareLockStale. Brokers of earlier releases do not take
// the lock, and between them the race is as it was.

// shareLockStale is how long a group's lock may be held before another broker
// takes it over. A variable so that a test need not wait it out.
var shareLockStale = 10 * time.Second

const (
	lockKeyPrefix  = "lock."
	startKeyPrefix = "start."
)

func groupHash(group string) string {
	g := sha256.Sum256([]byte(group))
	return hex.EncodeToString(g[:12])
}

// lock takes the group's lock, waiting while another broker holds it. The
// returned function releases it, and is safe to call when err is not nil.
func (m *shareMembers) lock(ctx context.Context, group string) (unlock func(), err error) {
	key := lockKeyPrefix + groupHash(group)
	release := func(rev uint64) func() {
		return func() {
			rctx, cancel := context.WithTimeout(context.Background(), natsFlushTimeout)
			defer cancel()
			_ = m.kv.Delete(rctx, key, jetstream.LastRevision(rev))
		}
	}
	stamp := func() []byte { return []byte(strconv.FormatInt(time.Now().UnixNano(), 10)) }
	for delay := 2 * time.Millisecond; ; delay = min(delay*2, 100*time.Millisecond) {
		rev, err := m.kv.Create(ctx, key, stamp())
		if err == nil {
			return release(rev), nil
		}
		if !errors.Is(err, jetstream.ErrKeyExists) {
			return func() {}, err
		}
		if entry, gerr := m.kv.Get(ctx, key); gerr == nil {
			if at, perr := strconv.ParseInt(string(entry.Value()), 10, 64); perr != nil ||
				time.Since(time.Unix(0, at)) > shareLockStale {
				// Its holder died, or wrote what cannot be read: take it over, once
				// across the brokers that find it so.
				if rev, uerr := m.kv.Update(ctx, key, stamp(), entry.Revision()); uerr == nil {
					return release(rev), nil
				}
				continue
			}
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return func() {}, ctx.Err()
		}
	}
}

// The start of a group's consumer, kept beside it. The consumer's own metadata
// holds it (shared.go, metaStartSeq), but a broker of an earlier release that
// updates the consumer replaces the metadata and drops it. This copy is in the
// members bucket, where nothing else writes it, tagged with the consumer's
// creation time so that a copy left by a consumer since deleted is never taken
// for the new one's. A broker that joins the group puts it back into the
// metadata (sharedConsumer).

func startValue(created time.Time, start string) []byte {
	return []byte(strconv.FormatInt(created.UnixNano(), 10) + " " + start)
}

// putStart records start as the start of the consumer made at created.
func (m *shareMembers) putStart(ctx context.Context, group string, created time.Time, start string) {
	_, _ = m.kv.Put(ctx, startKeyPrefix+groupHash(group), startValue(created, start))
}

// start is the recorded start of the consumer made at created.
func (m *shareMembers) start(ctx context.Context, group string, created time.Time) (string, bool) {
	entry, err := m.kv.Get(ctx, startKeyPrefix+groupHash(group))
	if err != nil {
		return "", false
	}
	at, start, ok := strings.Cut(string(entry.Value()), " ")
	if !ok || at != strconv.FormatInt(created.UnixNano(), 10) {
		return "", false
	}
	if _, err := strconv.ParseUint(start, 10, 64); err != nil {
		return "", false
	}
	return start, true
}

// forgetStart drops the group's recorded start, with its consumer.
func (m *shareMembers) forgetStart(ctx context.Context, group string) {
	_ = m.kv.Delete(ctx, startKeyPrefix+groupHash(group))
}

// endGroup deletes the consumer of group if no session is a member, under the
// group's lock so that a joiner is either seen here or makes its consumer after.
// A lock that cannot be taken is not waited on for ever: the group then ends as
// it did before the lock, which can lose what a joiner racing it was owed.
func (q *offlineQueue) endGroup(ctx context.Context, group string) (deleted bool, err error) {
	unlock, lerr := q.members.lock(ctx, group)
	defer unlock()
	if lerr == nil {
		has, err := q.members.hasMember(ctx, group)
		if err != nil || has {
			return false, err
		}
		if fn := afterGroupLock.Load(); fn != nil {
			(*fn)()
		}
	}
	if err := q.stream.DeleteConsumer(ctx, group); err != nil && !errors.Is(err, jetstream.ErrConsumerNotFound) {
		return false, err
	}
	q.members.forgetStart(ctx, group)
	return true, nil
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
	_, err := m.kv.Put(ctx, memberKey(group, clientID), memberValue(instance, group, clientID))
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
		if written, _, _ := parseMemberValue(entry.Value()); written != instance {
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
	has, err := m.hasMember(ctx, group)
	return !has, err
}

// hasMember reports whether any session's entry is in the group.
func (m *shareMembers) hasMember(ctx context.Context, group string) (bool, error) {
	lister, err := m.kv.ListKeysFiltered(ctx, groupPrefix(group))
	if err != nil {
		return false, err
	}
	for range lister.Keys() {
		_ = lister.Stop()
		return true, nil
	}
	return false, nil
}

// sweepShareMembers removes the entries of sessions that have no record in the
// session store (persistent sessions), which the entry's lapse would take
// MaxSessionExpiry plus a minute to do: a session killed with its broker whose
// record the store's sweep has since reclaimed, or one whose record was removed
// by a discard that never reached the group. A group left with no member has its
// backlog deleted, as when the last session leaves.
//
// An entry is only removed when it has gone unrewritten for shareMemberSweepGrace
// (a live member is rewritten every refreshEvery) and the record is known to be
// absent; a failed read keeps it. The deletion is conditional on the revision
// read, so a session that subscribes again in between keeps its entry. Without
// persistent sessions there are no records and nothing is swept.
func (b *Broker) sweepShareMembers(ctx context.Context) error {
	q := b.queue.Load()
	if q == nil || q.members == nil || b.store == nil {
		return nil
	}
	m := q.members
	keys, err := m.kv.Keys(ctx)
	if errors.Is(err, jetstream.ErrNoKeysFound) {
		return nil
	}
	if err != nil {
		return err
	}
	touched := map[string]bool{}
	for _, key := range keys {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		entry, err := m.kv.Get(ctx, key)
		if err != nil {
			continue
		}
		_, group, clientID := parseMemberValue(entry.Value())
		if group == "" || time.Since(entry.Created()) < shareMemberSweepGrace {
			continue
		}
		if b.sessionMayHoldGroup(ctx, clientID, group) {
			continue
		}
		if err := m.kv.Delete(ctx, key, jetstream.LastRevision(entry.Revision())); err != nil {
			continue
		}
		b.logger.Debug("swept the membership of a session with no record", "client_id", clientID)
		touched[group] = true
	}
	for group := range touched {
		has, err := m.hasMember(ctx, group)
		if err != nil || has {
			continue
		}
		if _, err := q.endGroup(ctx, group); err != nil {
			b.logger.Warn("could not delete the backlog of an ended shared subscription", "group", group, "error", err)
		}
	}
	return nil
}

// sessionMayHoldGroup reports whether the stored session of clientID could be a
// member of group, and is true whenever that cannot be told: a record that
// cannot be read or decoded, and a record a broker is serving (Attached), which
// is the one place its subscriptions can be newer than the record (a SUBSCRIBE
// writes the entry before the record, and a failed record write is only
// logged). It is false for a missing record, one past its expiry, and one
// released (or left by a broker the store has not yet reclaimed) that does not
// list the group, such as a restore that failed and kept the record.
func (b *Broker) sessionMayHoldGroup(ctx context.Context, clientID, group string) bool {
	entry, err := b.store.kv.Get(ctx, sessionKey(clientID))
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return false
	}
	if err != nil {
		return true
	}
	rec, err := decodeRecord(entry.Value())
	if err != nil {
		return true
	}
	if !rec.resumable(time.Now()) {
		return false
	}
	if rec.Attached {
		return true
	}
	for _, sub := range rec.Subscriptions {
		if name, ok := b.groupName(sub.Filter); ok && name == group {
			return true
		}
	}
	return false
}

// groupName is the name of the consumer behind a shared subscription, which
// names the group across brokers. ok is false for a filter that is not shared.
func (b *Broker) groupName(filter string) (name string, ok bool) {
	share, _, err := topic.SplitShared(filter)
	q := b.queue.Load()
	if err != nil || share == "" || q == nil {
		return "", false
	}
	subject, err := b.codec.filterToSubject(filter)
	if err != nil {
		return "", false
	}
	trimmed, ok := topic.TrimPrefix(q.prefix, topic.Prefix(b.opts.SubjectPrefix, subject))
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
	q := b.queue.Load()
	if q == nil || q.members == nil {
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
	empty, err := q.members.leave(ctx, group, s.clientID, s.instance)
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
	if fn := beforeGroupDelete.Load(); fn != nil {
		(*fn)()
	}
	// The held messages of this session were handed back before this, so the
	// consumer deleted here takes them with it, as it does the undelivered ones.
	deleted, err := q.endGroup(ctx, group)
	if err != nil {
		b.logger.Warn("could not delete the backlog of an ended shared subscription",
			"filter", filter, "error", err)
		return
	}
	if deleted {
		b.logger.Debug("a shared subscription ended: no session is subscribed", "filter", filter)
	}
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
func (b *Broker) refreshMembersLoop(q *offlineQueue) {
	defer b.wg.Done()
	t := time.NewTicker(q.members.refreshEvery())
	defer t.Stop()
	for {
		select {
		case <-b.shutdown:
			return
		case <-t.C:
			b.refreshMembers(q)
		}
	}
}

// beforeGroupDelete, when set, runs between a leaving session finding the group
// empty and deleting its consumer, which is where a joiner can arrive. A test
// uses it to hold the leaver there.
var beforeGroupDelete atomic.Pointer[func()]

// afterGroupLock, when set, runs while a leaver holds the group's lock, having
// found the group empty and before it deletes the consumer. A test uses it to
// hold the leaver there while a joiner arrives.
var afterGroupLock atomic.Pointer[func()]

// beforeMemberRefreshWrite, when set, runs between a refresh reading a session's
// subscription and writing its entry. A test uses it to unsubscribe there.
var beforeMemberRefreshWrite atomic.Pointer[func()]

func (b *Broker) refreshMembers(q *offlineQueue) {
	b.mu.Lock()
	sessions := make([]*session, 0, len(b.sessions))
	for _, s := range b.sessions {
		sessions = append(sessions, s)
	}
	b.mu.Unlock()
	for _, s := range sessions {
		for _, sub := range s.subscriptions() {
			if sub.share == "" {
				continue
			}
			if sub.backlog() == nil {
				// Made while the queue was off, and missed when it came up
				// (a SUBSCRIBE that read the queue just before it was set).
				b.giveBacklog(q, s, sub)
				continue
			}
			group, ok := b.groupName(sub.filter)
			if !ok {
				continue
			}
			if fn := beforeMemberRefreshWrite.Load(); fn != nil {
				(*fn)()
			}
			ctx, cancel := context.WithTimeout(context.Background(), natsFlushTimeout)
			err := q.members.join(ctx, group, s.clientID, s.instance)
			cancel()
			if err != nil {
				b.logger.Debug("could not refresh a shared subscription's member", "filter", sub.filter, "error", err)
				continue
			}
			// The copy of the consumer's start beside it lapses like an entry does.
			if bl := sub.backlog(); bl != nil {
				if info := bl.cons.CachedInfo(); info != nil {
					if start, ok := info.Config.Metadata[metaStartSeq]; ok {
						sctx, scancel := context.WithTimeout(context.Background(), natsFlushTimeout)
						q.members.putStart(sctx, group, info.Created, start)
						scancel()
					}
				}
			}
			// The subscription was read before this write. If it was dropped in
			// between, leaving found no entry (or the old one) and this write
			// has put one back for a session that is not a member.
			if _, held := s.subscription(sub.filter); !held {
				b.leaveGroup(s, sub.filter)
			}
		}
	}
}
