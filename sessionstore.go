package natsmqtt5

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"

	"github.com/taumatix/natsmqtt5/packet"
)

// MaxPersistentClientIDLen caps the Client Identifier a broker with
// Options.PersistentSessions accepts. The identifier is encoded into a
// JetStream key-value key, and a key is part of a NATS subject, so it cannot be
// the 65535 bytes MQTT permits. A longer one is refused with 0x85 (Client
// Identifier not valid), which MQTT-5.0 §3.1.3.1 provides for: a server states
// the identifiers it accepts, and 128 bytes is more than five times the 23 that
// every MQTT server must accept.
const MaxPersistentClientIDLen = 128

const (
	// sessionRecordVersion is written into every record. A broker reading a
	// version it does not know refuses to resume the session rather than
	// resuming half of it.
	sessionRecordVersion = 1

	// sessionOpTimeout bounds a single key-value operation. Exceeding it means
	// JetStream is in trouble, not that it is briefly slow.
	sessionOpTimeout = 5 * time.Second

	// sessionSweepInterval is how often a broker looks for records no client
	// will come back for.
	sessionSweepInterval = 5 * time.Minute

	// ownerPingTimeout bounds the liveness check against the broker a record
	// says owns it. A NATS server that supports no-responders answers a request
	// to a subject nobody listens on immediately, so this is the fallback for
	// one that does not, and it only gates a deletion that is already overdue
	// by a whole MaxSessionExpiry.
	ownerPingTimeout = 2 * time.Second

	// claimAttempts bounds the compare-and-swap retry loop. Each attempt only
	// repeats because another broker wrote the same key first, so needing five
	// means a stampede on one Client Identifier rather than a transient fault.
	claimAttempts = 5
)

// storedSubscription is one entry of a session's subscription set as written to
// JetStream. The NATS subject and the share name are deliberately absent: both
// are derived from Filter, so storing them would let a record disagree with
// itself, and recomputing them means a broker configured with a different
// SubjectPrefix rebuilds the subscription where it now belongs.
type storedSubscription struct {
	Filter     string
	Opts       packet.Subscription
	GrantedQoS packet.QoS
	ID         int
}

// sessionRecord is the durable half of a session: what a broker that has never
// seen this client needs in order to serve it (MQTT-5.0 §4.1).
//
// What is absent is as deliberate as what is present. In-flight QoS 1 and QoS 2
// message state stays in the broker process, because resending it on reconnect
// needs the offline message queue that ROADMAP.md still lists as missing;
// persisting the identifiers without the messages would let the broker claim a
// redelivery it cannot make. The Will Message is absent for a different reason:
// it belongs to the network connection rather than to the session
// (MQTT-5.0 §3.1.2.5), and a broker reading a record cannot know whether the
// connection that set the Will has ended or whether its owner is simply busy —
// so publishing it from here would mean announcing live clients as dead.
type sessionRecord struct {
	Version  int
	ClientID string
	// Owner names the broker instance that last wrote this record — not the one
	// currently serving the session, which is Attached. It is the arbitration
	// token: a broker claims a session by compare-and-swapping its own instance
	// into this field, and the broker it displaced learns it has lost the
	// session by seeing a name that is not its own come past the watcher.
	//
	// It stays set when the session is released, so that a broker's own release
	// is distinguishable from another broker's claim. Clearing it would make
	// every clean disconnect look, to the broker that performed it, exactly
	// like being displaced.
	Owner string
	// Attached is whether Owner is currently serving a connection for this
	// session.
	Attached bool
	Identity string
	Username string

	Subscriptions []storedSubscription

	ExpirySeconds uint32
	// ExpiresAt is when a released session stops being resumable. It is zero
	// while a broker is serving the session, because a session with a live
	// connection does not expire, and also for a released session whose Session
	// Expiry Interval says it never expires.
	ExpiresAt time.Time
}

// resumable reports whether a CONNECT with Clean Start 0 may take this record
// over.
//
// A record that is still attached is always resumable: either its broker is
// alive, in which case this is an ordinary takeover [MQTT-3.1.4-3], or it died
// without releasing the session. Expiry is measured from the moment a session
// is released, and a broker killed outright never records one — so a session
// whose broker vanished is resumed rather than silently dropped, and the sweep
// is what eventually reclaims one nobody comes back for.
func (r *sessionRecord) resumable(now time.Time) bool {
	if r.Attached || r.ExpiresAt.IsZero() {
		return true
	}
	return now.Before(r.ExpiresAt)
}

// sessionStore keeps session state in a JetStream key-value bucket so that a
// session outlives the broker process that created it and can move between
// brokers.
//
// Each broker instance has an identifier it writes into the records it owns,
// and watches the bucket for those records being claimed elsewhere. A claim is
// a compare-and-swap on the record's revision, so of two brokers racing for one
// Client Identifier exactly one wins; the loser's client is disconnected with
// 0x8E instead of being served in parallel.
type sessionStore struct {
	kv     jetstream.KeyValue
	nc     *nats.Conn
	logger *slog.Logger
	// maxSessionExpiry is how long a record owned by an unreachable broker is
	// kept before the sweep may reclaim it.
	maxSessionExpiry time.Duration
	subjectPrefix    string

	// owner identifies this broker instance for the lifetime of the process.
	owner string
	// ping is where this instance answers liveness checks, so another broker's
	// sweep can tell a crashed owner from a busy one.
	ping *nats.Subscription

	// onLost is called when a record this broker owns is claimed elsewhere.
	onLost func(clientID string)

	stop context.CancelFunc
}

// newSessionStore provisions the bucket and starts the watcher and the sweep.
// onLost is called, off the caller's goroutine, for every Client Identifier
// this broker stops owning.
func newSessionStore(ctx context.Context, js jetstream.JetStream, nc *nats.Conn, opts *resolved, logger *slog.Logger, onLost func(string)) (*sessionStore, error) {
	bucket := opts.StreamPrefix + "_sessions"
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      bucket,
		Description: "MQTT v5 session state held by natsmqtt5",
		Storage:     opts.SessionStorage,
		Replicas:    opts.SessionReplicas,
		// A session needs its current state and nothing else, and history costs
		// storage on every reconnect.
		History: 1,
	})
	if err != nil {
		return nil, fmt.Errorf("creating key-value bucket %s: %w", bucket, err)
	}

	owner := nuid.Next()
	s := &sessionStore{
		kv:               kv,
		nc:               nc,
		logger:           logger.With("session_bucket", bucket, "broker_instance", owner),
		maxSessionExpiry: opts.maxSessionExpiry,
		subjectPrefix:    opts.SubjectPrefix,
		owner:            owner,
		onLost:           onLost,
	}

	if s.ping, err = nc.Subscribe(brokerPingSubject(opts.SubjectPrefix, owner), replyEmpty); err != nil {
		return nil, fmt.Errorf("subscribing to the broker liveness subject: %w", err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	s.stop = cancel
	if err := s.watch(runCtx); err != nil {
		cancel()
		_ = s.ping.Unsubscribe()
		return nil, err
	}
	go s.sweepLoop(runCtx)
	return s, nil
}

func replyEmpty(m *nats.Msg) { _ = m.Respond(nil) }

// brokerPingSubject is where a broker instance answers liveness checks. It sits
// under the shared subject prefix, so it is reachable from every broker sharing
// the bucket and nowhere near MQTT traffic.
func brokerPingSubject(prefix, owner string) string {
	return prefix + ".$broker." + owner + ".ping"
}

// sessionKey encodes a Client Identifier into a key-value key. A Client
// Identifier is arbitrary UTF-8 and a key must match ^[-/_=.a-zA-Z0-9]+$, so
// the identifier is base64url encoded: that alphabet is a strict subset of what
// a key allows, and the encoding is total, which no escaping scheme over so
// small a legal alphabet would be. The record carries the identifier in clear
// text so an operator reading the bucket can still see whose session it is.
func sessionKey(clientID string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(clientID))
}

func clientIDFromKey(key string) (string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(key)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

// claim takes ownership of a Client Identifier's session, creating a record
// when there is none.
//
// It returns the record this broker now owns, the revision that ownership rests
// on, and whether the record was resumed rather than created — which is exactly
// the Session Present flag of the CONNACK [MQTT-3.2.2-3].
func (s *sessionStore) claim(ctx context.Context, clientID, identity, username string, cleanStart bool) (*sessionRecord, uint64, bool, error) {
	// A CONNECT waits on this, so it must fail rather than hang when JetStream
	// does not answer: a client that is refused reconnects, one that is left
	// waiting holds a connection open until its own patience runs out.
	ctx, cancel := context.WithTimeout(ctx, sessionOpTimeout)
	defer cancel()

	key := sessionKey(clientID)

	// Every arm of the loop either returns or has lost a compare-and-swap to
	// another broker, in which case everything it read is stale and the whole
	// decision has to be made again.
	var lastErr error
	for attempt := 0; attempt < claimAttempts; attempt++ {
		entry, err := s.kv.Get(ctx, key)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			rec, rev, err := s.createFresh(ctx, key, clientID, identity, username)
			if errors.Is(err, jetstream.ErrKeyExists) {
				lastErr = err
				continue
			}
			return rec, rev, false, err
		}
		if err != nil {
			return nil, 0, false, fmt.Errorf("reading the session record for %q: %w", clientID, err)
		}

		rec, decodeErr := decodeRecord(entry.Value())
		if decodeErr != nil {
			// A record this broker cannot read is not a reason to refuse the
			// client: a session that cannot be resumed is a session that starts
			// fresh, which is what the client would get from a broker that had
			// never stored one.
			s.logger.Warn("discarding an unreadable session record",
				"client_id", clientID, "error", decodeErr)
		}

		if decodeErr != nil || cleanStart || !rec.resumable(time.Now()) {
			// Deleting before recreating is what makes Clean Start mean clean:
			// overwriting would keep every field the new record happens not to
			// set [MQTT-3.1.2-4].
			if err := s.kv.Delete(ctx, key, jetstream.LastRevision(entry.Revision())); err != nil {
				lastErr = err
				continue
			}
			rec, rev, err := s.createFresh(ctx, key, clientID, identity, username)
			if errors.Is(err, jetstream.ErrKeyExists) {
				lastErr = err
				continue
			}
			return rec, rev, false, err
		}

		rec.Owner = s.owner
		rec.Attached = true
		rec.Identity, rec.Username = identity, username
		rec.ExpiresAt = time.Time{}
		rev, err := s.kv.Update(ctx, key, encodeRecord(rec), entry.Revision())
		if err != nil {
			// Another broker claimed the same Client Identifier between the
			// read and the write. Only one of us may serve it, so read again
			// and find out whether we are still in the running.
			lastErr = err
			continue
		}
		return rec, rev, true, nil
	}
	return nil, 0, false, fmt.Errorf("could not claim the session for %q in %d attempts: %w",
		clientID, claimAttempts, lastErr)
}

func (s *sessionStore) createFresh(ctx context.Context, key, clientID, identity, username string) (*sessionRecord, uint64, error) {
	rec := &sessionRecord{
		Version:  sessionRecordVersion,
		ClientID: clientID,
		Owner:    s.owner,
		Attached: true,
		Identity: identity,
		Username: username,
	}
	rev, err := s.kv.Create(ctx, key, encodeRecord(rec))
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyExists) {
			return nil, 0, err
		}
		return nil, 0, fmt.Errorf("creating the session record for %q: %w", clientID, err)
	}
	return rec, rev, nil
}

// errLostSession marks a write that lost its compare-and-swap, which means
// another broker has taken the Client Identifier over and this one must stop
// serving it.
var errLostSession = errors.New("natsmqtt5: the session was claimed by another broker")

// save writes the session's current state, keeping ownership, and returns the
// revision the next write must present.
func (s *sessionStore) save(ctx context.Context, rec *sessionRecord, rev uint64) (uint64, error) {
	newRev, err := s.kv.Update(ctx, sessionKey(rec.ClientID), encodeRecord(rec), rev)
	if err != nil {
		return rev, fmt.Errorf("%w: %v", errLostSession, err)
	}
	return newRev, nil
}

// release hands the session back: it records when the session stops being
// resumable and clears the owner, so the next broker to read the record knows
// nobody is serving it. A session whose Session Expiry Interval is zero ends
// with its connection (MQTT-5.0 §3.1.2.11.2), so its record is deleted instead.
func (s *sessionStore) release(ctx context.Context, rec *sessionRecord, rev uint64) (uint64, error) {
	key := sessionKey(rec.ClientID)
	if rec.ExpirySeconds == 0 {
		if err := s.kv.Delete(ctx, key, jetstream.LastRevision(rev)); err != nil {
			return rev, fmt.Errorf("%w: %v", errLostSession, err)
		}
		// The key is gone, so there is no revision a later write could present.
		return rev, nil
	}

	rec.Attached = false
	rec.ExpiresAt = expiryDeadline(time.Now(), rec.ExpirySeconds)
	newRev, err := s.kv.Update(ctx, key, encodeRecord(rec), rev)
	if err != nil {
		return rev, fmt.Errorf("%w: %v", errLostSession, err)
	}
	return newRev, nil
}

// neverExpires is the Session Expiry Interval meaning the session is kept until
// the client says otherwise (MQTT-5.0 §3.1.2.11.2).
const neverExpires = 0xFFFFFFFF

// expiryDeadline turns a Session Expiry Interval into the instant the session
// stops being resumable. The zero time means it does not expire, which is why a
// record still held by a broker also carries the zero time.
func expiryDeadline(now time.Time, seconds uint32) time.Time {
	if seconds == neverExpires {
		return time.Time{}
	}
	return now.Add(time.Duration(seconds) * time.Second)
}

// watch turns another broker's claim into a disconnection here. Without it two
// brokers would both believe they serve one Client Identifier, and the client
// that moved would keep receiving on the connection it abandoned.
func (s *sessionStore) watch(ctx context.Context) error {
	// UpdatesOnly skips the replay of the whole bucket: a record that was
	// already there when this broker started cannot be one it has lost.
	w, err := s.kv.WatchAll(ctx, jetstream.UpdatesOnly())
	if err != nil {
		return fmt.Errorf("watching the session bucket: %w", err)
	}

	go func() {
		defer func() { _ = w.Stop() }()
		for {
			select {
			case <-ctx.Done():
				return
			case entry, ok := <-w.Updates():
				if !ok {
					if ctx.Err() == nil {
						// Without the watcher this broker can no longer be
						// told it has lost a session, so it would keep serving
						// a client another broker is also serving. Say so
						// loudly; there is no silent degradation here.
						s.logger.Error("the session watcher stopped; takeovers by other brokers will not be noticed")
					}
					return
				}
				if entry != nil {
					s.onUpdate(entry)
				}
			}
		}
	}()
	return nil
}

// onUpdate reacts to one bucket change. A record now owned by someone else — or
// deleted outright by a Clean Start elsewhere — is a session this broker has
// lost. Deciding that here costs nothing when the session is not ours: onLost
// looks it up and finds nothing.
func (s *sessionStore) onUpdate(entry jetstream.KeyValueEntry) {
	if entry.Operation() != jetstream.KeyValuePut {
		clientID, err := clientIDFromKey(entry.Key())
		if err == nil {
			s.onLost(clientID)
		}
		return
	}

	rec, err := decodeRecord(entry.Value())
	if err != nil || rec.Owner == s.owner {
		// Our own write looping back, or a record from a broker speaking a
		// version we do not know: neither says we have lost anything.
		return
	}
	s.onLost(rec.ClientID)
}

// sweepLoop deletes records no client will come back for. Without it a bucket
// accumulates one record per Client Identifier that ever connected, since a
// record is otherwise only removed by the client returning.
func (s *sessionStore) sweepLoop(ctx context.Context) {
	t := time.NewTicker(sessionSweepInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			// A sweep that overruns its own interval would stack up behind the
			// ticker, so it is bounded well inside it and simply picks up where
			// it left off next time.
			sweepCtx, cancel := context.WithTimeout(ctx, sessionSweepInterval/2)
			err := s.sweep(sweepCtx)
			cancel()
			if err != nil && ctx.Err() == nil {
				s.logger.Warn("the session sweep did not finish", "error", err)
			}
		}
	}
}

func (s *sessionStore) sweep(ctx context.Context) error {
	keys, err := s.kv.Keys(ctx)
	if errors.Is(err, jetstream.ErrNoKeysFound) {
		return nil
	}
	if err != nil {
		return err
	}

	for _, key := range keys {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		entry, err := s.kv.Get(ctx, key)
		if err != nil {
			continue
		}
		rec, err := decodeRecord(entry.Value())
		if err != nil {
			// An unreadable record can never be resumed, so it is only taking
			// up space.
			_ = s.kv.Delete(ctx, key, jetstream.LastRevision(entry.Revision()))
			continue
		}
		if !s.reclaimable(ctx, rec, entry) {
			continue
		}
		if err := s.kv.Delete(ctx, key, jetstream.LastRevision(entry.Revision())); err == nil {
			s.logger.Debug("swept a session record", "client_id", rec.ClientID)
		}
	}

	// Every delete leaves a marker behind. Purging the old ones keeps the
	// bucket's stream from growing with the churn of short-lived clients.
	return s.kv.PurgeDeletes(ctx, jetstream.DeleteMarkersOlderThan(sessionSweepInterval))
}

// reclaimable reports whether the sweep may delete a record.
//
// An expired record is straightforward. The awkward case is one still attached
// to a broker that was killed before it could release it: it has no expiry
// deadline, so nothing else will ever remove it. Two conditions must hold
// together before the sweep takes that decision — the record must be older than
// any Session Expiry Interval this broker would have granted, and its broker
// must not answer a liveness request. Requiring both means a single lost or
// slow reply cannot cost a live client its subscriptions.
func (s *sessionStore) reclaimable(ctx context.Context, rec *sessionRecord, entry jetstream.KeyValueEntry) bool {
	now := time.Now()
	switch {
	case !rec.resumable(now):
		return true
	case !rec.Attached:
		// Released, and its deadline has not passed — or it has none, which
		// means the client asked for a session that never expires and only the
		// client may end it.
		return false
	case rec.Owner == s.owner:
		// Attached here. This broker knows whether it is alive.
		return false
	case now.Sub(entry.Created()) < s.maxSessionExpiry:
		return false
	}
	return !s.ownerAlive(ctx, rec.Owner)
}

// ownerAlive reports whether the named broker instance answers.
func (s *sessionStore) ownerAlive(ctx context.Context, owner string) bool {
	ctx, cancel := context.WithTimeout(ctx, ownerPingTimeout)
	defer cancel()
	_, err := s.nc.RequestWithContext(ctx, brokerPingSubject(s.subjectPrefix, owner), nil)
	return err == nil
}

func (s *sessionStore) close() {
	if s.stop != nil {
		s.stop()
		s.stop = nil
	}
	if s.ping != nil {
		_ = s.ping.Unsubscribe()
		s.ping = nil
	}
}

func encodeRecord(r *sessionRecord) []byte {
	// A sessionRecord is plain data, so the only way Marshal fails is a bug
	// here rather than anything a client did.
	b, err := json.Marshal(r)
	if err != nil {
		panic("natsmqtt5: encoding a session record: " + err.Error())
	}
	return b
}

func decodeRecord(b []byte) (*sessionRecord, error) {
	var r sessionRecord
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, err
	}
	if r.Version != sessionRecordVersion {
		return nil, fmt.Errorf("session record version %d, this broker writes %d", r.Version, sessionRecordVersion)
	}
	return &r, nil
}
