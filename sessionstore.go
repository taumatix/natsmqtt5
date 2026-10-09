package natsmqtt5

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
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
// What is absent is as deliberate as what is present. The payloads of in-flight
// QoS 1 and QoS 2 messages are not in it: the record keeps each one's Packet
// Identifier, QoS state and offline-queue sequence, and a restored session reads
// the payload back from the queue (restore.go). A message with no queue copy is
// not recorded, because an identifier without a message would promise a
// redelivery the broker cannot make. The Will Message is absent for a different reason:
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

	// AwayAt is when the session was released, so the broker that next claims
	// it can replay the offline queue from then (Options.OfflineQueue). It is
	// cleared by that claim: left in a record while a broker serves the
	// session, it would send a later claim back to an old disconnect, and the
	// client would get again what it had already received.
	AwayAt time.Time `json:",omitzero"`

	// AwayFromSeq is the lowest offline-queue sequence the released connection
	// had not delivered (conn.awayFloor), and Delivered the ids of messages at or
	// above it that it had. A claim replays from the sequence instead of from
	// AwayAt, which is later than what a client that was behind is owed, and
	// Delivered is what keeps that rewind from sending a QoS 2 message twice
	// [MQTT-4.3.3-2]. Both are cleared by the claim, as AwayAt is.
	//
	// Records written before these fields existed decode with them empty, and
	// the replay falls back to AwayAt exactly as it did. The record version
	// stays 1 for that reason: a broker that predates the fields ignores them.
	AwayFromSeq uint64            `json:",omitempty"`
	Delivered   []storedDelivered `json:",omitempty"`

	// DeliveredSince is the time Delivered reaches back to: it names every id
	// delivered since then (up to the bound on the list), whatever its
	// sequence. A claim whose record has it may rewind the replay to that time,
	// as a session held in memory does, and so finds a queued message whose
	// live copy never arrived [MQTT-4.4.0-1]. Cleared by the claim, and
	// additive like AwayFromSeq: a record without it is replayed as it was.
	DeliveredSince time.Time `json:",omitzero"`

	// AwayLateSeq is the lowest sequence of a message whose live copy reached
	// the released session after it was released: the copy was stored before the
	// connection ended and its delivery was delayed past that. A claim replays
	// from it as well [MQTT-4.4.0-1]. Cleared by the claim, and additive like
	// AwayFromSeq.
	AwayLateSeq uint64 `json:",omitempty"`

	// Inflight is the QoS 1 and QoS 2 messages sent to the client and not yet
	// acknowledged when the session was released, in the order they were sent,
	// and ReceivedQoS2 the Packet Identifiers of the client's QoS 2 PUBLISH
	// packets received and not yet released. A claim resends the first and
	// refuses to forward a resend of the second twice [MQTT-4.4.0-1],
	// [MQTT-4.3.3-10]. Both are cleared by the claim, as AwayAt is, so a record
	// read while a broker serves the session never describes a stale set.
	//
	// They are written only when the session is released, and so are bounded
	// there (fitRecord): a broker that is killed outright records neither.
	//
	// Like AwayFromSeq they are additive: the record version stays 1, and a
	// record without them restores an empty set, as it always did.
	Inflight     []storedInflight `json:",omitempty"`
	ReceivedQoS2 []uint16         `json:",omitempty"`

	// Withdrawn is the identifiers of messages the broker took back on resume
	// (a filter denied) and whose acknowledgement the client may still send. A
	// claim restores them so that acknowledgement is ignored rather than
	// answered with 0x82, and the identifier is not handed to a new message
	// first. Cleared by the claim, and additive like Inflight.
	Withdrawn []storedWithdrawn `json:",omitempty"`

	// Spill names payload-bucket values that hold the in-flight entries which
	// did not fit in the record, in order, after those in Inflight (spillRecord).
	// Each value is a JSON array of entries. Cleared by the claim, and additive
	// like Inflight: a broker that predates it ignores the key and restores only
	// the entries in Inflight.
	Spill []string `json:",omitempty"`

	// awayWas, awayFromSeqWas, deliveredWas, inflightWas and receivedQoS2Was are
	// what this broker's claim found and cleared, for the handshake to restore
	// from. They are never written.
	awayWas           time.Time
	awayFromSeqWas    uint64
	awayLateSeqWas    uint64
	deliveredSinceWas time.Time
	deliveredWas      []storedDelivered
	inflightWas       []storedInflight
	receivedQoS2Was   []uint16
	withdrawnWas      []storedWithdrawn
	spillWas          []string
}

// storedWithdrawn is one withdrawn identifier of the client's session: the
// acknowledgement it is owed, and Age, the connections attached since it was
// withdrawn, which is how the two-connections rule carries across a restart
// (session.forgetStaleWithdrawals).
type storedWithdrawn struct {
	ID   uint16
	Owed packet.Type
	Age  uint64 `json:",omitempty"`
}

// storedInflight is one unacknowledged message of the client's session. The
// payload is not kept: Seq names its copy in the offline queue stream.
type storedInflight struct {
	ID  uint16
	QoS packet.QoS
	// Rel is set once the PUBREL has gone out (QoS 2): the client owns the
	// message and what is owed is the PUBREL, which needs no payload and no Seq.
	Rel bool `json:",omitempty"`
	// Seq is the stream sequence of the queue copy of the message, 0 with Rel.
	Seq uint64 `json:",omitempty"`
	// Retain and SubID are what the delivery to this client carried that the
	// queue copy does not say: the RETAIN flag as the subscription asked for it,
	// and the Subscription Identifier of the filter that earned the message.
	Retain bool `json:",omitempty"`
	SubID  int  `json:",omitempty"`
	// Ack is the acknowledgement subject of the shared-subscription backlog
	// message this entry stands for, so that the broker that restores the session
	// acknowledges, holds or hands back the very message the previous one was
	// holding.
	Ack string `json:",omitempty"`
	// Pub is the PUBLISH packet as it first went out, for a message that has no
	// copy in the offline queue (a retained message, one a plain NATS publisher
	// sent, any with the queue off): the payload cannot be read back, so it is
	// kept here when it is small (maxStoredPublish). At is when the broker took
	// the message in and Exp the Message Expiry Interval it arrived with, which
	// a resend counts down from.
	Pub []byte `json:",omitempty"`
	// Blob names the PUBLISH in the payload bucket (sessionStore.putBlob) when it
	// is too large for Pub: the record keeps the key and the payload stays out of
	// it, so the record remains within a value. At and Exp are as for Pub.
	Blob string `json:",omitempty"`
	// BlobAt is when the payload named by Blob was last written, so a broker that
	// restores the record does not write it again at once. A record without it
	// (from a release before it existed) is rewritten once.
	BlobAt time.Time `json:",omitempty"`
	At     time.Time `json:",omitempty"`
	Exp    *uint32   `json:",omitempty"`
}

// maxStoredPublish is the largest PUBLISH kept inside a session record for a
// message with no queue copy. The record is rewritten while the session moves,
// and a value holds about a megabyte, so a bigger one is kept in the payload
// bucket instead (storedInflight.Blob) rather than squeezing the rest of the
// record out.
const maxStoredPublish = 16 << 10

// storedDelivered is a delivered message's id and the queue sequence of its
// copy, as kept in the record by session.awayState.
type storedDelivered struct {
	ID  string
	Seq uint64
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
	kv jetstream.KeyValue
	// blobs holds the PUBLISH of an in-flight message that has no queue copy and
	// is too large for the record, under the digest of its encoding. Values are
	// immutable and shared by every session that holds the same message; they
	// age out on their own (blobTTL) and are refreshed while a session needs them.
	js           jetstream.JetStream
	blobMu       sync.Mutex
	blobs        jetstream.KeyValue
	blobStorage  jetstream.StorageType
	blobReplicas int
	blobName     string
	blobGrace    time.Duration // zero means blobGrace
	nc           *nats.Conn
	logger       *slog.Logger
	// maxSessionExpiry is how long a record owned by an unreachable broker is
	// kept before the sweep may reclaim it.
	maxSessionExpiry time.Duration
	subjectPrefix    string
	// queueMaxAge is how far back the offline queue reaches, which is how far a
	// session left attached by a dead broker has to be replayed from.
	queueMaxAge time.Duration

	// owner identifies this broker instance for the lifetime of the process.
	owner string
	// ping is where this instance answers liveness checks, so another broker's
	// sweep can tell a crashed owner from a busy one.
	ping *nats.Subscription

	// onLost is called when a record this broker owns is claimed elsewhere.
	onLost func(clientID string, revision uint64)

	stop context.CancelFunc
}

// newSessionStore provisions the bucket and starts the watcher and the sweep.
// onLost is called, off the caller's goroutine, for every Client Identifier
// this broker stops owning.
func newSessionStore(ctx context.Context, js jetstream.JetStream, nc *nats.Conn, opts *resolved, logger *slog.Logger, onLost func(string, uint64)) (*sessionStore, error) {
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
		js:               js,
		blobName:         opts.StreamPrefix + "_inflight",
		blobStorage:      opts.SessionStorage,
		blobReplicas:     opts.SessionReplicas,
		nc:               nc,
		logger:           logger.With("session_bucket", bucket, "broker_instance", owner),
		maxSessionExpiry: opts.maxSessionExpiry,
		subjectPrefix:    opts.SubjectPrefix,
		queueMaxAge:      opts.OfflineQueueMaxAge,
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
			s.deleteOwnerBlobs(ctx, clientID)
			rec, rev, err := s.createFresh(ctx, key, clientID, identity, username)
			if errors.Is(err, jetstream.ErrKeyExists) {
				lastErr = err
				continue
			}
			return rec, rev, false, err
		}

		previousOwner := rec.Owner
		// A record still Attached to another instance is either that broker's
		// live session, which this claim is taking over, or one it left when it
		// was killed. Only the second has a position worth replaying from, and
		// only the owner's silence tells them apart.
		attachedElsewhere := rec.Attached && previousOwner != s.owner
		ownerDead := attachedElsewhere && !s.ownerAlive(ctx, previousOwner)
		if attachedElsewhere && !ownerDead {
			rec.AwayAt, rec.AwayFromSeq, rec.AwayLateSeq, rec.Delivered, rec.DeliveredSince = time.Time{}, 0, 0, nil, time.Time{}
		}
		legacy := ownerDead && rec.AwayAt.IsZero() && s.queueMaxAge > 0
		rec.Owner = s.owner
		rec.Attached = true
		rec.Identity, rec.Username = identity, username
		rec.ExpiresAt = time.Time{}
		// What the last connection left is what this one resumes from, and it
		// stays in the record: a connection checkpoints its position as it
		// advances (conn.checkpointLoop), and until the first checkpoint a broker
		// killed outright leaves this one for the claim after it.
		rec.awayWas = rec.AwayAt
		rec.awayFromSeqWas = rec.AwayFromSeq
		rec.awayLateSeqWas = rec.AwayLateSeq
		rec.deliveredSinceWas = rec.DeliveredSince
		rec.deliveredWas = rec.Delivered
		if legacy {
			// Attached with no position at all: written by a broker that predates
			// the checkpoint, or killed before its first write. Anything in the
			// queue may be owed, so the replay starts at the oldest message the
			// queue still holds, and may repeat what the dead connection
			// delivered.
			rec.awayWas = time.Now().Add(-s.queueMaxAge)
		}
		if rec.AwayAt.IsZero() {
			// The position a connection with nothing to replay starts from: when it
			// attached.
			rec.AwayAt = time.Now()
		}
		if legacy {
			rec.AwayAt = rec.awayWas
		}
		rec.inflightWas, rec.Inflight = rec.Inflight, nil
		rec.receivedQoS2Was, rec.ReceivedQoS2 = rec.ReceivedQoS2, nil
		rec.withdrawnWas, rec.Withdrawn = rec.Withdrawn, nil
		rec.spillWas, rec.Spill = rec.Spill, nil
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
		// Nothing was published for this session before it existed, so a broker
		// killed straight away leaves a successor that replays from here.
		AwayAt: time.Now(),
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
	rec.AwayAt = time.Now()
	newRev, err := s.kv.Update(ctx, key, encodeRecord(rec), rev)
	if err != nil {
		return rev, fmt.Errorf("%w: %v", errLostSession, err)
	}
	return newRev, nil
}

// blobTTL is how long a stored payload outlives its last write. A session is
// resumable for at most maxSessionExpiry after it is released, and the payload
// is rewritten once it is older than a quarter of the TTL (blobStale), so one
// that a session still needs has at least maxSessionExpiry left when the
// session is released.
func blobTTL(maxSessionExpiry time.Duration) time.Duration { return 2 * maxSessionExpiry }

// blobStale reports whether a payload written at t is due to be written again.
// A time in the future (a write time another broker recorded, on a clock ahead of
// this one) counts as stale: trusting it would let a payload expire unnoticed.
func (s *sessionStore) blobStale(t time.Time) bool {
	age := time.Since(t)
	return t.IsZero() || age < 0 || age > blobTTL(s.maxSessionExpiry)/4
}

// blobBucket is the payload bucket, created on the first payload that needs it
// so that a broker which never sees one needs no more of JetStream than it ever
// did. Reading it never creates it.
func (s *sessionStore) blobBucket(ctx context.Context, create bool) (jetstream.KeyValue, error) {
	s.blobMu.Lock()
	defer s.blobMu.Unlock()
	if s.blobs != nil {
		return s.blobs, nil
	}
	var kv jetstream.KeyValue
	var err error
	if create {
		kv, err = s.js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
			Bucket:      s.blobName,
			Description: "Payloads of unacknowledged MQTT v5 messages too large for a session record, held by natsmqtt5",
			Storage:     s.blobStorage,
			Replicas:    s.blobReplicas,
			History:     1,
			TTL:         blobTTL(s.maxSessionExpiry),
		})
	} else {
		kv, err = s.js.KeyValue(ctx, s.blobName)
	}
	if err != nil {
		return nil, fmt.Errorf("opening key-value bucket %s: %w", s.blobName, err)
	}
	s.blobs = kv
	return kv, nil
}

// blobOwner is the prefix of the keys one client's payloads are kept under. A
// payload's digest includes its Packet Identifier, so two sessions that were sent
// the same message with the same identifier would otherwise share a key, and
// one finishing would delete the other's resend.
func blobOwner(clientID string) string {
	sum := sha256.Sum256([]byte(clientID))
	return hex.EncodeToString(sum[:16])
}

// blobDigest is the part of a key that is the digest of the payload: the whole
// of a key written before keys named an owner, the part after the dot since.
func blobDigest(key string) string {
	if i := strings.LastIndexByte(key, '.'); i >= 0 {
		return key[i+1:]
	}
	return key
}

// putBlob keeps raw in the payload bucket and returns its key: owner, a dot and
// the digest of raw. Writing the same bytes again refreshes their age.
func (s *sessionStore) putBlob(ctx context.Context, owner string, raw []byte) (string, error) {
	kv, err := s.blobBucket(ctx, true)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	key := owner + "." + hex.EncodeToString(sum[:])
	if _, err := kv.Put(ctx, key, raw); err != nil {
		return "", err
	}
	return key, nil
}

// deleteBlob removes a payload once nothing needs it. A key without an owner was
// written by a release that shared keys between sessions, so it is left to the
// bucket's TTL.
func (s *sessionStore) deleteBlob(ctx context.Context, key string) error {
	if !strings.Contains(key, ".") {
		return nil
	}
	kv, err := s.blobBucket(ctx, false)
	if err != nil {
		return err
	}
	return kv.Delete(ctx, key)
}

// deleteOwnerBlobs removes every payload kept for one client. It is for a session
// that is discarded (Clean Start, expiry, an unreadable record): nothing will
// ever restore it, so nothing should keep its payloads until the TTL. It is
// best effort, and a key written before keys named an owner is not found.
func (s *sessionStore) deleteOwnerBlobs(ctx context.Context, clientID string) {
	kv, err := s.blobBucket(ctx, false)
	if err != nil {
		// No bucket means no payload was ever too large for a record.
		return
	}
	lister, err := kv.ListKeysFiltered(ctx, blobOwner(clientID)+".*")
	if err != nil {
		s.logger.Debug("could not list the payloads of a discarded session", "client_id", clientID, "error", err)
		return
	}
	for key := range lister.Keys() {
		if err := kv.Delete(ctx, key); err != nil {
			s.logger.Debug("could not delete a payload of a discarded session",
				"client_id", clientID, "error", err)
		}
	}
}

// getBlob reads a payload back, verified against its key so that a truncated
// or foreign value is never resent as the message.
func (s *sessionStore) getBlob(ctx context.Context, key string) ([]byte, error) {
	kv, err := s.blobBucket(ctx, false)
	if err != nil {
		return nil, err
	}
	e, err := kv.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	raw := e.Value()
	if sum := sha256.Sum256(raw); hex.EncodeToString(sum[:]) != blobDigest(key) {
		return nil, fmt.Errorf("stored payload does not match its key %s", key)
	}
	return raw, nil
}

// valueLimit is the largest session record a release writes. A key-value value
// travels as one NATS message, so the server's max_payload (1 MiB unless
// configured) bounds it, and the room left over covers the headers a message
// carries and the record outgrowing the estimate by a little.
func (s *sessionStore) valueLimit() int {
	max := int(s.nc.MaxPayload())
	if max <= 0 {
		max = defaultMaxPayload
	}
	return max - max/8
}

// blobLimit is the largest PUBLISH the payload bucket holds. A payload is the
// whole value of a key-value write, which carries no headers, so unlike a
// session record it needs no room for the record to outgrow its estimate: a
// retained message can be as large as max_payload less a little, and this keeps
// up with it.
func (s *sessionStore) blobLimit() int {
	max := int(s.nc.MaxPayload())
	if max <= 0 {
		max = defaultMaxPayload
	}
	return max - blobHeadroom
}

// blobHeadroom is what blobLimit leaves under max_payload for anything a
// key-value write adds to its value.
const blobHeadroom = 512

// defaultMaxPayload is the NATS server's default max_payload, assumed when the
// server did not say.
const defaultMaxPayload = 1 << 20

// trimDelivered drops the oldest delivered ids until the ids take at most
// budget bytes of the record, so that they never crowd out the in-flight
// entries, which a client is owed [MQTT-4.4.0-1] and which the ids only help to
// deduplicate. It returns how many were dropped.
func trimDelivered(rec *sessionRecord, budget int) int {
	all := rec.Delivered
	if len(all) == 0 {
		return 0
	}
	rec.Delivered = nil
	base := len(encodeRecord(rec))
	rec.Delivered = all
	keep := len(all)
	for keep > 0 {
		rec.Delivered = all[len(all)-keep:]
		if len(encodeRecord(rec))-base <= budget {
			break
		}
		keep -= max(1, keep/8)
	}
	if keep <= 0 {
		rec.Delivered = nil
	} else {
		rec.Delivered = all[len(all)-keep:]
	}
	return len(all) - max(keep, 0)
}

// spillKeyLen is the length of every payload key: an owner, a dot and a digest.
const spillKeyLen = 32 + 1 + 64

// spillRecord makes rec fit limit bytes without losing in-flight entries it can
// keep elsewhere: the newest entries are written, in chunks that each fit a
// payload, to the payload bucket and named in rec.Spill. It returns how many
// entries it could not keep (the chunks beyond what the record can name, or
// that could not be written), and the first write error.
//
// A record that fits is left alone and its Spill cleared. What is still too
// large afterwards (received QoS 2 identifiers) is for fitRecord.
func (s *sessionStore) spillRecord(rec *sessionRecord, limit int) (lost int, err error) {
	rec.Spill = nil
	if n := trimDelivered(rec, limit/2); n > 0 {
		s.logger.Warn("delivered ids dropped to keep the session record within a value; a replay may repeat a QoS 2 message",
			"client", rec.ClientID, "dropped", n)
	}
	if len(encodeRecord(rec)) <= limit || len(rec.Inflight) == 0 {
		return 0, nil
	}
	all := rec.Inflight
	sizes := make([]int, len(all))
	for i, e := range all {
		raw, _ := json.Marshal(e)
		sizes[i] = len(raw) + 1
	}
	chunkMax := s.blobLimit()
	// pack divides all[from:] into spans that each fit one value.
	pack := func(from int) (spans [][2]int) {
		used := 2
		for i := from; i < len(all); i++ {
			if len(spans) == 0 || used+sizes[i] > chunkMax {
				spans = append(spans, [2]int{i, i})
				used = 2
			}
			used += sizes[i]
			spans[len(spans)-1][1] = i + 1
		}
		return spans
	}
	placeholder := strings.Repeat("x", spillKeyLen)
	fits := func(keep int, chunks int) bool {
		rec.Inflight, rec.Spill = all[:keep], make([]string, chunks)
		for i := range rec.Spill {
			rec.Spill[i] = placeholder
		}
		return len(encodeRecord(rec)) <= limit
	}

	keep := len(all)
	var spans [][2]int
	for keep > 0 {
		keep -= max(1, keep/8)
		spans = pack(keep)
		if fits(keep, len(spans)) {
			break
		}
	}
	if keep == 0 {
		// Even the chunks' names are too many; the last ones go.
		for len(spans) > 0 && !fits(0, len(spans)) {
			spans = spans[:len(spans)-1]
		}
	}
	rec.Inflight, rec.Spill = all[:keep], nil
	kept := keep
	for _, span := range spans {
		raw, _ := json.Marshal(all[span[0]:span[1]])
		ctx, cancel := context.WithTimeout(context.Background(), sessionOpTimeout)
		key, perr := s.putBlob(ctx, blobOwner(rec.ClientID), raw)
		cancel()
		if perr != nil {
			err = perr
			break
		}
		rec.Spill = append(rec.Spill, key)
		kept += span[1] - span[0]
	}
	return len(all) - kept, err
}

// loadSpill reads the entries named by keys back, in order. A value that cannot
// be read is counted and skipped; the entries of the others keep their order.
func (s *sessionStore) loadSpill(ctx context.Context, keys []string) (entries []storedInflight, unreadable int) {
	for _, key := range keys {
		raw, err := s.getBlob(ctx, key)
		var chunk []storedInflight
		if err == nil {
			err = json.Unmarshal(raw, &chunk)
		}
		if err != nil {
			unreadable++
			s.logger.Warn("spilled in-flight entries of a session could not be read", "key", key, "error", err)
			continue
		}
		entries = append(entries, chunk...)
	}
	return entries, unreadable
}

// fitRecord cuts rec until its encoding is within limit bytes, and returns how
// many in-flight entries and received QoS 2 identifiers it left out.
//
// Receive Maximum allows 65535 messages in flight, which can exceed a value, so
// something has to give. The newest in-flight entries go first, because the
// oldest are the ones a resend must put on the wire first [MQTT-4.6.0-5] and
// the client holds the identifiers it was sent; then received identifiers, whose
// loss lets a resent QoS 2 PUBLISH be forwarded a second time. Both are logged
// by the caller. A record that does not fit with neither is left as it is, and
// its release fails as an oversized record always did.
func fitRecord(rec *sessionRecord, limit int) (cutInflight, cutQoS2 int) {
	size := len(encodeRecord(rec))
	for size > limit && len(rec.Inflight) > 0 {
		keep := len(rec.Inflight) - max(1, len(rec.Inflight)/8)
		cutInflight += len(rec.Inflight) - keep
		rec.Inflight = rec.Inflight[:keep]
		size = len(encodeRecord(rec))
	}
	for size > limit && len(rec.ReceivedQoS2) > 0 {
		keep := len(rec.ReceivedQoS2) - max(1, len(rec.ReceivedQoS2)/8)
		cutQoS2 += len(rec.ReceivedQoS2) - keep
		rec.ReceivedQoS2 = rec.ReceivedQoS2[:keep]
		size = len(encodeRecord(rec))
	}
	return cutInflight, cutQoS2
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

// onUpdate reacts to one bucket change. A record now naming a different broker
// is a session this one has lost. Deciding that here costs nothing when the
// session is not ours: onLost looks it up and finds nothing.
//
// Deletions are ignored, and the reason is the whole point of this function. A
// key-value delete carries no value, so nothing in the event says which broker
// performed it — and this broker performs them itself, on every Clean Start,
// immediately before writing the fresh record. Treating a delete as a takeover
// means disconnecting, with 0x8E, the client that has this moment connected.
// Nothing is lost by ignoring them: a claim that takes a session away always
// finishes by writing a record that names its new owner, and it is that write
// which carries the signal.
func (s *sessionStore) onUpdate(entry jetstream.KeyValueEntry) {
	if entry.Operation() != jetstream.KeyValuePut {
		return
	}

	rec, err := decodeRecord(entry.Value())
	if err != nil || rec.Owner == s.owner {
		// Our own write looping back, or a record from a broker speaking a
		// version we do not know: neither says we have lost anything.
		return
	}
	s.onLost(rec.ClientID, entry.Revision())
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

	kept := newBlobClaims()
	complete := true
	for _, key := range keys {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		entry, err := s.kv.Get(ctx, key)
		if err != nil {
			if !errors.Is(err, jetstream.ErrKeyNotFound) {
				complete = false
			}
			continue
		}
		rec, err := decodeRecord(entry.Value())
		if err != nil {
			// An unreadable record can never be resumed, so it is only taking
			// up space. The key still names the client, which is the one thing
			// an operator needs in order to go looking for what wrote it.
			clientID, keyErr := clientIDFromKey(key)
			if keyErr != nil {
				clientID = key
			}
			s.logger.Warn("deleting an unreadable session record",
				"client_id", clientID, "error", err)
			_ = s.kv.Delete(ctx, key, jetstream.LastRevision(entry.Revision()))
			continue
		}
		if !s.reclaimable(ctx, rec, entry) {
			kept.add(rec)
			continue
		}
		if err := s.kv.Delete(ctx, key, jetstream.LastRevision(entry.Revision())); err == nil {
			s.deleteOwnerBlobs(ctx, rec.ClientID)
			s.logger.Debug("swept a session record", "client_id", rec.ClientID)
		} else {
			kept.add(rec)
		}
	}

	// Only a complete view of the records may condemn a payload: one that could
	// not be read might be the record that names it.
	if complete {
		if err := s.sweepBlobs(ctx, kept); err != nil && ctx.Err() == nil {
			s.logger.Debug("could not sweep the payload bucket", "error", err)
		}
	}

	// Every delete leaves a marker behind. Purging the old ones keeps the
	// bucket's stream from growing with the churn of short-lived clients.
	return s.kv.PurgeDeletes(ctx, jetstream.DeleteMarkersOlderThan(sessionSweepInterval))
}

// blobGrace is how old a payload must be before the sweep may call it an orphan.
// A payload is written before the record that names it, so a younger one may
// simply be waiting for that record.
const blobGrace = 10 * time.Minute

// blobClaims is what the surviving session records say they need from the
// payload bucket.
type blobClaims struct {
	owners map[string]bool
	keys   map[string]bool
}

func newBlobClaims() *blobClaims {
	return &blobClaims{owners: map[string]bool{}, keys: map[string]bool{}}
}

func (c *blobClaims) add(rec *sessionRecord) {
	c.owners[blobOwner(rec.ClientID)] = true
	for _, e := range rec.Inflight {
		if e.Blob != "" {
			c.keys[e.Blob] = true
		}
	}
}

// sweepBlobs deletes payloads no session record can still need: those under an
// owner that has no record (a session discarded before the payload was written,
// or whose cleanup was lost), and keys from before keys named an owner that no
// record names. Payloads of a live owner stay to the bucket's TTL, since the
// record cannot say which of them it has stopped using.
func (s *sessionStore) sweepBlobs(ctx context.Context, kept *blobClaims) error {
	kv, err := s.blobBucket(ctx, false)
	if err != nil {
		return nil // no payload was ever too large for a record
	}
	keys, err := kv.Keys(ctx)
	if errors.Is(err, jetstream.ErrNoKeysFound) {
		return nil
	}
	if err != nil {
		return err
	}
	grace := s.blobGrace
	if grace == 0 {
		grace = blobGrace
	}
	for _, key := range keys {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		owner, _, owned := strings.Cut(key, ".")
		if owned && kept.owners[owner] || !owned && kept.keys[key] {
			continue
		}
		entry, err := kv.Get(ctx, key)
		if err != nil || time.Since(entry.Created()) < grace {
			continue
		}
		if err := kv.Delete(ctx, key, jetstream.LastRevision(entry.Revision())); err != nil {
			s.logger.Debug("could not delete an orphaned payload", "error", err)
		}
	}
	return nil
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
	return brokerAnswers(ctx, s.nc, s.subjectPrefix, owner)
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
