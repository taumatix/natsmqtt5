package natsmqtt5

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"

	"github.com/taumatix/natsmqtt5/packet"
)

const (
	// willRecordVersion is written into every Will record. A record of a
	// version this broker does not know is left alone: it was written by a
	// broker that understands it better.
	willRecordVersion = 1

	// defaultWillCheckInterval is how often a broker looks for Will Messages
	// whose owner has gone; see Options.WillCheckInterval.
	defaultWillCheckInterval = 5 * time.Second

	// willOpTimeout bounds one key-value operation on the Will bucket.
	willOpTimeout = 5 * time.Second

	// willDeadRetryGap is the pause between the two liveness checks that must
	// both fail before a broker is taken for dead.
	willDeadRetryGap = 250 * time.Millisecond
)

// willRecord is a Will Message held in JetStream so that it outlives the broker
// serving the connection that set it.
//
// It exists for one purpose: a broker that is killed outright runs none of its
// disconnect handlers, so its clients' Wills would never be published. A record
// is written when a connection with a Will is established, and removed when the
// Will is published, cancelled or discarded. A record that outlives its broker
// is therefore evidence that the connection ended without cleanup.
//
// The Will's payload is stored in clear text, as the broker holds it in memory.
// Anyone who can read the bucket can read it, so it deserves the access control
// of the session bucket. It is a different thing from the session record on
// purpose: the Will belongs to the network connection (MQTT-5.0 §3.1.2.5), not
// the session, and a record that names its connection can be adopted by another
// broker without guessing which of several connections it speaks for.
type willRecord struct {
	Version  int
	ClientID string
	// Owner is the broker instance that holds the record: the one serving the
	// connection, or the broker that adopted it after that one died.
	Owner string
	Will  *packet.Will
	// Delay is the Will Delay Interval already reduced to the Session Expiry
	// Interval, since "the Server delays publishing the Client's Will Message
	// until the Will Delay Interval has passed or the Session ends, whichever
	// happens first" (MQTT-5.0 §3.1.3.2.2).
	Delay time.Duration
	// Ended is set once the connection is known to have ended, either by its
	// broker (a disconnect that left a delay to wait out) or by an adopter that
	// found the broker gone. Until then the record describes an open connection.
	Ended bool
	// DueAt is when a record with Ended set may be published.
	DueAt time.Time
	// Adopted is set when the record belongs to a broker that found the
	// connection's own broker gone, rather than to the one serving it. Its owner
	// is then not the connection's broker, so a CONNECT arriving at that owner
	// still has to settle it.
	Adopted bool
}

// willLease is one connection's claim on its Will record. It carries the
// revision the claim rests on: every change to the record is a compare-and-swap
// on it, so whoever loses a race finds out rather than overwriting.
type willLease struct {
	key string
	rec willRecord
	rev uint64
}

// willStore keeps the Will Messages of open connections in a JetStream
// key-value bucket and publishes those whose broker has died.
//
// Liveness reuses the per-broker ping subject the session store introduced
// (brokerPingSubject): a broker that is running answers it from its NATS
// client, independently of how busy its MQTT connections are, so an owner that
// stops answering is gone rather than slow. That is what makes a stored Will
// safe to act on — the failure the roadmap warned about was announcing a live
// client as dead.
type willStore struct {
	kv            jetstream.KeyValue
	nc            *nats.Conn
	logger        *slog.Logger
	subjectPrefix string
	checkEvery    time.Duration

	owner string
	ping  *nats.Subscription

	// fence closes every client connection of this broker. It is called when the
	// broker can no longer prove to the rest of the cluster that it is alive,
	// see selfLoop.
	fence func()
	// fenceAfter is how long this broker may go without a successful round trip
	// to NATS before it fences itself; selfEvery is how often it checks;
	// selfTimeout bounds one check. waitOut is how long a surviving broker must
	// have seen an owner silent before it takes the owner's Wills: fenceAfter
	// plus the longest the owner can take to notice (one check interval, and one
	// timed-out check, with slack for a second). The survivor starts counting
	// when a ping has already failed, which is no earlier than the owner's last
	// success, so a silent owner has closed its connections by then and the Will
	// of a client still connected is not published.
	fenceAfter, selfEvery, selfTimeout, waitOut time.Duration
	// silentSince is when each owner was first seen not answering. Only
	// adoptOrphans touches it.
	silentSince map[string]time.Time

	// index mirrors the bucket from a watcher, so a check reads memory instead
	// of listing the bucket and fetching every record. synced is false until the
	// watcher has delivered the initial contents and whenever it is down, and a
	// check then reads the bucket directly.
	indexMu sync.Mutex
	index   map[string]*willLease
	synced  bool

	// publish sends an adopted Will. The broker supplies it.
	publish func(clientID string, w *packet.Will)
	// closing closes when the broker shuts down, which abandons Wills waiting
	// out a delay: their record is left for another broker to adopt.
	closing <-chan struct{}

	stop context.CancelFunc
}

func newWillStore(ctx context.Context, js jetstream.JetStream, nc *nats.Conn, opts *resolved, logger *slog.Logger,
	publish func(string, *packet.Will), fence func(), closing <-chan struct{}) (*willStore, error) {
	bucket := opts.StreamPrefix + "_wills"
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      bucket,
		Description: "MQTT v5 Will Messages of open connections held by natsmqtt5",
		Storage:     opts.SessionStorage,
		Replicas:    opts.SessionReplicas,
		History:     1,
	})
	if err != nil {
		return nil, fmt.Errorf("creating key-value bucket %s: %w", bucket, err)
	}

	owner := nuid.Next()
	every := opts.WillCheckInterval
	if every <= 0 {
		every = defaultWillCheckInterval
	}
	w := &willStore{
		kv:            kv,
		nc:            nc,
		logger:        logger.With("will_bucket", bucket, "will_instance", owner),
		subjectPrefix: opts.SubjectPrefix,
		checkEvery:    every,
		owner:         owner,
		publish:       publish,
		fence:         fence,
		closing:       closing,
		silentSince:   map[string]time.Time{},
		index:         map[string]*willLease{},
	}
	w.fenceAfter = 2 * every
	w.selfEvery = every / 2
	w.selfTimeout = min(ownerPingTimeout, every)
	w.waitOut = w.fenceAfter + w.selfEvery + 2*w.selfTimeout
	if w.ping, err = nc.Subscribe(brokerPingSubject(opts.SubjectPrefix, owner), replyEmpty); err != nil {
		return nil, fmt.Errorf("subscribing to the Will liveness subject: %w", err)
	}
	runCtx, cancel := context.WithCancel(context.Background())
	w.stop = cancel
	go w.watchLoop(runCtx)
	go w.checkLoop(runCtx)
	go w.selfLoop(runCtx)
	return w, nil
}

func (w *willStore) close() {
	if w.stop != nil {
		w.stop()
		w.stop = nil
	}
	if w.ping != nil {
		_ = w.ping.Unsubscribe()
		w.ping = nil
	}
}

// willKeyPrefix is the part of a key every record of one client shares. A key is
// the client's encoded identifier, a dot, and an identifier of the connection:
// keying by client alone would let a reconnecting client's record overwrite a
// predecessor's that another broker is about to publish.
func willKeyPrefix(clientID string) string { return sessionKey(clientID) + "." }

// register writes the Will of a connection that has just been established. It
// returns nil, after logging, when the record cannot be written: the Will is
// still held in memory and published if this broker survives, which is what it
// did before the bucket existed.
func (w *willStore) register(ctx context.Context, clientID string, will *packet.Will, delay time.Duration) *willLease {
	if w == nil {
		return nil
	}
	l := &willLease{
		key: willKeyPrefix(clientID) + nuid.Next(),
		rec: willRecord{Version: willRecordVersion, ClientID: clientID, Owner: w.owner, Will: will, Delay: delay},
	}
	ctx, cancel := context.WithTimeout(ctx, willOpTimeout)
	defer cancel()
	rev, err := w.kv.Create(ctx, l.key, encodeWillRecord(&l.rec))
	if err != nil {
		w.logger.Warn("could not store a Will Message; it is kept in memory only",
			"client_id", clientID, "error", err)
		return nil
	}
	l.rev = rev
	return l
}

// ended records that the connection is over and the Will is due at dueAt. It
// reports false when the record is no longer this broker's to change, in which
// case another broker owns the Will now and this one must not publish it.
func (w *willStore) ended(l *willLease, dueAt time.Time) bool {
	if w == nil || l == nil {
		return true
	}
	next := l.rec
	next.Ended, next.DueAt = true, dueAt
	ctx, cancel := context.WithTimeout(context.Background(), willOpTimeout)
	defer cancel()
	rev, err := w.kv.Update(ctx, l.key, encodeWillRecord(&next), l.rev)
	switch {
	case err == nil:
		l.rec, l.rev = next, rev
		return true
	case isRevisionConflict(err):
		return false
	}
	w.logger.Warn("could not mark a Will Message as waiting out its delay", "client_id", l.rec.ClientID, "error", err)
	return true
}

// fire reports whether the caller may publish the Will. It removes the record
// with a compare-and-swap, and only the broker that wins the removal publishes,
// which is what makes the publication happen once however many brokers saw the
// record. A store failure other than losing the race errs towards publishing:
// a Will announced twice is a lesser fault than one never announced.
func (w *willStore) fire(l *willLease) bool {
	if w == nil || l == nil {
		return true
	}
	err := w.remove(l)
	if err == nil {
		return true
	}
	if isRevisionConflict(err) {
		return false
	}
	w.logger.Warn("could not remove a Will Message record; publishing anyway",
		"client_id", l.rec.ClientID, "error", err)
	return true
}

// drop removes the record of a Will that will not be published.
func (w *willStore) drop(l *willLease) {
	if w == nil || l == nil {
		return
	}
	if err := w.remove(l); err != nil && !isRevisionConflict(err) {
		w.logger.Warn("could not remove a Will Message record", "client_id", l.rec.ClientID, "error", err)
	}
}

func (w *willStore) remove(l *willLease) error {
	ctx, cancel := context.WithTimeout(context.Background(), willOpTimeout)
	defer cancel()
	return w.kv.Delete(ctx, l.key, jetstream.LastRevision(l.rev))
}

// isRevisionConflict reports that a compare-and-swap lost: the record was
// changed or removed by someone else since the revision was read.
func isRevisionConflict(err error) bool {
	return errors.Is(err, jetstream.ErrKeyExists) || errors.Is(err, jetstream.ErrKeyNotFound)
}

// settle deals with the Wills a client's earlier connections left behind, as a
// new connection for the same Client Identifier is accepted.
//
// A record whose connection has ended, or whose broker has gone, is the old
// session's: if the new connection resumes the session the Will is cancelled
// [MQTT-3.1.3-9], and if it starts a new one the old session has ended, so the
// Will is published now. A record of a connection that is still open on a live
// broker is that broker's to handle, as a takeover always was.
func (w *willStore) settle(ctx context.Context, clientID string, resumed bool) {
	if w == nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 2*willOpTimeout)
	defer cancel()
	keys, err := w.kv.ListKeysFiltered(ctx, willKeyPrefix(clientID)+"*")
	if err != nil {
		w.logger.Warn("could not look for Will Messages left by an earlier connection",
			"client_id", clientID, "error", err)
		return
	}
	for key := range keys.Keys() {
		l, ok := w.read(ctx, key)
		if !ok || (l.rec.Owner == w.owner && !l.rec.Adopted) {
			continue
		}
		if !l.rec.Ended && w.ownerAlive(ctx, l.rec.Owner) {
			continue
		}
		if resumed {
			// A survivor may adopt the record between our read and our removal,
			// which changes its revision; read it again and remove that one, or
			// the adopter would publish a Will this resumption must cancel
			// [MQTT-3.1.3-9].
			for attempt := 0; attempt < 3; attempt++ {
				err := w.remove(l)
				if err == nil || !isRevisionConflict(err) {
					break
				}
				if l, ok = w.read(ctx, key); !ok {
					break
				}
			}
			w.logger.Debug("cancelled a Will Message: the session was resumed", "client_id", clientID)
			continue
		}
		if w.fire(l) {
			w.publish(clientID, l.rec.Will)
		}
	}
}

// watchLoop keeps index equal to the bucket. A watcher that ends while the
// store is running is replaced, and the checks read the bucket directly in the
// meantime.
func (w *willStore) watchLoop(ctx context.Context) {
	for ctx.Err() == nil {
		w.watchOnce(ctx)
		w.indexMu.Lock()
		w.synced = false
		w.index = map[string]*willLease{}
		w.indexMu.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-time.After(w.checkEvery):
		}
	}
}

func (w *willStore) watchOnce(ctx context.Context) {
	watcher, err := w.kv.WatchAll(ctx)
	if err != nil {
		w.logger.Warn("could not watch the Will bucket; checks read it directly", "error", err)
		return
	}
	defer watcher.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case e, ok := <-watcher.Updates():
			if !ok {
				return
			}
			w.indexMu.Lock()
			switch {
			case e == nil:
				w.synced = true
			case e.Operation() != jetstream.KeyValuePut:
				delete(w.index, e.Key())
			default:
				if rec, err := decodeWillRecord(e.Value()); err == nil {
					w.index[e.Key()] = &willLease{key: e.Key(), rec: *rec, rev: e.Revision()}
				} else {
					delete(w.index, e.Key())
				}
			}
			w.indexMu.Unlock()
		}
	}
}

// indexed returns a copy of the foreign records the watcher knows, or false when
// it is not in sync.
func (w *willStore) indexed() ([]*willLease, bool) {
	w.indexMu.Lock()
	defer w.indexMu.Unlock()
	if !w.synced {
		return nil, false
	}
	out := make([]*willLease, 0, len(w.index))
	for _, l := range w.index {
		if l.rec.Owner != w.owner {
			c := *l
			out = append(out, &c)
		}
	}
	return out, true
}

func (w *willStore) read(ctx context.Context, key string) (*willLease, bool) {
	entry, err := w.kv.Get(ctx, key)
	if err != nil {
		return nil, false
	}
	rec, err := decodeWillRecord(entry.Value())
	if err != nil {
		return nil, false
	}
	return &willLease{key: key, rec: *rec, rev: entry.Revision()}, true
}

// checkLoop adopts the Wills of brokers that have gone.
func (w *willStore) checkLoop(ctx context.Context) {
	t := time.NewTicker(w.checkEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			w.adoptOrphans(ctx)
		}
	}
}

// selfLoop makes this broker prove, every selfEvery, that it can still reach the
// rest of the cluster: its NATS connection is up, it gets an answer on its own
// liveness subject, and it can read the Will bucket. A broker that cannot do so
// for fenceAfter closes its client connections. A client whose broker is cut off
// from NATS is not served by anyone, so its connection is over, and closing it
// is what lets a surviving broker publish the Will without announcing a client
// that is still connected as dead [MQTT-3.1.2-8].
func (w *willStore) selfLoop(ctx context.Context) {
	lastOK := time.Now()
	t := time.NewTicker(w.selfEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		started := time.Now()
		if w.reachable(ctx) {
			lastOK = started
			continue
		}
		if time.Since(lastOK) >= w.fenceAfter && ctx.Err() == nil {
			w.logger.Warn("cut off from NATS: closing client connections so that their Will Messages can be published",
				"cut_off_for", time.Since(lastOK).Round(time.Millisecond))
			w.fence()
		}
	}
}

// reachable reports whether this broker can still talk to the cluster.
func (w *willStore) reachable(ctx context.Context) bool {
	if !w.nc.IsConnected() {
		return false
	}
	ctx, cancel := context.WithTimeout(ctx, w.selfTimeout)
	defer cancel()
	if _, err := w.nc.RequestWithContext(ctx, brokerPingSubject(w.subjectPrefix, w.owner), nil); err != nil {
		return false
	}
	_, err := w.kv.Get(ctx, "fence-probe")
	return err == nil || errors.Is(err, jetstream.ErrKeyNotFound)
}

// adoptOrphans claims every Will whose owner no longer answers a liveness
// request. The claim is a compare-and-swap that names this broker as owner, so
// of any number of brokers looking at the same record exactly one holds it; that
// one waits out what is left of the delay and publishes.
func (w *willStore) adoptOrphans(ctx context.Context) {
	listCtx, cancel := context.WithTimeout(ctx, 2*willOpTimeout)
	defer cancel()
	leases, ok := w.indexed()
	if !ok {
		var err error
		if leases, err = w.listForeign(listCtx); err != nil {
			w.logger.Warn("could not list Will Messages", "error", err)
			return
		}
	}
	alive := map[string]bool{}
	seen := map[string]bool{}
	defer func() {
		for owner := range w.silentSince {
			if !seen[owner] {
				delete(w.silentSince, owner)
			}
		}
	}()
	for _, l := range leases {
		up, known := alive[l.rec.Owner]
		if !known {
			// One request, not two: silence has to last waitOut across checks
			// before anything is adopted, which is the confirmation.
			up = brokerAnswers(listCtx, w.nc, w.subjectPrefix, l.rec.Owner)
			alive[l.rec.Owner] = up
		}
		seen[l.rec.Owner] = true
		if up {
			delete(w.silentSince, l.rec.Owner)
			continue
		}
		// Silent is not dead: an owner cut off from NATS still holds its clients
		// until it fences itself, which takes up to waitOut.
		first, ok := w.silentSince[l.rec.Owner]
		if !ok {
			first = time.Now()
			w.silentSince[l.rec.Owner] = first
		}
		if time.Since(first) >= w.waitOut {
			w.adopt(l)
		}
	}
}

// listForeign reads the bucket record by record: the path taken until the
// watcher is in sync.
func (w *willStore) listForeign(ctx context.Context) ([]*willLease, error) {
	keys, err := w.kv.ListKeys(ctx)
	if err != nil {
		return nil, err
	}
	var out []*willLease
	for key := range keys.Keys() {
		if l, ok := w.read(ctx, key); ok && l.rec.Owner != w.owner {
			out = append(out, l)
		}
	}
	return out, nil
}

func (w *willStore) adopt(l *willLease) {
	now := time.Now()
	next := l.rec
	next.Owner, next.Adopted = w.owner, true
	if !next.Ended {
		// The connection ended some time before anyone noticed. The delay is
		// counted from the moment it was noticed, which can only be later than
		// the truth, never earlier.
		next.Ended, next.DueAt = true, now.Add(next.Delay)
	}
	ctx, cancel := context.WithTimeout(context.Background(), willOpTimeout)
	defer cancel()
	rev, err := w.kv.Update(ctx, l.key, encodeWillRecord(&next), l.rev)
	if err != nil {
		// Another broker adopted it first, or the connection's own broker was
		// only briefly unreachable and has settled it: either way not ours.
		return
	}
	l.rec, l.rev = next, rev
	w.logger.Info("adopted the Will Message of a broker that is gone",
		"client_id", next.ClientID, "due_in", time.Until(next.DueAt).Round(time.Millisecond))
	go w.publishWhenDue(l)
}

func (w *willStore) publishWhenDue(l *willLease) {
	if d := time.Until(l.rec.DueAt); d > 0 {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-t.C:
		case <-w.closing:
			// Left in the bucket, owned by a broker that is about to be gone,
			// so a surviving broker adopts it in turn.
			return
		}
	}
	if w.fire(l) {
		w.publish(l.rec.ClientID, l.rec.Will)
	}
}

// ownerAlive reports whether the broker instance answers, asking twice before
// saying it does not. "No responders" is definitive, but a timeout is not, and
// a live client announced as dead is the failure this store must not cause.
func (w *willStore) ownerAlive(ctx context.Context, owner string) bool {
	for attempt := 0; attempt < 2; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(willDeadRetryGap):
			case <-ctx.Done():
				return true
			}
		}
		if brokerAnswers(ctx, w.nc, w.subjectPrefix, owner) {
			return true
		}
	}
	return false
}

// brokerAnswers sends one liveness request to a broker instance.
func brokerAnswers(ctx context.Context, nc *nats.Conn, prefix, owner string) bool {
	ctx, cancel := context.WithTimeout(ctx, ownerPingTimeout)
	defer cancel()
	_, err := nc.RequestWithContext(ctx, brokerPingSubject(prefix, owner), nil)
	return err == nil
}

func encodeWillRecord(r *willRecord) []byte {
	b, err := json.Marshal(r)
	if err != nil {
		panic("natsmqtt5: encoding a Will record: " + err.Error())
	}
	return b
}

func decodeWillRecord(b []byte) (*willRecord, error) {
	var r willRecord
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, err
	}
	if r.Version != willRecordVersion || r.Will == nil {
		return nil, fmt.Errorf("Will record version %d not understood", r.Version)
	}
	return &r, nil
}
