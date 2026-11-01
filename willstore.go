package natsmqtt5

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
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

	// minProbeTimeout is the least a broker waits for its fence probe (reachable)
	// to be acknowledged, whatever WillCheckInterval is.
	minProbeTimeout = time.Second

	// willDeadRetryGap is the pause between the two liveness checks that must
	// both fail before a broker is taken for dead.
	willDeadRetryGap = 250 * time.Millisecond

	// defaultWillSweepAfter is how long after its due time a record that has ended is
	// kept before any broker deletes it unpublished. The delay is already inside the due
	// time, so no Will Delay Interval can outlast it; what it catches is a record nobody
	// is going to publish. See willStore.sweepStale.
	defaultWillSweepAfter = 24 * time.Hour
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

// willRef is what the index keeps of a record: who holds it, the revision it was seen
// at, and whether and when it is due. Everything else, the Will's payload above all,
// stays in the bucket and is read when a record is to be adopted, so the index costs
// the same for a 64 KiB Will as for a 7-byte one.
type willRef struct {
	key   string
	owner string
	rev   uint64
	ended bool
	dueAt time.Time
}

// willHeader is the part of a record the index decodes; json skips the rest.
type willHeader struct {
	Version int
	Owner   string
	Ended   bool
	DueAt   time.Time
}

func refOf(key string, rev uint64, b []byte) (willRef, error) {
	var h willHeader
	if err := json.Unmarshal(b, &h); err != nil {
		return willRef{}, err
	}
	if h.Version != willRecordVersion {
		return willRef{}, fmt.Errorf("will record version %d not understood", h.Version)
	}
	return willRef{key: key, owner: h.Owner, rev: rev, ended: h.Ended, dueAt: h.DueAt}, nil
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
	kv         jetstream.KeyValue
	nc         *nats.Conn
	logger     *slog.Logger
	pingBase   string
	checkEvery time.Duration

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
	// retryFor is how long fire keeps trying to remove a record before it publishes anyway
	// (only when this broker is not cut off): twice waitOut, the horizon by which a
	// survivor that could see the record has adopted it, plus one operation.
	retryFor time.Duration
	// cutOff is true from the moment this broker fences itself until it next reaches the
	// cluster.
	cutOff atomic.Bool
	// probeFault, when set, makes the fence probe's write fail with the error it returns (nil
	// means no fault). A seam for tests.
	probeFault atomic.Pointer[func() error]
	// silentSince is when each owner was first seen not answering. Only
	// adoptOrphans touches it.
	silentSince map[string]time.Time

	// index mirrors the bucket from a watcher, so a check reads memory instead
	// of listing the bucket and fetching every record. synced is false until the
	// watcher has delivered the initial contents and whenever it is down, and a
	// check then reads the bucket directly.
	indexMu sync.Mutex
	index   map[string]willRef
	synced  bool

	// sweepAfter is defaultWillSweepAfter, in nanoseconds so that a test can shorten it.
	sweepAfter atomic.Int64

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
		kv:          kv,
		nc:          nc,
		logger:      logger.With("will_bucket", bucket, "will_instance", owner),
		pingBase:    livenessBase(opts.SubjectPrefix, opts.QueuePlainPublishes),
		checkEvery:  every,
		owner:       owner,
		publish:     publish,
		fence:       fence,
		closing:     closing,
		silentSince: map[string]time.Time{},
		index:       map[string]willRef{},
	}
	w.sweepAfter.Store(int64(defaultWillSweepAfter))
	w.selfEvery = every / 2
	// Silence is counted from the start of the last successful check, which can be a whole
	// check interval before the NATS server failed. Fencing at twice the interval from that
	// point left a leader election (4 to 9 s after the leader goes quiet, measured up to 8.7 s)
	// only 7.5 s at the default, and a broker closed its clients for it now and then. One check
	// interval more makes the documented twice-the-interval true counted from the failure.
	w.fenceAfter = 2*every + w.selfEvery
	// The probe is a replicated write, which a loaded server can take a few hundred
	// milliseconds to acknowledge. Bounded by the interval alone, a short interval
	// (the tests' 150 ms) failed slow writes as unanswered and fenced a healthy broker.
	// waitOut grows with it, so a survivor still waits out the owner's slowest check.
	w.selfTimeout = min(ownerPingTimeout, max(every, minProbeTimeout))
	w.waitOut = w.fenceAfter + w.selfEvery + 2*w.selfTimeout
	w.retryFor = 2*w.waitOut + willOpTimeout
	if w.ping, err = nc.Subscribe(brokerPingSubject(livenessBase(opts.SubjectPrefix, opts.QueuePlainPublishes), owner), replyEmpty); err != nil {
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

// fire publishes the Will, by calling publish, once this broker has won the removal of
// its record with a compare-and-swap. Only the winner publishes, which is what makes the
// publication happen once however many brokers saw the record.
//
// A removal that fails for another reason than losing the race leaves it unknown whether
// a survivor has adopted the record, so publishing at once could announce the Will twice
// (a broker cut off from its cluster is exactly the one whose record a survivor adopts).
// The removal is retried until the store answers: a conflict means the adopter has it,
// success means this broker publishes. A broker that has fenced itself keeps trying until
// the store is back, since the survivors hold the Will meanwhile; one that has not, so the
// store alone is failing and nothing else will publish, gives up after retryFor and
// publishes, a Will announced twice being a lesser fault than one never announced.
func (w *willStore) fire(l *willLease, publish func()) {
	if w == nil || l == nil {
		publish()
		return
	}
	err := w.remove(l)
	if err == nil {
		publish()
		return
	}
	if isRevisionConflict(err) {
		return
	}
	w.logger.Warn("could not remove a Will Message record; retrying before publishing",
		"client_id", l.rec.ClientID, "error", err)
	go w.fireLater(l, publish)
}

// fireLater is fire's retry loop.
func (w *willStore) fireLater(l *willLease, publish func()) {
	giveUp := time.Now().Add(w.retryFor)
	t := time.NewTicker(w.selfEvery)
	defer t.Stop()
	for {
		select {
		case <-t.C:
		case <-w.closing:
			// The record stays, owned by a broker about to be gone, for a survivor to adopt.
			return
		}
		err := w.remove(l)
		switch {
		case err == nil:
			publish()
			return
		case isRevisionConflict(err):
			return
		}
		if time.Now().After(giveUp) && !w.cutOff.Load() {
			w.logger.Warn("could not remove a Will Message record; publishing anyway",
				"client_id", l.rec.ClientID, "error", err)
			publish()
			return
		}
	}
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
		w.fire(l, func() { w.publish(clientID, l.rec.Will) })
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
		w.index = map[string]willRef{}
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
	defer func() { _ = watcher.Stop() }() // a stop error on the way out has nothing to act on
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
				if ref, err := refOf(e.Key(), e.Revision(), e.Value()); err == nil {
					w.index[e.Key()] = ref
				} else {
					delete(w.index, e.Key())
				}
			}
			w.indexMu.Unlock()
		}
	}
}

// indexed returns the foreign records the watcher knows, or false when it is not in sync.
func (w *willStore) indexed() ([]willRef, bool) {
	w.indexMu.Lock()
	defer w.indexMu.Unlock()
	if !w.synced {
		return nil, false
	}
	out := make([]willRef, 0, len(w.index))
	for _, r := range w.index {
		if r.owner != w.owner {
			out = append(out, r)
		}
	}
	return out, true
}

// sweepStale deletes, unpublished, the records of Wills that ended and were due more than
// sweepAfter ago, whoever holds them. The bucket has no time-to-live: a record of an open
// connection can be years old and must stay, so the age that matters is how long a Will has
// been overdue, which is independent of its Will Delay Interval. A record that long overdue
// has outlived every path that publishes it (the broker that held it fired it or died, and a
// survivor adopts a dead broker's records within seconds), so what is left is one a fault
// leaked, and each is a record every broker indexes for ever. The delete is a
// compare-and-swap on the revision seen, so a record changed meanwhile is left alone.
func (w *willStore) sweepStale(ctx context.Context) {
	after := time.Duration(w.sweepAfter.Load())
	var stale []willRef
	w.indexMu.Lock()
	if w.synced {
		for _, r := range w.index {
			if r.ended && time.Since(r.dueAt) > after {
				stale = append(stale, r)
			}
		}
	}
	w.indexMu.Unlock()
	for _, r := range stale {
		dctx, cancel := context.WithTimeout(ctx, willOpTimeout)
		err := w.kv.Delete(dctx, r.key, jetstream.LastRevision(r.rev))
		cancel()
		switch {
		case err == nil:
			w.logger.Warn("deleted a Will Message record that was overdue and never published",
				"key", r.key, "due_at", r.dueAt, "owner", r.owner)
		case !isRevisionConflict(err):
			w.logger.Warn("could not delete an overdue Will Message record", "key", r.key, "error", err)
		}
	}
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
			w.sweepStale(ctx)
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
			w.cutOff.Store(false)
			continue
		}
		if time.Since(lastOK) >= w.fenceAfter && ctx.Err() == nil {
			w.logger.Warn("cut off from NATS: closing client connections so that their Will Messages can be published",
				"cut_off_for", time.Since(lastOK).Round(time.Millisecond))
			w.cutOff.Store(true)
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
	if _, err := w.nc.RequestWithContext(ctx, brokerPingSubject(w.pingBase, w.owner), nil); err != nil {
		return false
	}
	// A write, not a read. Every replica of the bucket answers a read, so a broker whose NATS
	// server was cut off from the rest of its cluster went on reading its own server's copy, never
	// fenced, and kept a client connected while a survivor published that client's Will
	// [MQTT-3.1.2-8]. A write is acknowledged only once a majority of the bucket's servers holds it,
	// which is the same majority a survivor needs to adopt the Will. The key has no dot, so it is
	// never a record's (willKeyPrefix), and its empty value is skipped by the index and the listing.
	if f := w.probeFault.Load(); f != nil && (*f)() != nil {
		return false
	}
	_, err := w.kv.Put(ctx, fenceProbeKey, nil)
	return err == nil
}

// fenceProbeKey is the key reachable writes. Every broker writes the same one, and the bucket
// keeps one revision of a key, so the probes take one message whatever the number of brokers.
const fenceProbeKey = "fence-probe"

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
		up, known := alive[l.owner]
		if !known {
			// One request, not two: silence has to last waitOut across checks
			// before anything is adopted, which is the confirmation.
			up = brokerAnswers(listCtx, w.nc, w.pingBase, l.owner)
			alive[l.owner] = up
		}
		seen[l.owner] = true
		if up {
			delete(w.silentSince, l.owner)
			continue
		}
		// Silent is not dead: an owner cut off from NATS still holds its clients
		// until it fences itself, which takes up to waitOut.
		first, ok := w.silentSince[l.owner]
		if !ok {
			first = time.Now()
			w.silentSince[l.owner] = first
		}
		if time.Since(first) >= w.waitOut {
			w.adopt(l)
		}
	}
}

// listForeign reads the bucket record by record: the path taken until the
// watcher is in sync.
func (w *willStore) listForeign(ctx context.Context) ([]willRef, error) {
	keys, err := w.kv.ListKeys(ctx)
	if err != nil {
		return nil, err
	}
	var out []willRef
	for key := range keys.Keys() {
		if l, ok := w.read(ctx, key); ok && l.rec.Owner != w.owner {
			out = append(out, willRef{key: key, owner: l.rec.Owner, rev: l.rev, ended: l.rec.Ended, dueAt: l.rec.DueAt})
		}
	}
	return out, nil
}

// adopt claims the record ref points at. The record is read here, not held in the index,
// and is taken only if it is still the owner's: a record another broker adopted, or its own
// broker settled, since the index saw it is not ours.
func (w *willStore) adopt(ref willRef) {
	rctx, rcancel := context.WithTimeout(context.Background(), willOpTimeout)
	l, ok := w.read(rctx, ref.key)
	rcancel()
	if !ok || l.rec.Owner != ref.owner {
		return
	}
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
		if isRevisionConflict(err) {
			// Another broker adopted it first, or the connection's own broker was
			// only briefly unreachable and has settled it: either way not ours.
			return
		}
		// Not a lost race: the store did not answer (a Raft election, a timeout). The update
		// may still have been applied, and then this broker owns a record nobody will publish,
		// since every broker skips its own records when it looks for orphans and the others see
		// an owner that answers. Look again, and take the record up if it is ours.
		w.logger.Warn("could not adopt a Will Message; checking whether the adoption was applied",
			"client_id", next.ClientID, "error", err)
		cctx, ccancel := context.WithTimeout(context.Background(), willOpTimeout)
		defer ccancel()
		cur, ok := w.read(cctx, l.key)
		if !ok || cur.rec.Owner != w.owner || !cur.rec.Adopted || cur.rev == l.rev {
			return
		}
		w.logger.Info("adopted the Will Message of a broker that is gone",
			"client_id", cur.rec.ClientID, "due_in", time.Until(cur.rec.DueAt).Round(time.Millisecond))
		go w.publishWhenDue(cur)
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
	w.fire(l, func() { w.publish(l.rec.ClientID, l.rec.Will) })
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
		if brokerAnswers(ctx, w.nc, w.pingBase, owner) {
			return true
		}
	}
	return false
}

// brokerAnswers sends one liveness request to a broker instance.
func brokerAnswers(ctx context.Context, nc *nats.Conn, base, owner string) bool {
	ctx, cancel := context.WithTimeout(ctx, ownerPingTimeout)
	defer cancel()
	_, err := nc.RequestWithContext(ctx, brokerPingSubject(base, owner), nil)
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
		return nil, fmt.Errorf("will record version %d not understood", r.Version)
	}
	return &r, nil
}
