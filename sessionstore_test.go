package natsmqtt5

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These are the store's own tests: the end-to-end suite in persistent_test.go
// proves a client sees its session come back, and these pin the decisions that
// a client cannot observe — how a Client Identifier becomes a key, which
// records the sweep is allowed to delete, and who wins when two brokers claim
// the same session at once.

func TestSessionKeyRoundTripsEveryClientIdentifier(t *testing.T) {
	for _, clientID := range []string{
		"plain",
		"with.dots",
		"with spaces",
		"with/slashes",
		"nats>wildcards*here",
		"café ☕ δοκιμή",
		"\x00\x01\xff",
		strings.Repeat("x", MaxPersistentClientIDLen),
	} {
		key := sessionKey(clientID)
		assert.True(t, keyLegalForNATS(key), "key %q for client %q is not a legal key-value key", key, clientID)

		back, err := clientIDFromKey(key)
		require.NoError(t, err)
		assert.Equal(t, clientID, back)
	}
}

// keyLegalForNATS mirrors the key rule nats.go enforces, so a change to the
// encoding that produced an unusable key would fail here rather than at a
// client's first reconnect.
func keyLegalForNATS(key string) bool {
	if key == "" {
		return false
	}
	for _, r := range key {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '-', r == '/', r == '_', r == '=', r == '.':
		default:
			return false
		}
	}
	return true
}

func TestRecordResumability(t *testing.T) {
	now := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)

	for name, tc := range map[string]struct {
		rec  sessionRecord
		want bool
	}{
		"attached to a broker": {
			rec:  sessionRecord{Owner: "broker-a", Attached: true},
			want: true,
		},
		"attached to a broker that died past the deadline it would have had": {
			rec:  sessionRecord{Owner: "broker-a", Attached: true, ExpiresAt: now.Add(-time.Hour)},
			want: true,
		},
		"released, deadline ahead": {
			rec:  sessionRecord{Owner: "broker-a", ExpiresAt: now.Add(time.Minute)},
			want: true,
		},
		"released, deadline passed": {
			rec:  sessionRecord{Owner: "broker-a", ExpiresAt: now.Add(-time.Second)},
			want: false,
		},
		"released, never expires": {
			rec:  sessionRecord{Owner: "broker-a", ExpirySeconds: neverExpires},
			want: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.rec.resumable(now))
		})
	}
}

func TestExpiryDeadline(t *testing.T) {
	now := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
	assert.Equal(t, now.Add(30*time.Second), expiryDeadline(now, 30))
	assert.True(t, expiryDeadline(now, neverExpires).IsZero(),
		"a session that never expires must carry no deadline")
}

// A record written by a broker speaking a version this one does not know is
// refused rather than half-read, which is what lets the format change later.
func TestDecodeRecordRefusesAnotherVersion(t *testing.T) {
	_, err := decodeRecord([]byte(`{"Version":99,"ClientID":"x"}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "version 99")

	_, err = decodeRecord([]byte(`not json`))
	assert.Error(t, err)
}

// When a second CONNECT takes a Client Identifier over on the same broker, the
// connection it displaced is still finishing on its own goroutine and still
// holds a revision — one that its successor's claim superseded rather than
// invalidated, so the compare-and-swap would succeed and hand back a session
// that is being served. The claim generation is what stops it.
func TestADisplacedConnectionCannotWriteItsSuccessorsClaim(t *testing.T) {
	s := newSession("x")

	displaced := s.bindRecord(&sessionRecord{ClientID: "x"}, 7)
	current := s.bindRecord(&sessionRecord{ClientID: "x"}, 9)

	rec, _ := s.snapshot(displaced)
	assert.Nil(t, rec, "the displaced connection must not be given a record to write")

	rec, rev := s.snapshot(current)
	require.NotNil(t, rec, "the connection holding the current claim must be")
	assert.Equal(t, uint64(9), rev)

	// The same rule applies to a write that was already in flight when the
	// successor claimed: committing it would install a superseded revision.
	s.commitRecord(displaced, rec, 8)
	_, rev = s.snapshot(current)
	assert.Equal(t, uint64(9), rev, "a late commit from a displaced connection must be ignored")
}

// fakeEntry is a key-value entry a test can hand straight to onUpdate, so the
// decision can be examined without racing a real bucket to produce the event.
type fakeEntry struct {
	key   string
	value []byte
	op    jetstream.KeyValueOp
}

func (e fakeEntry) Bucket() string                  { return "test" }
func (e fakeEntry) Key() string                     { return e.key }
func (e fakeEntry) Value() []byte                   { return e.value }
func (e fakeEntry) Revision() uint64                { return 1 }
func (e fakeEntry) Created() time.Time              { return time.Time{} }
func (e fakeEntry) Delta() uint64                   { return 0 }
func (e fakeEntry) Operation() jetstream.KeyValueOp { return e.op }

// What the watcher does with each kind of bucket event, which is the whole of
// this broker's answer to "have I lost this session?".
func TestOnUpdateDecidesWhoHasLostASession(t *testing.T) {
	const mine, theirs = "broker-me", "broker-them"

	for name, tc := range map[string]struct {
		entry fakeEntry
		want  string // the Client Identifier reported lost, "" for none
	}{
		// A Clean Start deletes the record before writing a fresh one, and a
		// delete carries no value — nothing in the event says who did it. Acting
		// on it means disconnecting the client that has just connected. Nothing
		// is lost by ignoring deletes: a claim that takes a session away always
		// finishes with a Put naming its new owner.
		"a delete says nothing about who deleted it": {
			entry: fakeEntry{key: sessionKey("c"), op: jetstream.KeyValueDelete},
			want:  "",
		},
		"a purge says nothing either": {
			entry: fakeEntry{key: sessionKey("c"), op: jetstream.KeyValuePurge},
			want:  "",
		},
		"another broker's claim is a loss": {
			entry: fakeEntry{
				key:   sessionKey("c"),
				value: encodeRecord(&sessionRecord{Version: sessionRecordVersion, ClientID: "c", Owner: theirs}),
				op:    jetstream.KeyValuePut,
			},
			want: "c",
		},
		"our own write is not": {
			entry: fakeEntry{
				key:   sessionKey("c"),
				value: encodeRecord(&sessionRecord{Version: sessionRecordVersion, ClientID: "c", Owner: mine}),
				op:    jetstream.KeyValuePut,
			},
			want: "",
		},
		"a record we cannot read is not": {
			entry: fakeEntry{key: sessionKey("c"), value: []byte("{"), op: jetstream.KeyValuePut},
			want:  "",
		},
	} {
		t.Run(name, func(t *testing.T) {
			var lost []string
			s := &sessionStore{owner: mine, onLost: func(id string) { lost = append(lost, id) }}

			s.onUpdate(tc.entry)

			if tc.want == "" {
				assert.Empty(t, lost, "nothing should have been reported lost")
				return
			}
			assert.Equal(t, []string{tc.want}, lost)
		})
	}
}

// storeFixture is a session store against a real embedded NATS server.
func storeFixture(t *testing.T, customise ...func(*Options)) (*sessionStore, chan string) {
	t.Helper()

	srv, err := natsserver.NewServer(&natsserver.Options{
		Host: "127.0.0.1", Port: -1, JetStream: true,
		StoreDir: t.TempDir(), NoLog: true, NoSigs: true,
	})
	require.NoError(t, err)
	go srv.Start()
	require.True(t, srv.ReadyForConnections(10*time.Second))
	t.Cleanup(srv.Shutdown)

	return storeOn(t, srv.ClientURL(), customise...)
}

// storeOn builds another store against a NATS server that already exists, so a
// test can have two brokers race for one session.
func storeOn(t *testing.T, natsURL string, customise ...func(*Options)) (*sessionStore, chan string) {
	t.Helper()

	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	t.Cleanup(nc.Close)

	js, err := jetstream.New(nc)
	require.NoError(t, err)

	opts := Options{PersistentSessions: true}
	for _, fn := range customise {
		fn(&opts)
	}
	r, err := opts.resolve()
	require.NoError(t, err)

	lost := make(chan string, 16)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := newSessionStore(context.Background(), js, nc, r, logger, func(id string) { lost <- id })
	require.NoError(t, err)
	t.Cleanup(s.close)

	return s, lost
}

func TestClaimCreatesThenResumes(t *testing.T) {
	s, _ := storeFixture(t)
	ctx := context.Background()

	rec, rev, present, err := s.claim(ctx, "c1", "id", "user", false)
	require.NoError(t, err)
	assert.False(t, present, "there was no stored session to resume")
	assert.Equal(t, s.owner, rec.Owner)

	rec.Subscriptions = []storedSubscription{{Filter: "a/#", GrantedQoS: 1}}
	rev, err = s.save(ctx, rec, rev)
	require.NoError(t, err)

	rec.ExpirySeconds = 300
	_, err = s.release(ctx, rec, rev)
	require.NoError(t, err)

	back, _, present, err := s.claim(ctx, "c1", "id", "user", false)
	require.NoError(t, err)
	assert.True(t, present, "a released session inside its expiry must be resumable")
	require.Len(t, back.Subscriptions, 1)
	assert.Equal(t, "a/#", back.Subscriptions[0].Filter)
}

func TestCleanStartClaimDropsTheStoredSubscriptions(t *testing.T) {
	s, _ := storeFixture(t)
	ctx := context.Background()

	rec, rev, _, err := s.claim(ctx, "c2", "", "", false)
	require.NoError(t, err)
	rec.Subscriptions = []storedSubscription{{Filter: "a/#"}}
	rec.ExpirySeconds = 300
	rev, err = s.save(ctx, rec, rev)
	require.NoError(t, err)
	_, err = s.release(ctx, rec, rev)
	require.NoError(t, err)

	fresh, _, present, err := s.claim(ctx, "c2", "", "", true)
	require.NoError(t, err)
	assert.False(t, present)
	assert.Empty(t, fresh.Subscriptions)
}

// A Session Expiry Interval of zero ends the session with its connection
// (MQTT-5.0 §3.1.2.11.2), so releasing it must leave nothing behind.
func TestReleaseDeletesAZeroExpirySession(t *testing.T) {
	s, _ := storeFixture(t)
	ctx := context.Background()

	rec, rev, _, err := s.claim(ctx, "c3", "", "", false)
	require.NoError(t, err)
	_, err = s.release(ctx, rec, rev)
	require.NoError(t, err)

	_, err = s.kv.Get(ctx, sessionKey("c3"))
	assert.ErrorIs(t, err, jetstream.ErrKeyNotFound)
}

// The write that follows a lost claim must fail rather than overwrite the
// broker that won it — this is the whole point of holding a revision.
func TestSaveFailsOnceTheSessionHasBeenClaimedElsewhere(t *testing.T) {
	s, _ := storeFixture(t)
	ctx := context.Background()

	rec, rev, _, err := s.claim(ctx, "c4", "", "", false)
	require.NoError(t, err)

	// Another broker claims the same Client Identifier.
	other, _ := storeOn(t, s.nc.ConnectedUrl())
	_, _, _, err = other.claim(ctx, "c4", "", "", false)
	require.NoError(t, err)

	_, err = s.save(ctx, rec, rev)
	assert.ErrorIs(t, err, errLostSession)
}

// The broker that loses a session has to be told, because its client is still
// connected and must be disconnected with 0x8E.
func TestTheDisplacedBrokerIsNotified(t *testing.T) {
	s, lost := storeFixture(t)
	ctx := context.Background()

	_, _, _, err := s.claim(ctx, "c5", "", "", false)
	require.NoError(t, err)

	other, _ := storeOn(t, s.nc.ConnectedUrl())
	_, _, _, err = other.claim(ctx, "c5", "", "", false)
	require.NoError(t, err)

	select {
	case id := <-lost:
		assert.Equal(t, "c5", id)
	case <-time.After(5 * time.Second):
		t.Fatal("the displaced broker was never told it lost the session")
	}
}

func TestSweepDeletesExpiredRecordsAndKeepsLiveOnes(t *testing.T) {
	s, _ := storeFixture(t)
	ctx := context.Background()

	// Expired: released a second ago with a one-second interval.
	gone, rev, _, err := s.claim(ctx, "expired", "", "", false)
	require.NoError(t, err)
	gone.Attached = false
	gone.ExpirySeconds = 1
	gone.ExpiresAt = time.Now().Add(-time.Second)
	_, err = s.save(ctx, gone, rev)
	require.NoError(t, err)

	// Live: released with an hour to run.
	alive, rev, _, err := s.claim(ctx, "alive", "", "", false)
	require.NoError(t, err)
	alive.Attached = false
	alive.ExpirySeconds = 3600
	alive.ExpiresAt = time.Now().Add(time.Hour)
	_, err = s.save(ctx, alive, rev)
	require.NoError(t, err)

	require.NoError(t, s.sweep(ctx))

	_, err = s.kv.Get(ctx, sessionKey("expired"))
	assert.ErrorIs(t, err, jetstream.ErrKeyNotFound, "an expired record must be swept")
	_, err = s.kv.Get(ctx, sessionKey("alive"))
	assert.NoError(t, err, "a record still inside its expiry must survive the sweep")
}

// A broker killed outright leaves a record claiming it is still serving the
// session, and nothing else will ever remove it. The sweep reclaims one only
// when the record is older than any expiry this broker would grant and the
// owner does not answer.
func TestSweepReclaimsARecordFromAnUnreachableOwner(t *testing.T) {
	// MaxSessionExpiry of one nanosecond makes every record instantly old
	// enough, which leaves the owner's silence as the only thing under test.
	s, _ := storeFixture(t, func(o *Options) { o.MaxSessionExpiry = time.Nanosecond })
	ctx := context.Background()

	dead, rev, _, err := s.claim(ctx, "orphan", "", "", false)
	require.NoError(t, err)
	dead.Owner = "a-broker-that-is-not-running"
	_, err = s.save(ctx, dead, rev)
	require.NoError(t, err)

	// A second live store owns a record of its own; the sweep must leave it be.
	other, _ := storeOn(t, s.nc.ConnectedUrl())
	_, _, _, err = other.claim(ctx, "served", "", "", false)
	require.NoError(t, err)

	require.NoError(t, s.sweep(ctx))

	_, err = s.kv.Get(ctx, sessionKey("orphan"))
	assert.ErrorIs(t, err, jetstream.ErrKeyNotFound,
		"a record whose owner does not answer must be reclaimed")
	_, err = s.kv.Get(ctx, sessionKey("served"))
	assert.NoError(t, err, "a record whose owner is running must survive the sweep")
}
