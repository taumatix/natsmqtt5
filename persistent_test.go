package natsmqtt5_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// These tests are about the one thing a broker cannot fake: a session that
// outlives the process holding it. Each starts a real NATS server with
// JetStream, runs one or two real brokers against it, and drives the Eclipse
// Paho v5 client over a real socket — then kills a broker, or moves the client
// to another one, and asserts on what the client observes.

// persistent turns on session persistence for a test broker.
func persistent(o *natsmqtt5.Options) { o.PersistentSessions = true }

// durableConnect is a CONNECT that asks to keep its session for expiry seconds.
func durableConnect(clientID string, expiry uint32) *paho.Connect {
	return &paho.Connect{
		ClientID:   clientID,
		CleanStart: false,
		KeepAlive:  30,
		Properties: &paho.ConnectProperties{SessionExpiryInterval: natsmqtt5.Ptr(expiry)},
	}
}

// The point of the feature: a session state that survives the broker process
// it was created on. The client reconnects to a broker that has never seen it,
// is told Session Present, and receives on a subscription it never re-made.
func TestPersistentSessionSurvivesABrokerRestart(t *testing.T) {
	natsURL := startNATS(t)

	addrA, stopA := startStoppableBroker(t, natsURL, persistent)
	first, connack := connectClient(t, addrA, durableConnect("restart-me", 300))
	assert.False(t, connack.SessionPresent, "the first connection has no stored session")
	first.subscribe(paho.SubscribeOptions{Topic: "restart/#", QoS: 1})
	require.NoError(t, first.Client.Disconnect(&paho.Disconnect{ReasonCode: 0}))
	stopA()

	addrB := startBroker(t, natsURL, persistent)
	second, connack := connectClient(t, addrB, durableConnect("restart-me", 300))
	assert.True(t, connack.SessionPresent, "a stored session must be resumed on a different broker process")

	// No new SUBSCRIBE: the restored subscription must be live on the NATS
	// server before the CONNACK claimed the session was present.
	pub, _ := connectClient(t, addrB, connectOpts("pub"))
	pub.publish(&paho.Publish{Topic: "restart/a", QoS: 1, Payload: []byte("after")})

	got := second.expectMessage()
	assert.Equal(t, "restart/a", got.Topic)
	assert.Equal(t, "after", got.Payload)
}

// Two brokers are up at once and the client moves from one to the other. The
// connection it left behind must be told 0x8E, because two brokers serving one
// Client Identifier is the failure this feature has to rule out
// [MQTT-3.1.4-3].
func TestPersistentSessionMovesToAnotherLiveBroker(t *testing.T) {
	natsURL := startNATS(t)
	addrA := startBroker(t, natsURL, persistent)
	addrB := startBroker(t, natsURL, persistent)

	disconnected := make(chan *paho.Disconnect, 1)
	nc := dial(t, addrA)
	onA := paho.NewClient(paho.ClientConfig{
		Conn:               nc,
		ClientID:           "roamer",
		OnServerDisconnect: func(d *paho.Disconnect) { disconnected <- d },
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := onA.Connect(ctx, durableConnect("roamer", 300))
	require.NoError(t, err)

	_, err = onA.Subscribe(ctx, &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: "roam/#", QoS: 1}},
	})
	require.NoError(t, err)

	onB, connack := connectClient(t, addrB, durableConnect("roamer", 300))
	assert.True(t, connack.SessionPresent, "the session must move to the second broker")

	select {
	case d := <-disconnected:
		assert.Equal(t, byte(packet.SessionTakenOver), d.ReasonCode,
			"the broker that lost the session must disconnect its client with 0x8E")
	case <-time.After(5 * time.Second):
		t.Fatal("the connection on the first broker was never displaced")
	}

	pub, _ := connectClient(t, addrA, connectOpts("pub"))
	pub.publish(&paho.Publish{Topic: "roam/x", QoS: 1, Payload: []byte("moved")})
	assert.Equal(t, "moved", onB.expectMessage().Payload)
}

// Turning persistence on must not cost the behaviour that already worked: a
// client that disconnects cleanly and comes back to the same broker resumes a
// session whose NATS subscriptions were never torn down.
//
// The release that a clean disconnect writes goes past this broker's own bucket
// watcher, and a watcher that read its own write as another broker's claim
// would discard exactly the session it was keeping for the reconnect.
func TestPersistentSessionResumesOnTheSameBrokerWithoutARestart(t *testing.T) {
	addr := startBroker(t, startNATS(t), persistent)
	pub, _ := connectClient(t, addr, connectOpts("pub"))

	first, _ := connectClient(t, addr, durableConnect("stay", 300))
	first.subscribe(paho.SubscribeOptions{Topic: "stay/#", QoS: 1})
	require.NoError(t, first.Client.Disconnect(&paho.Disconnect{ReasonCode: 0}))

	second, connack := connectClient(t, addr, durableConnect("stay", 300))
	assert.True(t, connack.SessionPresent)

	pub.publish(&paho.Publish{Topic: "stay/a", QoS: 1, Payload: []byte("still subscribed")})
	assert.Equal(t, "still subscribed", second.expectMessage().Payload)
}

// A second CONNECT for the same Client Identifier on the same broker displaces
// the first [MQTT-3.1.4-3], and the displaced connection tears down on its own
// goroutine while the new one is already claiming the record. The record must
// end up describing the connection that won, not the one that was leaving:
// sessionstore_test.go pins the guard that makes that so, and this is the path
// it guards.
func TestPersistentSessionSurvivesATakeoverOnTheSameBroker(t *testing.T) {
	natsURL := startNATS(t)
	addrA, stopA := startStoppableBroker(t, natsURL, persistent)

	first, _ := connectClient(t, addrA, durableConnect("dup", 300))
	first.subscribe(paho.SubscribeOptions{Topic: "one/#", QoS: 1})

	second, connack := connectClient(t, addrA, durableConnect("dup", 300))
	require.True(t, connack.SessionPresent)
	// Subscribing after the takeover is the part that fails if the displaced
	// connection wrote the record last: this broker would be holding a
	// revision the store has moved past, and the filter would never be stored.
	second.subscribe(paho.SubscribeOptions{Topic: "two/#", QoS: 1})
	require.NoError(t, second.Client.Disconnect(&paho.Disconnect{ReasonCode: 0}))
	stopA()

	addrB := startBroker(t, natsURL, persistent)
	third, connack := connectClient(t, addrB, durableConnect("dup", 300))
	require.True(t, connack.SessionPresent)

	pub, _ := connectClient(t, addrB, connectOpts("pub"))
	pub.publish(&paho.Publish{Topic: "one/a", QoS: 1, Payload: []byte("before the takeover")})
	assert.Equal(t, "before the takeover", third.expectMessage().Payload)

	pub.publish(&paho.Publish{Topic: "two/a", QoS: 1, Payload: []byte("after the takeover")})
	assert.Equal(t, "after the takeover", third.expectMessage().Payload)
}

// Subscription Options are part of the session, not of the SUBSCRIBE that
// created it, so they have to survive the restart too — otherwise a resumed
// subscription would silently deliver under different rules than the client
// asked for.
func TestPersistentSessionRestoresSubscriptionOptions(t *testing.T) {
	natsURL := startNATS(t)

	addrA, stopA := startStoppableBroker(t, natsURL, persistent)
	first, _ := connectClient(t, addrA, durableConnect("opts", 300))
	_, err := first.Client.Subscribe(context.Background(), &paho.Subscribe{
		Properties:    &paho.SubscribeProperties{SubscriptionIdentifier: natsmqtt5.Ptr(77)},
		Subscriptions: []paho.SubscribeOptions{{Topic: "opts/#", QoS: 1, RetainAsPublished: true}},
	})
	require.NoError(t, err)
	require.NoError(t, first.Client.Disconnect(&paho.Disconnect{ReasonCode: 0}))
	stopA()

	addrB := startBroker(t, natsURL, persistent)
	second, connack := connectClient(t, addrB, durableConnect("opts", 300))
	require.True(t, connack.SessionPresent)

	pub, _ := connectClient(t, addrB, connectOpts("pub"))
	pub.publish(&paho.Publish{Topic: "opts/a", QoS: 1, Retain: true, Payload: []byte("kept")})

	got := second.expectMessage()
	assert.True(t, got.Retain, "Retain As Published must survive the restart")
	require.NotNil(t, got.Properties)
	require.NotNil(t, got.Properties.SubscriptionIdentifier)
	assert.Equal(t, 77, *got.Properties.SubscriptionIdentifier,
		"the Subscription Identifier must survive the restart")
}

// An UNSUBSCRIBE has to reach the stored record, or a restart would resurrect
// a subscription the client has already dropped.
func TestPersistentSessionForgetsAnUnsubscribedFilter(t *testing.T) {
	natsURL := startNATS(t)

	addrA, stopA := startStoppableBroker(t, natsURL, persistent)
	first, _ := connectClient(t, addrA, durableConnect("unsub", 300))
	first.subscribe(
		paho.SubscribeOptions{Topic: "keep/#", QoS: 1},
		paho.SubscribeOptions{Topic: "drop/#", QoS: 1},
	)
	_, err := first.Client.Unsubscribe(context.Background(), &paho.Unsubscribe{Topics: []string{"drop/#"}})
	require.NoError(t, err)
	require.NoError(t, first.Client.Disconnect(&paho.Disconnect{ReasonCode: 0}))
	stopA()

	addrB := startBroker(t, natsURL, persistent)
	second, connack := connectClient(t, addrB, durableConnect("unsub", 300))
	require.True(t, connack.SessionPresent)

	pub, _ := connectClient(t, addrB, connectOpts("pub"))
	pub.publish(&paho.Publish{Topic: "drop/a", QoS: 1, Payload: []byte("gone")})
	second.expectNoMessage()

	pub.publish(&paho.Publish{Topic: "keep/a", QoS: 1, Payload: []byte("still here")})
	assert.Equal(t, "still here", second.expectMessage().Payload)
}

// Clean Start discards the stored record as well as the in-memory session
// [MQTT-3.1.2-4], so a restart afterwards must not bring the old subscriptions
// back from JetStream.
func TestPersistentSessionIsDiscardedByCleanStart(t *testing.T) {
	natsURL := startNATS(t)

	addrA, stopA := startStoppableBroker(t, natsURL, persistent)
	first, _ := connectClient(t, addrA, durableConnect("wiped", 300))
	first.subscribe(paho.SubscribeOptions{Topic: "wiped/#", QoS: 1})
	require.NoError(t, first.Client.Disconnect(&paho.Disconnect{ReasonCode: 0}))

	second, connack := connectClient(t, addrA, connectOpts("wiped"))
	assert.False(t, connack.SessionPresent, "Clean Start must not resume")
	require.NoError(t, second.Client.Disconnect(&paho.Disconnect{ReasonCode: 0}))
	stopA()

	addrB := startBroker(t, natsURL, persistent)
	third, connack := connectClient(t, addrB, durableConnect("wiped", 300))
	assert.False(t, connack.SessionPresent, "the stored record must have been deleted by the Clean Start")

	pub, _ := connectClient(t, addrB, connectOpts("pub"))
	pub.publish(&paho.Publish{Topic: "wiped/a", QoS: 1, Payload: []byte("x")})
	third.expectNoMessage()
}

// A Clean Start deletes the stored record before writing a fresh one, and that
// deletion goes past this broker's own bucket watcher. A key-value delete
// carries no value, so nothing in the event says who did it — and a broker that
// treats every delete as "someone claimed your session" disconnects the client
// that has just this moment connected, with 0x8E, for doing nothing wrong.
//
// Nothing is lost by ignoring deletes: a claim that takes a session away always
// finishes by writing a record naming its new owner, and that write is what
// carries the signal.
func TestPersistentCleanStartDoesNotDisconnectTheNewConnection(t *testing.T) {
	addr := startBroker(t, startNATS(t), persistent)

	// Leave a stored record behind, so the Clean Start below has something to
	// delete.
	first, _ := connectClient(t, addr, durableConnect("wiper", 300))
	first.subscribe(paho.SubscribeOptions{Topic: "wiper/#", QoS: 1})
	require.NoError(t, first.Client.Disconnect(&paho.Disconnect{ReasonCode: 0}))

	fresh, connack := connectClient(t, addr, connectOpts("wiper"))
	require.False(t, connack.SessionPresent)
	fresh.subscribe(paho.SubscribeOptions{Topic: "wiper/#", QoS: 1})

	fresh.expectNoServerDisconnect()

	pub, _ := connectClient(t, addr, connectOpts("pub"))
	pub.publish(&paho.Publish{Topic: "wiper/a", QoS: 1, Payload: []byte("served")})
	assert.Equal(t, "served", fresh.expectMessage().Payload)
}

// Ignoring deletions must not cost the signal they appeared to carry. A Clean
// Start on another broker deletes the record and writes a fresh one, and it is
// that write — naming its new owner — which has to reach the broker still
// serving the client [MQTT-3.1.4-3].
func TestPersistentCleanStartOnAnotherBrokerDisplacesTheOldConnection(t *testing.T) {
	natsURL := startNATS(t)
	addrA := startBroker(t, natsURL, persistent)
	addrB := startBroker(t, natsURL, persistent)

	onA, _ := connectClient(t, addrA, durableConnect("evictee", 300))
	onA.subscribe(paho.SubscribeOptions{Topic: "evict/#", QoS: 1})

	_, connack := connectClient(t, addrB, connectOpts("evictee"))
	assert.False(t, connack.SessionPresent, "a Clean Start resumes nothing")

	assert.Equal(t, byte(packet.SessionTakenOver), onA.expectServerDisconnect(),
		"the broker that lost the Client Identifier must disconnect its client")
}

// A stored session past its Session Expiry Interval is gone, whichever broker
// the client comes back to (MQTT-5.0 §3.1.2.11.2).
func TestPersistentSessionExpires(t *testing.T) {
	natsURL := startNATS(t)

	addrA, stopA := startStoppableBroker(t, natsURL, persistent)
	first, _ := connectClient(t, addrA, durableConnect("brief", 1))
	first.subscribe(paho.SubscribeOptions{Topic: "brief/#", QoS: 1})
	require.NoError(t, first.Client.Disconnect(&paho.Disconnect{ReasonCode: 0}))
	stopA()

	time.Sleep(1500 * time.Millisecond)

	addrB := startBroker(t, natsURL, persistent)
	_, connack := connectClient(t, addrB, durableConnect("brief", 1))
	assert.False(t, connack.SessionPresent, "a session past its expiry must not be resumed")
}

// A stored session can be claimed by any connection the Authenticator lets use
// its Client Identifier, on any broker, for as long as the Session Expiry
// Interval runs. Restoring its filters unchecked would let a permission that
// has since been narrowed be outlived by the subscription it was meant to
// remove.
func TestPersistentSessionReauthorisesRestoredSubscriptions(t *testing.T) {
	natsURL := startNATS(t)

	allowSecret := true
	authorize := func(o *natsmqtt5.Options) {
		o.PersistentSessions = true
		o.Authorizer = natsmqtt5.AuthorizerFunc(func(_ context.Context, req *natsmqtt5.AuthzRequest) error {
			if req.Action == natsmqtt5.ActionSubscribe && req.Topic == "secret/#" && !allowSecret {
				return errors.New("no longer permitted")
			}
			return nil
		})
	}

	addrA, stopA := startStoppableBroker(t, natsURL, authorize)
	first, _ := connectClient(t, addrA, durableConnect("narrowed", 300))
	first.subscribe(
		paho.SubscribeOptions{Topic: "secret/#", QoS: 1},
		paho.SubscribeOptions{Topic: "public/#", QoS: 1},
	)
	require.NoError(t, first.Client.Disconnect(&paho.Disconnect{ReasonCode: 0}))
	stopA()

	// The client's permissions are narrowed while it is away.
	allowSecret = false

	addrB := startBroker(t, natsURL, authorize)
	second, connack := connectClient(t, addrB, durableConnect("narrowed", 300))
	require.True(t, connack.SessionPresent)

	pub, _ := connectClient(t, addrB, connectOpts("pub"))
	pub.publish(&paho.Publish{Topic: "secret/a", QoS: 1, Payload: []byte("classified")})
	second.expectNoMessage()

	pub.publish(&paho.Publish{Topic: "public/a", QoS: 1, Payload: []byte("fine")})
	assert.Equal(t, "fine", second.expectMessage().Payload)
}

// A Client Identifier is arbitrary UTF-8, and a JetStream key-value key is not,
// so the mapping between the two has to hold for the identifiers a NATS key
// would refuse verbatim.
func TestPersistentSessionWithAnAwkwardClientID(t *testing.T) {
	natsURL := startNATS(t)
	const clientID = "sensor 7/café.δ>*"

	addrA, stopA := startStoppableBroker(t, natsURL, persistent)
	first, _ := connectClient(t, addrA, durableConnect(clientID, 300))
	first.subscribe(paho.SubscribeOptions{Topic: "awkward/#", QoS: 1})
	require.NoError(t, first.Client.Disconnect(&paho.Disconnect{ReasonCode: 0}))
	stopA()

	addrB := startBroker(t, natsURL, persistent)
	second, connack := connectClient(t, addrB, durableConnect(clientID, 300))
	require.True(t, connack.SessionPresent)

	pub, _ := connectClient(t, addrB, connectOpts("pub"))
	pub.publish(&paho.Publish{Topic: "awkward/a", QoS: 1, Payload: []byte("ok")})
	assert.Equal(t, "ok", second.expectMessage().Payload)
}

// A stored session is keyed by its Client Identifier, and a JetStream key is
// part of a NATS subject, so the broker states a limit and refuses beyond it
// rather than accepting a client whose session it could never write
// (MQTT-5.0 §3.1.3.1).
func TestPersistentSessionsCapTheClientIdentifier(t *testing.T) {
	addr := startBroker(t, startNATS(t), persistent)

	atTheLimit := strings.Repeat("x", natsmqtt5.MaxPersistentClientIDLen)
	longest, connack := connectClient(t, addr, durableConnect(atTheLimit, 300))
	assert.False(t, connack.SessionPresent)
	// The subscription proves the identifier survived the trip to JetStream
	// rather than merely being accepted at the door.
	longest.subscribe(paho.SubscribeOptions{Topic: "limit/#", QoS: 1})

	pub, _ := connectClient(t, addr, connectOpts("pub"))
	pub.publish(&paho.Publish{Topic: "limit/a", QoS: 1, Payload: []byte("fits")})
	assert.Equal(t, "fits", longest.expectMessage().Payload)

	tooLong := strings.Repeat("x", natsmqtt5.MaxPersistentClientIDLen+1)
	_, connack, err := tryConnect(t, addr, durableConnect(tooLong, 300))
	require.Error(t, err, "an over-long Client Identifier must be refused")
	require.NotNil(t, connack)
	assert.Equal(t, byte(packet.ClientIdentifierNotValid), connack.ReasonCode)
}

// The limit only applies where it is needed: a broker that does not store
// sessions never builds a key out of the identifier.
func TestLongClientIdentifiersAreFineWithoutPersistence(t *testing.T) {
	addr := startBroker(t, startNATS(t))

	long := strings.Repeat("x", natsmqtt5.MaxPersistentClientIDLen+50)
	_, connack := connectClient(t, addr, connectOpts(long))
	assert.False(t, connack.SessionPresent)
}

// Persistence is opt-in, so a broker left at its defaults keeps the v0.1
// behaviour: the session lives in the process and dies with it.
func TestSessionsAreNotPersistedByDefault(t *testing.T) {
	natsURL := startNATS(t)

	addrA, stopA := startStoppableBroker(t, natsURL)
	first, _ := connectClient(t, addrA, durableConnect("ephemeral", 300))
	first.subscribe(paho.SubscribeOptions{Topic: "ephemeral/#", QoS: 1})
	require.NoError(t, first.Client.Disconnect(&paho.Disconnect{ReasonCode: 0}))
	stopA()

	addrB := startBroker(t, natsURL)
	_, connack := connectClient(t, addrB, durableConnect("ephemeral", 300))
	assert.False(t, connack.SessionPresent,
		"without PersistentSessions a session must not outlive the broker process")
}
