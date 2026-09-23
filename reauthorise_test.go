package natsmqtt5_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// A session is keyed by its Client Identifier alone, so whoever the
// Authenticator lets use that identifier inherits the session and everything it
// is subscribed to. That is why a resumed session's filters are put back past
// the Authorizer rather than trusted.
//
// TestPersistentSessionReauthorisesRestoredSubscriptions covers the session
// rebuilt from a durable record. These cover the session this broker still
// holds in memory, where the NATS subscriptions were never torn down and so
// there is nothing to "restore" — the check has to reach the live set instead.

// narrowingAuthorizer denies ActionSubscribe on "secret/#" once deny is set. The
// flag is atomic because the broker reads it on its own goroutines while the
// test writes it.
func narrowingAuthorizer(deny *atomic.Bool) func(*natsmqtt5.Options) {
	return func(o *natsmqtt5.Options) {
		o.Authorizer = natsmqtt5.AuthorizerFunc(func(_ context.Context, req *natsmqtt5.AuthzRequest) error {
			if req.Action == natsmqtt5.ActionSubscribe && req.Topic == "secret/#" && deny.Load() {
				return errors.New("no longer permitted")
			}
			return nil
		})
	}
}

// A permission narrowed between two connections must take effect on the second
// one, not when the session eventually expires.
func TestInMemorySessionReauthorisesSubscriptionsOnResume(t *testing.T) {
	var deny atomic.Bool
	addr := startBroker(t, startNATS(t), narrowingAuthorizer(&deny))

	first, _ := connectClient(t, addr, durableConnect("narrowed-memory", 300))
	first.subscribe(
		paho.SubscribeOptions{Topic: "secret/#", QoS: 1},
		paho.SubscribeOptions{Topic: "public/#", QoS: 1},
	)
	require.NoError(t, first.Client.Disconnect(&paho.Disconnect{ReasonCode: 0}))

	// The client's permissions are narrowed while it is away.
	deny.Store(true)

	second, connack := connectClient(t, addr, durableConnect("narrowed-memory", 300))
	require.True(t, connack.SessionPresent,
		"the same broker still holds the session, so this is the in-memory branch")

	pub, _ := connectClient(t, addr, connectOpts("pub"))
	pub.publish(&paho.Publish{Topic: "secret/a", QoS: 1, Payload: []byte("classified")})
	second.expectNoMessage()

	// The filter that is still permitted has to survive: dropping the whole
	// subscription set would pass the assertion above for the wrong reason.
	pub.publish(&paho.Publish{Topic: "public/a", QoS: 1, Payload: []byte("fine")})
	assert.Equal(t, "fine", second.expectMessage().Payload)
}

// Losing every filter is still a resumption, not a discard: Session Present
// stays true, the QoS 2 receive state and the Packet Identifier space survive,
// and the client is left connected to re-subscribe.
func TestASessionCanBeResumedWithEveryFilterDenied(t *testing.T) {
	var deny atomic.Bool
	addr := startBroker(t, startNATS(t), narrowingAuthorizer(&deny))

	first, _ := connectClient(t, addr, durableConnect("all-denied", 300))
	first.subscribe(paho.SubscribeOptions{Topic: "secret/#", QoS: 1})
	require.NoError(t, first.Client.Disconnect(&paho.Disconnect{ReasonCode: 0}))

	deny.Store(true)

	second, connack := connectClient(t, addr, durableConnect("all-denied", 300))
	require.True(t, connack.SessionPresent, "a session with nothing left is still present")

	pub, _ := connectClient(t, addr, connectOpts("pub"))
	pub.publish(&paho.Publish{Topic: "secret/a", QoS: 1, Payload: []byte("classified")})
	second.expectNoMessage()
	second.expectNoServerDisconnect()
}

// A shared subscription is authorised under the whole filter, `$share` and
// ShareName included, which is what subscribeOne passes on a SUBSCRIBE. An
// integrator writing an Authorizer has to strip the prefix itself, and this is
// what says so.
func TestASharedSubscriptionIsReauthorisedUnderItsFullFilter(t *testing.T) {
	const shared = "$share/g/secret/#"

	var deny atomic.Bool
	authorize := func(o *natsmqtt5.Options) {
		o.Authorizer = natsmqtt5.AuthorizerFunc(func(_ context.Context, req *natsmqtt5.AuthzRequest) error {
			if req.Action == natsmqtt5.ActionSubscribe && req.Topic == shared && deny.Load() {
				return errors.New("no longer permitted")
			}
			return nil
		})
	}
	addr := startBroker(t, startNATS(t), authorize)

	first, _ := connectClient(t, addr, durableConnect("shared-memory", 300))
	first.subscribe(paho.SubscribeOptions{Topic: shared, QoS: 1})
	require.NoError(t, first.Client.Disconnect(&paho.Disconnect{ReasonCode: 0}))

	deny.Store(true)

	second, connack := connectClient(t, addr, durableConnect("shared-memory", 300))
	require.True(t, connack.SessionPresent)

	pub, _ := connectClient(t, addr, connectOpts("pub"))
	pub.publish(&paho.Publish{Topic: "secret/a", QoS: 1, Payload: []byte("classified")})
	second.expectNoMessage()
}

// Retransmission on resume hands the previous connection's undelivered payloads
// over at CONNACK time, before the client has sent anything. A payload on a
// filter this connection may no longer have must not be among them — and one on
// a filter it still has must.
func TestWithdrawnFilterTakesItsUnacknowledgedMessagesWithIt(t *testing.T) {
	var deny atomic.Bool
	addr := startBroker(t, startNATS(t), narrowingAuthorizer(&deny))

	raw := dialRaw(t, addr)
	require.False(t, raw.connect(rawConnect("withdrawn-q1", 300)).SessionPresent)
	raw.subscribe("secret/#", packet.QoS1)
	raw.subscribe("public/#", packet.QoS1)

	pub, _ := connectClient(t, addr, connectOpts("pub"))
	pub.publish(&paho.Publish{Topic: "secret/a", QoS: 1, Payload: []byte("classified")})
	pub.publish(&paho.Publish{Topic: "public/a", QoS: 1, Payload: []byte("fine")})

	// Received and deliberately left unacknowledged, in the order they were
	// sent — which is the order a resend has to use [MQTT-4.6.0-5], so a broker
	// that resent the withdrawn one would be caught by the first read below.
	classified := raw.expectPublish()
	require.Equal(t, "secret/a", classified.Topic)
	require.Equal(t, "public/a", raw.expectPublish().Topic)
	raw.drop()

	deny.Store(true)

	resumed := dialRaw(t, addr)
	require.True(t, resumed.connect(rawConnect("withdrawn-q1", 300)).SessionPresent)

	again := resumed.expectPublish()
	require.Equal(t, "public/a", again.Topic, "the permitted filter's arrears must still come back")
	assert.True(t, again.Dup)
	resumed.expectNothing()

	// The client flushes the acknowledgement it still owed from the previous
	// connection. The broker sent it that message, so answering with
	// 0x82 Protocol Error and disconnecting would punish the client for the
	// broker's own withdrawal.
	resumed.send(&packet.Puback{Ack: packet.Ack{PacketID: classified.PacketID}})
	resumed.expectNothing()
}

// The withdrawal is scoped to what a denied filter earned, not to what no
// surviving filter matches. handleUnsubscribe leaves the in-flight set alone,
// so a message earned by a filter the client has since unsubscribed from
// matches nothing live — and is still one the broker MUST resend
// [MQTT-4.4.0-1]. Denying some unrelated filter must not take it away.
func TestAnUnrelatedDenialLeavesTheArrearsOfAnUnsubscribedFilterAlone(t *testing.T) {
	var deny atomic.Bool
	addr := startBroker(t, startNATS(t), narrowingAuthorizer(&deny))

	raw := dialRaw(t, addr)
	require.False(t, raw.connect(rawConnect("orphaned", 300)).SessionPresent)
	raw.subscribe("secret/#", packet.QoS1)
	raw.subscribe("orphan/#", packet.QoS1)

	pub, _ := connectClient(t, addr, connectOpts("pub"))
	pub.publish(&paho.Publish{Topic: "orphan/a", QoS: 1, Payload: []byte("still owed")})
	owed := raw.expectPublish()
	require.Equal(t, "orphan/a", owed.Topic)

	// The client drops the filter that earned it. The message stays owed.
	raw.send(&packet.Unsubscribe{PacketID: 90, Filters: []string{"orphan/#"}})
	_, ok := raw.read().(*packet.Unsuback)
	require.True(t, ok, "an UNSUBSCRIBE must be answered with an UNSUBACK")
	raw.drop()

	deny.Store(true)

	resumed := dialRaw(t, addr)
	require.True(t, resumed.connect(rawConnect("orphaned", 300)).SessionPresent)

	again := resumed.expectPublish()
	assert.Equal(t, "orphan/a", again.Topic)
	assert.Equal(t, owed.PacketID, again.PacketID, "the original Packet Identifier [MQTT-4.4.0-1]")
	assert.True(t, again.Dup)
}

// The exception, and the reason the withdrawal is not simply "drop everything
// that no longer matches": past its PUBREC the client owns the message
// [MQTT-4.3.3-8], so what is outstanding is a PUBREL carrying no payload.
// Withholding it would leave the client waiting for a PUBCOMP forever and its
// Packet Identifier unusable, to prevent a disclosure that already happened.
func TestAWithdrawnFilterStillOwesItsPubrel(t *testing.T) {
	var deny atomic.Bool
	addr := startBroker(t, startNATS(t), narrowingAuthorizer(&deny))

	raw := dialRaw(t, addr)
	require.False(t, raw.connect(rawConnect("withdrawn-q2", 300)).SessionPresent)
	raw.subscribe("secret/#", packet.QoS2)

	pub, _ := connectClient(t, addr, connectOpts("pub"))
	pub.publish(&paho.Publish{Topic: "secret/a", QoS: 2, Payload: []byte("exactly once")})

	first := raw.expectPublish()
	require.Equal(t, packet.QoS2, first.QoS)
	raw.send(&packet.Pubrec{Ack: packet.Ack{PacketID: first.PacketID}})
	require.Equal(t, first.PacketID, raw.expectPubrel().PacketID)
	raw.drop()

	deny.Store(true)

	resumed := dialRaw(t, addr)
	require.True(t, resumed.connect(rawConnect("withdrawn-q2", 300)).SessionPresent)
	assert.Equal(t, first.PacketID, resumed.expectPubrel().PacketID)

	resumed.send(&packet.Pubcomp{Ack: packet.Ack{PacketID: first.PacketID}})
	resumed.expectNothing()
}

// A QoS 2 exchange the client never took ownership of is withdrawn like any
// other payload. What it leaves behind is a Packet Identifier the client may
// still acknowledge in two steps, and neither step may get it disconnected:
// the PUBREC is answered with 0x92 (MQTT-5.0 §3.6.2.1) and the PUBCOMP that
// closes the exchange with silence.
func TestAWithdrawnQoS2ExchangeIsClosedWithoutDisconnectingTheClient(t *testing.T) {
	var deny atomic.Bool
	addr := startBroker(t, startNATS(t), narrowingAuthorizer(&deny))

	raw := dialRaw(t, addr)
	require.False(t, raw.connect(rawConnect("withdrawn-q2rec", 300)).SessionPresent)
	raw.subscribe("secret/#", packet.QoS2)

	pub, _ := connectClient(t, addr, connectOpts("pub"))
	pub.publish(&paho.Publish{Topic: "secret/a", QoS: 2, Payload: []byte("classified")})

	// Dropped without a PUBREC, so the broker still owns the message and a
	// resend would be the PUBLISH itself.
	first := raw.expectPublish()
	require.Equal(t, packet.QoS2, first.QoS)
	raw.drop()

	deny.Store(true)

	resumed := dialRaw(t, addr)
	require.True(t, resumed.connect(rawConnect("withdrawn-q2rec", 300)).SessionPresent)
	resumed.expectNothing()

	resumed.send(&packet.Pubrec{Ack: packet.Ack{PacketID: first.PacketID}})
	rel := resumed.expectPubrel()
	require.Equal(t, first.PacketID, rel.PacketID)
	assert.Equal(t, packet.PacketIdentifierNotFound, rel.ReasonCode)

	resumed.send(&packet.Pubcomp{Ack: packet.Ack{PacketID: first.PacketID}})
	resumed.expectNothing()
}

// The window this feature would otherwise leave open: sess.attach points the
// session's NATS handlers at the resuming connection before reauthoriseLive
// runs, so a message on a still-live denied subscription is queued on the new
// connection while the Authorizer is being asked about it. deliverLoop would
// then write it out right after the CONNACK.
//
// The Authorizer is parked on a channel to make the window deterministic
// instead of racing for it.
func TestAMessageQueuedWhileTheAuthorizerRunsIsNotDelivered(t *testing.T) {
	var (
		parked  = make(chan struct{})
		release = make(chan struct{})
		gate    atomic.Bool
	)
	authorize := func(o *natsmqtt5.Options) {
		o.Authorizer = natsmqtt5.AuthorizerFunc(func(_ context.Context, req *natsmqtt5.AuthzRequest) error {
			if req.Topic != "secret/#" || !gate.Load() {
				return nil
			}
			close(parked)
			<-release
			return errors.New("no longer permitted")
		})
	}
	addr := startBroker(t, startNATS(t), authorize)

	first, _ := connectClient(t, addr, durableConnect("queued", 300))
	first.subscribe(paho.SubscribeOptions{Topic: "secret/#", QoS: 1})
	require.NoError(t, first.Client.Disconnect(&paho.Disconnect{ReasonCode: 0}))

	gate.Store(true)

	// The CONNACK cannot arrive until the Authorizer is released, so the
	// handshake runs on its own goroutine.
	type resumption struct {
		client *testClient
		ack    *paho.Connack
	}
	done := make(chan resumption, 1)
	go func() {
		c, ack := connectClient(t, addr, durableConnect("queued", 300))
		done <- resumption{c, ack}
	}()

	<-parked

	// attach has already happened, so this lands in the resuming connection's
	// delivery queue against a subscription that is about to be denied.
	pub, _ := connectClient(t, addr, connectOpts("pub"))
	pub.publish(&paho.Publish{Topic: "secret/a", QoS: 1, Payload: []byte("classified")})
	time.Sleep(200 * time.Millisecond)

	close(release)

	got := <-done
	require.True(t, got.ack.SessionPresent)
	got.client.expectNoMessage()
}

// With persistence on, a denial on the in-memory branch has to reach the
// durable record — reauthoriseLive runs and then resumeSubscriptions writes the
// reduced set back. Reverse those two and the record keeps the denied filter,
// which is a filter coming back the moment the session moves.
//
// The obvious test does not discriminate: disconnecting cleanly and restarting
// makes releaseStoredSession rewrite the record at close, so the ordering stops
// mattering. Only a second live broker claiming the session out from under the
// first one reads the record this connection left behind.
func TestADenialOnAnInMemoryResumeReachesTheDurableRecord(t *testing.T) {
	natsURL := startNATS(t)

	var deny atomic.Bool
	authorize := func(o *natsmqtt5.Options) {
		o.PersistentSessions = true
		narrowingAuthorizer(&deny)(o)
	}

	addrA := startBroker(t, natsURL, authorize)
	first, _ := connectClient(t, addrA, durableConnect("recorded", 300))
	first.subscribe(
		paho.SubscribeOptions{Topic: "secret/#", QoS: 1},
		paho.SubscribeOptions{Topic: "public/#", QoS: 1},
	)
	require.NoError(t, first.Client.Disconnect(&paho.Disconnect{ReasonCode: 0}))

	deny.Store(true)

	// Resumed from A's memory, so this is the branch under test. The connection
	// is deliberately left up: what is being checked is the record it wrote on
	// the way in, not the one it writes on the way out.
	_, connack := connectClient(t, addrA, durableConnect("recorded", 300))
	require.True(t, connack.SessionPresent)

	// The permission is restored, so anything B restores comes from the record
	// rather than from the Authorizer refusing it a second time.
	deny.Store(false)

	addrB := startBroker(t, natsURL, authorize)
	onB, connack := connectClient(t, addrB, durableConnect("recorded", 300))
	require.True(t, connack.SessionPresent, "B claims the record A wrote")

	pub, _ := connectClient(t, addrB, connectOpts("pub"))
	pub.publish(&paho.Publish{Topic: "secret/a", QoS: 1, Payload: []byte("classified")})
	onB.expectNoMessage()

	pub.publish(&paho.Publish{Topic: "public/a", QoS: 1, Payload: []byte("fine")})
	assert.Equal(t, "fine", onB.expectMessage().Payload)
}
