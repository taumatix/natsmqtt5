package natsmqtt5_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// Broker.Reauthorize re-runs the Authorizer over a session that is already
// connected. Before it, a permission revoked while a client stayed connected
// took effect at its next CONNECT, however long that was.

// revocable denies anything under secret/ and any Will on alerts/revoked once
// revoked is set.
func revocable(revoked *atomic.Bool) func(*natsmqtt5.Options) {
	return func(o *natsmqtt5.Options) {
		o.Authorizer = natsmqtt5.AuthorizerFunc(func(_ context.Context, req *natsmqtt5.AuthzRequest) error {
			if !revoked.Load() {
				return nil
			}
			if req.Action == natsmqtt5.ActionSubscribe && strings.HasPrefix(req.Topic, "secret/") {
				return errors.New("revoked")
			}
			if req.Will && req.Topic == "alerts/revoked" {
				return errors.New("revoked")
			}
			return nil
		})
	}
}

// The subscription stops delivering, including what was already queued behind
// a full receive quota, and the connection stays up and keeps its quota: the
// one message in flight on the revoked filter is withdrawn, and its PUBACK
// still returns the slot (Receive Maximum 1, so a lost slot stops everything).
func TestReauthorizeStopsALiveSubscriptionWithoutCostingTheConnection(t *testing.T) {
	var revoked atomic.Bool
	b, addr, _ := startBrokerHandle(t, startNATS(t), revocable(&revoked))

	r := dialRaw(t, addr)
	cp := rawConnect("reauth-live", 300)
	cp.Properties.ReceiveMaximum = natsmqtt5.Ptr(uint16(1))
	r.connect(cp)
	r.subscribe("secret/#", packet.QoS1)
	r.subscribe("open/#", packet.QoS1)

	pub, _ := connectClient(t, addr, connectOpts("pub-reauth-live"))
	for _, n := range []string{"1", "2", "3"} {
		pub.publish(&paho.Publish{Topic: "secret/" + n, QoS: 1, Payload: []byte(n)})
	}
	first := r.expectPublish()
	require.Equal(t, "secret/1", first.Topic)
	// secret/2 and secret/3 wait behind the quota.
	time.Sleep(200 * time.Millisecond)

	revoked.Store(true)
	require.NoError(t, b.Reauthorize(context.Background(), "reauth-live"))

	r.send(&packet.Puback{Ack: packet.Ack{PacketID: first.PacketID}})
	pub.publish(&paho.Publish{Topic: "open/x", QoS: 1, Payload: []byte("still here")})

	next := r.expectPublish()
	assert.Equal(t, "open/x", next.Topic, "a message queued for the revoked filter was delivered after Reauthorize")
	r.send(&packet.Puback{Ack: packet.Ack{PacketID: next.PacketID}})

	pub.publish(&paho.Publish{Topic: "secret/4", QoS: 1, Payload: []byte("4")})
	r.expectNothing()
}

// A message in flight on a revoked filter is withdrawn, so resuming the
// session does not resend it [MQTT-4.4.0-1 applies to what the session still
// owes, and it owes nothing on a filter it may no longer have].
func TestReauthorizeWithdrawsWhatTheRevokedFilterHadInFlight(t *testing.T) {
	var revoked atomic.Bool
	b, addr, _ := startBrokerHandle(t, startNATS(t), revocable(&revoked))

	r := dialRaw(t, addr)
	r.connect(rawConnect("reauth-inflight", 300))
	r.subscribe("secret/#", packet.QoS1)
	pub, _ := connectClient(t, addr, connectOpts("pub-reauth-inflight"))
	pub.publish(&paho.Publish{Topic: "secret/1", QoS: 1, Payload: []byte("1")})
	require.Equal(t, "secret/1", r.expectPublish().Topic)

	revoked.Store(true)
	require.NoError(t, b.Reauthorize(context.Background(), "reauth-inflight"))
	r.drop()

	resumed := dialRaw(t, addr)
	connack := resumed.connect(rawConnect("reauth-inflight", 300))
	require.True(t, connack.SessionPresent)
	resumed.expectNothing()
}

// A Will on a topic the principal has lost is discarded, and the connection is
// left alone: disconnecting it would publish the very Will being revoked.
func TestReauthorizeDiscardsARevokedWillAndKeepsTheConnection(t *testing.T) {
	var revoked atomic.Bool
	b, addr, _ := startBrokerHandle(t, startNATS(t), revocable(&revoked))
	watcher, _ := connectClient(t, addr, connectOpts("watcher-reauth-will"))
	watcher.subscribe(paho.SubscribeOptions{Topic: "alerts/#", QoS: 0})

	w := dialRaw(t, addr)
	cp := rawConnect("reauth-will", 0)
	cp.Will = &packet.Will{Topic: "alerts/revoked", Payload: []byte("gone")}
	w.connect(cp)

	revoked.Store(true)
	require.NoError(t, b.Reauthorize(context.Background(), "reauth-will"))

	// Still connected: a PINGREQ is answered.
	w.send(&packet.Pingreq{})
	_, ok := w.read().(*packet.Pingresp)
	require.True(t, ok, "Reauthorize closed a connection it should only have narrowed")

	w.drop()
	watcher.expectNoMessage()
}

// The Will of a client already gone, waiting out its Will Delay Interval, is
// the same Will and must be discardable the same way.
func TestReauthorizeDiscardsAWillWaitingOutItsDelay(t *testing.T) {
	var revoked atomic.Bool
	b, addr, _ := startBrokerHandle(t, startNATS(t), revocable(&revoked))
	watcher, _ := connectClient(t, addr, connectOpts("watcher-reauth-delay"))
	watcher.subscribe(paho.SubscribeOptions{Topic: "alerts/#", QoS: 0})

	for _, c := range []struct{ id, topic string }{
		{"reauth-delayed", "alerts/revoked"},
		{"reauth-delayed-kept", "alerts/kept"},
	} {
		w := dialRaw(t, addr)
		cp := rawConnect(c.id, 300)
		cp.Will = &packet.Will{
			Topic: c.topic, Payload: []byte(c.id),
			// Long enough that a stalled runner cannot fire the revoked Will
			// before Reauthorize runs; the kept one is waited for, within the
			// 5 s of expectMessage.
			Properties: &packet.Properties{WillDelayInterval: packet.Uint32(3)},
		}
		w.connect(cp)
		w.drop()
	}

	revoked.Store(true)
	require.NoError(t, b.Reauthorize(context.Background(), "reauth-delayed"))
	require.NoError(t, b.Reauthorize(context.Background(), "reauth-delayed-kept"))

	m := watcher.expectMessage()
	assert.Equal(t, "alerts/kept", m.Topic, "the Will that was still permitted must still fire")
	watcher.expectNoMessage()
}

func TestReauthorizeOfAnUnknownClientIsNotAnError(t *testing.T) {
	var revoked atomic.Bool
	b, _, _ := startBrokerHandle(t, startNATS(t), revocable(&revoked))
	assert.NoError(t, b.Reauthorize(context.Background(), "nobody"))
}
