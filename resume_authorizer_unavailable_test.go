package natsmqtt5_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// A resume that cannot be checked is refused before anything is claimed: the
// stored filters survive the outage, the connection that holds the session is
// not displaced, and the policy service is asked once per filter.

// countingOutage fails "flaky/" filters while down is set, and counts the
// resume checks per filter.
func countingOutage(down *atomic.Bool, resumeChecks *sync.Map) func(*natsmqtt5.Options) {
	return func(o *natsmqtt5.Options) {
		o.Authorizer = natsmqtt5.AuthorizerFunc(func(_ context.Context, req *natsmqtt5.AuthzRequest) error {
			if req.Resume {
				n, _ := resumeChecks.LoadOrStore(req.Topic, new(atomic.Int32))
				n.(*atomic.Int32).Add(1)
			}
			if down.Load() && strings.HasPrefix(req.Topic, "flaky/") && req.Resume {
				return fmt.Errorf("policy store: %w", natsmqtt5.ErrAuthorizerUnavailable)
			}
			return nil
		})
	}
}

func resumeChecksOf(m *sync.Map, filter string) int32 {
	n, ok := m.Load(filter)
	if !ok {
		return 0
	}
	return n.(*atomic.Int32).Load()
}

func TestAResumeIsRefusedWhileTheAuthorizerIsUnavailableAndLosesNothing(t *testing.T) {
	var down atomic.Bool
	var checks sync.Map
	addr := startBroker(t, startNATS(t), countingOutage(&down, &checks))

	r := dialRaw(t, addr)
	r.connect(rawConnect("resume-unavail", 300))
	r.subscribe("flaky/a", packet.QoS0)
	r.subscribe("open/a", packet.QoS0)
	r.drop()

	down.Store(true)
	refused := dialRaw(t, addr)
	refused.send(rawConnect("resume-unavail", 300))
	ack, ok := refused.read().(*packet.Connack)
	require.True(t, ok)
	assert.Equal(t, packet.ImplementationSpecificError, ack.ReasonCode)
	assert.False(t, ack.SessionPresent)

	down.Store(false)
	again := dialRaw(t, addr)
	ack = again.connect(rawConnect("resume-unavail", 300))
	assert.True(t, ack.SessionPresent, "the refused attempt ended the session")

	pub, _ := connectClient(t, addr, connectOpts("pub-resume-unavail"))
	pub.publish(&paho.Publish{Topic: "flaky/a", QoS: 0, Payload: []byte("kept")})
	assert.Equal(t, "flaky/a", again.expectPublish().Topic, "the outage cost the session a filter")
	pub.publish(&paho.Publish{Topic: "open/a", QoS: 0, Payload: []byte("kept")})
	assert.Equal(t, "open/a", again.expectPublish().Topic)

	assert.EqualValues(t, 2, resumeChecksOf(&checks, "flaky/a"),
		"one refused attempt and one that resumed, and no second ask within an attempt")
}

func TestAResumeRefusedForAnOutageDisplacesNoOne(t *testing.T) {
	var down atomic.Bool
	var checks sync.Map
	addr := startBroker(t, startNATS(t), countingOutage(&down, &checks))

	holder := dialRaw(t, addr)
	holder.connect(rawConnect("resume-holder", 300))
	holder.subscribe("flaky/a", packet.QoS0)

	down.Store(true)
	second := dialRaw(t, addr)
	second.send(rawConnect("resume-holder", 300))
	ack, ok := second.read().(*packet.Connack)
	require.True(t, ok)
	assert.Equal(t, packet.ImplementationSpecificError, ack.ReasonCode)

	holder.send(&packet.Pingreq{})
	_, ok = holder.read().(*packet.Pingresp)
	require.True(t, ok, "a resume refused for an unavailable policy store displaced the live connection")

	pub, _ := connectClient(t, addr, connectOpts("pub-resume-holder"))
	pub.publish(&paho.Publish{Topic: "flaky/a", QoS: 0, Payload: []byte("still")})
	assert.Equal(t, "flaky/a", holder.expectPublish().Topic, "the holder lost its subscription")
}

// With the session in the store only, another broker is the one resuming: it
// must refuse too, and leave the record as the first broker wrote it.
func TestAStoredSessionIsNotReducedByAnOutageOnAnotherBroker(t *testing.T) {
	natsURL := startNATS(t)
	persistent := func(o *natsmqtt5.Options) { o.PersistentSessions = true }
	var down atomic.Bool
	var checks sync.Map

	addrA, stopA := startStoppableBroker(t, natsURL, persistent)
	first, _ := connectClient(t, addrA, durableConnect("resume-stored", 300))
	first.subscribe(
		paho.SubscribeOptions{Topic: "flaky/a", QoS: 0},
		paho.SubscribeOptions{Topic: "open/a", QoS: 0},
	)
	require.NoError(t, first.Client.Disconnect(&paho.Disconnect{ReasonCode: 0}))
	stopA()

	addrB := startBroker(t, natsURL, persistent, countingOutage(&down, &checks))
	down.Store(true)
	refused := dialRaw(t, addrB)
	refused.send(rawConnect("resume-stored", 300))
	ack, ok := refused.read().(*packet.Connack)
	require.True(t, ok)
	assert.Equal(t, packet.ImplementationSpecificError, ack.ReasonCode)

	down.Store(false)
	second := dialRaw(t, addrB)
	ack = second.connect(rawConnect("resume-stored", 300))
	assert.True(t, ack.SessionPresent)
	pub, _ := connectClient(t, addrB, connectOpts("pub-resume-stored"))
	pub.publish(&paho.Publish{Topic: "flaky/a", QoS: 0, Payload: []byte("kept")})
	assert.Equal(t, "flaky/a", second.expectPublish().Topic, "the outage reduced the stored session")
	pub.publish(&paho.Publish{Topic: "open/a", QoS: 0, Payload: []byte("kept")})
	assert.Equal(t, "open/a", second.expectPublish().Topic)
	assert.EqualValues(t, 2, resumeChecksOf(&checks, "flaky/a"))
}

// A plain denial on resume is still a drop, not a refusal.
func TestAResumeStillDropsAFilterTheAuthorizerDenies(t *testing.T) {
	var deny atomic.Bool
	addr := startBroker(t, startNATS(t), func(o *natsmqtt5.Options) {
		o.Authorizer = natsmqtt5.AuthorizerFunc(func(_ context.Context, req *natsmqtt5.AuthzRequest) error {
			if req.Resume && deny.Load() && req.Topic == "was/ok" {
				return errors.New("no")
			}
			return nil
		})
	})
	r := dialRaw(t, addr)
	r.connect(rawConnect("resume-deny", 300))
	r.subscribe("was/ok", packet.QoS0)
	r.drop()

	deny.Store(true)
	again := dialRaw(t, addr)
	ack := again.connect(rawConnect("resume-deny", 300))
	assert.True(t, ack.SessionPresent)
	pub, _ := connectClient(t, addr, connectOpts("pub-resume-deny"))
	pub.publish(&paho.Publish{Topic: "was/ok", QoS: 0, Payload: []byte("x")})
	pub.publish(&paho.Publish{Topic: "other", QoS: 0, Payload: []byte("x")})
	again.send(&packet.Pingreq{})
	_, ok := again.read().(*packet.Pingresp)
	require.True(t, ok, "a denied filter still delivered")
}
