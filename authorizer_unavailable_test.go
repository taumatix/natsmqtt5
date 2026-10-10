package natsmqtt5_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// An Authorizer that cannot reach its policy returns ErrAuthorizerUnavailable.
// The client is told 0x83 (retry) rather than 0x87 (you may not), and a
// sweep by Reauthorize leaves alone what it could not check.

// outage fails every check under "flaky/" with ErrAuthorizerUnavailable while
// down is set, and every check under "denied/" with a plain error always.
func outage(down *atomic.Bool) func(*natsmqtt5.Options) {
	return func(o *natsmqtt5.Options) {
		o.Authorizer = natsmqtt5.AuthorizerFunc(func(_ context.Context, req *natsmqtt5.AuthzRequest) error {
			if strings.HasPrefix(req.Topic, "denied/") {
				return errors.New("no")
			}
			if down.Load() && strings.HasPrefix(req.Topic, "flaky/") && req.Topic != "flaky/still" {
				return fmt.Errorf("policy store: %w", natsmqtt5.ErrAuthorizerUnavailable)
			}
			return nil
		})
	}
}

func TestAnUnavailableAuthorizerAnswersSubscribeWith0x83NotNotAuthorized(t *testing.T) {
	var down atomic.Bool
	down.Store(true)
	addr := startBroker(t, startNATS(t), outage(&down))

	r := dialRaw(t, addr)
	r.connect(rawConnect("unavail-sub", 0))
	r.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{
		{Filter: "flaky/a", QoS: packet.QoS0},
		{Filter: "denied/a", QoS: packet.QoS0},
		{Filter: "open/a", QoS: packet.QoS0},
	}})
	ack, ok := r.read().(*packet.Suback)
	require.True(t, ok)
	assert.Equal(t, []packet.ReasonCode{
		packet.ImplementationSpecificError, packet.NotAuthorized, packet.ReasonCode(packet.QoS0),
	}, ack.ReasonCodes)
}

func TestAnUnavailableAuthorizerAnswersPublishWith0x83NotNotAuthorized(t *testing.T) {
	var down atomic.Bool
	down.Store(true)
	addr := startBroker(t, startNATS(t), outage(&down))

	r := dialRaw(t, addr)
	r.connect(rawConnect("unavail-pub", 0))
	for i, c := range []struct {
		topic string
		want  packet.ReasonCode
	}{
		{"flaky/x", packet.ImplementationSpecificError},
		{"denied/x", packet.NotAuthorized},
	} {
		id := uint16(i + 1)
		r.send(&packet.Publish{QoS: packet.QoS1, Topic: c.topic, PacketID: id, Payload: []byte("p")})
		ack, ok := r.read().(*packet.Puback)
		require.True(t, ok, c.topic)
		assert.Equal(t, c.want, ack.ReasonCode, c.topic)
	}
}

// Refusing the Will is not a takeover: the connection that holds the ClientID
// is still there afterwards.
func TestAnUnavailableAuthorizerRefusesAWillWith0x83AndDisplacesNoOne(t *testing.T) {
	var down atomic.Bool
	addr := startBroker(t, startNATS(t), outage(&down))

	holder := dialRaw(t, addr)
	holder.connect(rawConnect("unavail-will", 300))

	down.Store(true)
	cp := rawConnect("unavail-will", 300)
	cp.Will = &packet.Will{Topic: "flaky/will", Payload: []byte("w")}
	second := dialRaw(t, addr)
	second.send(cp)
	ack, ok := second.read().(*packet.Connack)
	require.True(t, ok)
	assert.Equal(t, packet.ImplementationSpecificError, ack.ReasonCode)

	holder.send(&packet.Pingreq{})
	_, ok = holder.read().(*packet.Pingresp)
	require.True(t, ok, "a CONNECT refused for an unavailable policy store displaced the live connection")
}

// An outage must not unsubscribe anyone: Reauthorize keeps the subscription it
// could not check and the Will, tells the caller, and a later sweep with the
// policy back still revokes.
func TestReauthorizeLeavesWhatItCouldNotCheckAndSaysSo(t *testing.T) {
	var down atomic.Bool
	b, addr, _ := startBrokerHandle(t, startNATS(t), outage(&down))

	watcher, _ := connectClient(t, addr, connectOpts("watcher-unavail"))
	watcher.subscribe(paho.SubscribeOptions{Topic: "flaky/will", QoS: 0})

	r := dialRaw(t, addr)
	cp := rawConnect("unavail-reauth", 300)
	cp.Will = &packet.Will{Topic: "flaky/will", Payload: []byte("gone")}
	r.connect(cp)
	r.subscribe("flaky/#", packet.QoS0)
	pub, _ := connectClient(t, addr, connectOpts("pub-unavail"))

	down.Store(true)
	err := b.Reauthorize(context.Background(), "unavail-reauth")
	require.Error(t, err)
	assert.True(t, errors.Is(err, natsmqtt5.ErrAuthorizerUnavailable), "got %v", err)

	pub.publish(&paho.Publish{Topic: "flaky/still", QoS: 0, Payload: []byte("hi")})
	got := r.expectPublish()
	assert.Equal(t, "flaky/still", got.Topic, "an outage unsubscribed a client")

	r.drop()
	assert.Equal(t, "flaky/will", watcher.expectMessage().Topic, "an outage discarded a Will it could not check")
}
