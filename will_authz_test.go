package natsmqtt5_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// A Will Message is a publish the broker makes on the client's behalf, after the
// client has gone and at a moment the client chooses with the Will Delay
// Interval. It has to pass the same Authorizer a PUBLISH does, or a principal
// denied a topic can publish — and retain — to it by setting a Will and
// dropping its connection.
//
// The check is made at CONNECT, and a denied Will refuses the connection with
// 0x87: a Will the broker would refuse to publish is a Will it should not
// accept, which is the reasoning MQTT-5.0 already applies to a Will whose QoS or
// RETAIN the broker cannot honour [MQTT-3.2.2-12], [MQTT-3.2.2-13].

// denyAdminPublish denies ActionPublish on anything under "admin/" and records
// every request it sees.
type denyAdminPublish struct {
	mu   sync.Mutex
	seen []natsmqtt5.AuthzRequest
}

func (d *denyAdminPublish) option(o *natsmqtt5.Options) {
	o.Authorizer = natsmqtt5.AuthorizerFunc(func(_ context.Context, req *natsmqtt5.AuthzRequest) error {
		d.mu.Lock()
		d.seen = append(d.seen, *req)
		d.mu.Unlock()
		if req.Action == natsmqtt5.ActionPublish && strings.HasPrefix(req.Topic, "admin/") {
			return errors.New("admin topics are not yours")
		}
		return nil
	})
}

func (d *denyAdminPublish) willRequests() []natsmqtt5.AuthzRequest {
	d.mu.Lock()
	defer d.mu.Unlock()
	var out []natsmqtt5.AuthzRequest
	for _, r := range d.seen {
		if r.Will {
			out = append(out, r)
		}
	}
	return out
}

func willConnect(clientID, willTopic string, retain bool) *paho.Connect {
	cp := connectOpts(clientID)
	cp.WillMessage = &paho.WillMessage{Topic: willTopic, Payload: []byte("shutdown"), QoS: 1, Retain: retain}
	return cp
}

// The attack the check exists for: a retained Will on a denied topic. Neither
// a live subscriber nor one arriving afterwards may see it.
func TestAWillOnADeniedTopicRefusesTheConnection(t *testing.T) {
	authz := &denyAdminPublish{}
	addr := startBroker(t, startNATS(t), authz.option)

	watcher, _ := connectClient(t, addr, connectOpts("watcher"))
	watcher.subscribe(paho.SubscribeOptions{Topic: "admin/#", QoS: 1})

	_, connack, err := tryConnect(t, addr, willConnect("intruder", "admin/shutdown", true))
	require.Error(t, err, "the CONNECT must be refused")
	require.NotNil(t, connack)
	assert.Equal(t, byte(packet.NotAuthorized), connack.ReasonCode)

	watcher.expectNoMessage()

	// A subscriber arriving now would be handed a retained Will, had one been
	// stored when the refused connection closed.
	late, _ := connectClient(t, addr, connectOpts("late"))
	late.subscribe(paho.SubscribeOptions{Topic: "admin/#", QoS: 1})
	late.expectNoMessage()
}

// The Authorizer is asked about the Will as a publish, under the identity the
// Authenticator established and the Client Identifier the session will use, and
// is told it is a Will so it can tell it apart from a PUBLISH the client made.
func TestTheAuthorizerSeesTheWillAsAPublishByThePrincipal(t *testing.T) {
	authz := &denyAdminPublish{}
	addr := startBroker(t, startNATS(t), authz.option, func(o *natsmqtt5.Options) {
		o.Authenticator = natsmqtt5.AuthenticatorFunc(func(_ context.Context, req *natsmqtt5.AuthRequest) (*natsmqtt5.AuthResult, error) {
			return &natsmqtt5.AuthResult{Identity: "principal-" + req.Username, AssignClientID: "assigned-" + req.Username}, nil
		})
	})

	cp := willConnect("", "status/sensor-7", true)
	cp.UsernameFlag, cp.Username = true, "sensor-7"
	connectClient(t, addr, cp)

	reqs := authz.willRequests()
	require.Len(t, reqs, 1, "exactly one Authorizer call for the Will")
	assert.Equal(t, natsmqtt5.AuthzRequest{
		Action:   natsmqtt5.ActionPublish,
		Will:     true,
		ClientID: "assigned-sensor-7",
		Identity: "principal-sensor-7",
		Username: "sensor-7",
		Topic:    "status/sensor-7",
		QoS:      1,
		Retain:   true,
	}, reqs[0])
}

// A permitted Will is published exactly as before.
func TestAPermittedWillIsStillPublished(t *testing.T) {
	authz := &denyAdminPublish{}
	addr := startBroker(t, startNATS(t), authz.option)

	watcher, _ := connectClient(t, addr, connectOpts("watcher"))
	watcher.subscribe(paho.SubscribeOptions{Topic: "status/#", QoS: 1})

	dying, _ := connectClient(t, addr, willConnect("dying", "status/dying", false))
	require.NoError(t, dying.Client.Disconnect(&paho.Disconnect{ReasonCode: 0x04}),
		"reason 0x04 asks the server to publish the Will anyway")

	got := watcher.expectMessage()
	assert.Equal(t, "status/dying", got.Topic)
	assert.Equal(t, "shutdown", got.Payload)
}

// Refusing the CONNECT has to happen before the broker takes the session over.
// Otherwise a refused connection still displaces the live one on the same
// Client Identifier, and a denied Will becomes a way to knock any client off.
func TestARefusedWillDoesNotDisplaceTheLiveConnection(t *testing.T) {
	authz := &denyAdminPublish{}
	addr := startBroker(t, startNATS(t), authz.option)

	victim, _ := connectClient(t, addr, durableConnect("shared-id", 300))
	victim.subscribe(paho.SubscribeOptions{Topic: "news/#", QoS: 1})

	cp := willConnect("shared-id", "admin/shutdown", false)
	_, connack, err := tryConnect(t, addr, cp)
	require.Error(t, err)
	require.NotNil(t, connack)
	assert.Equal(t, byte(packet.NotAuthorized), connack.ReasonCode)

	victim.expectNoServerDisconnect()
	pub, _ := connectClient(t, addr, connectOpts("pub"))
	pub.publish(&paho.Publish{Topic: "news/today", QoS: 1, Payload: []byte("still here")})
	assert.Equal(t, "still here", victim.expectMessage().Payload)
}

// A CONNECT without a Will costs the Authorizer nothing.
func TestAConnectWithoutAWillDoesNotCallTheAuthorizer(t *testing.T) {
	authz := &denyAdminPublish{}
	addr := startBroker(t, startNATS(t), authz.option)

	connectClient(t, addr, connectOpts("no-will"))

	authz.mu.Lock()
	defer authz.mu.Unlock()
	assert.Empty(t, authz.seen)
}
