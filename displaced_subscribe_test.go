package natsmqtt5_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
)

// A SUBSCRIBE decided under one principal must not be installed into a session
// that another principal has since taken over.
//
// The Authorizer is a user-supplied call of unbounded duration, and taking a
// session over does not interrupt a connection that is inside it. So: A, as
// "victim", subscribes to secret/# and its Authorizer call is held open; B takes
// the Client Identifier over as "narrow", who may not read secret/#; B's resume
// sweep finds nothing to deny because the filter is not installed yet; then A's
// call returns allow — decided on victim's identity — and, before the fix,
// installed the filter into the session B now owns. Its NATS handler resolves
// the connection at delivery time, so B received victim's secrets.
func TestADisplacedConnectionCannotInstallAFilterIntoTheSessionItLost(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once

	addr := startBroker(t, startNATS(t), func(o *natsmqtt5.Options) {
		o.Authenticator = natsmqtt5.AuthenticatorFunc(func(_ context.Context, req *natsmqtt5.AuthRequest) (*natsmqtt5.AuthResult, error) {
			return &natsmqtt5.AuthResult{Identity: req.Username}, nil
		})
		o.Authorizer = natsmqtt5.AuthorizerFunc(func(ctx context.Context, req *natsmqtt5.AuthzRequest) error {
			if req.Action != natsmqtt5.ActionSubscribe || req.Topic != "secret/#" {
				return nil
			}
			if req.Identity == "narrow" {
				return errors.New("narrow may not read secret/#")
			}
			// victim may read it, but its decision is held open until the
			// takeover has happened.
			once.Do(func() { close(entered) })
			select {
			case <-release:
			case <-ctx.Done():
			}
			return nil
		})
	})

	as := func(user string) *paho.Connect {
		cp := durableConnect("shared-id", 300)
		cp.UsernameFlag, cp.Username = true, user
		return cp
	}

	victim, _ := connectClient(t, addr, as("victim"))
	go func() {
		// Its SUBACK can never arrive — the socket is closed under it — so the
		// error is expected and ignored.
		_, _ = victim.trySubscribe(paho.SubscribeOptions{Topic: "secret/#", QoS: 0})
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the SUBSCRIBE never reached the Authorizer")
	}

	narrow, connack := connectClient(t, addr, as("narrow"))
	require.True(t, connack.SessionPresent, "the takeover must resume the session, or there is nothing to leak into")
	// A permitted filter, so the absence below is not just a client that
	// receives nothing at all.
	narrow.subscribe(paho.SubscribeOptions{Topic: "public/#", QoS: 0})

	close(release)

	publisher, _ := connectClient(t, addr, connectOpts("publisher"))
	// Published over a window rather than once: the displaced goroutine
	// installs whenever the scheduler gets to it, and a single message could
	// land before that and prove nothing.
	deadline := time.Now().Add(time.Second)
	for i := 0; time.Now().Before(deadline); i++ {
		publisher.publish(&paho.Publish{Topic: fmt.Sprintf("secret/%d", i), Payload: []byte("victim's secret")})
		time.Sleep(20 * time.Millisecond)
	}
	publisher.publish(&paho.Publish{Topic: "public/x", Payload: []byte("public")})

	for {
		m := narrow.expectMessage()
		require.NotContains(t, m.Topic, "secret/",
			"narrow received %q on %q through a filter decided on victim's identity", m.Payload, m.Topic)
		if m.Topic == "public/x" {
			return
		}
	}
}
