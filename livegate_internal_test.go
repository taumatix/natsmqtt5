package natsmqtt5

import (
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5/packet"
	"github.com/taumatix/natsmqtt5/topic"
)

// The NATS subscriptions behind a filter exist before the filter is installed,
// and their handler delivers to whichever connection the session has at the
// time. So a subscription refused at install — decided for a connection that
// has since been displaced — is bound for a moment with nobody entitled to what
// it carries. The end-to-end test in displaced_subscribe_test.go cannot hit
// that moment on purpose; this holds a subscription in exactly that state, over
// a real NATS server, and publishes into it.
func TestABoundButUninstalledSubscriptionDeliversNothing(t *testing.T) {
	srv, err := natsserver.NewServer(&natsserver.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true})
	require.NoError(t, err)
	go srv.Start()
	require.True(t, srv.ReadyForConnections(10*time.Second))
	t.Cleanup(srv.Shutdown)

	nc, err := nats.Connect(srv.ClientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)

	r, err := Options{NATSURL: srv.ClientURL()}.resolve()
	require.NoError(t, err)
	b := &Broker{opts: r, logger: r.logger, nc: nc}

	newConn := func(sess *session) *conn {
		return &conn{broker: b, sess: sess, logger: r.logger,
			deliveries: make(chan *delivery, 8), done: make(chan struct{})}
	}
	sess := newSession("gate")
	displaced := newConn(sess)
	current := newConn(sess)
	sess.attach(current)

	subject, err := topic.FilterToSubject("secret/#")
	require.NoError(t, err)
	sub := &subscription{filter: "secret/#", subject: topic.Prefix(r.SubjectPrefix, subject),
		opts: packet.Subscription{Filter: "secret/#"}}
	require.NoError(t, displaced.bindNATS(sub))
	t.Cleanup(func() { unsubscribeAll(sub) })

	publish := func(payload string) {
		t.Helper()
		pubSubject, err := topic.FilterToSubject("secret/x")
		require.NoError(t, err)
		require.NoError(t, nc.Publish(topic.Prefix(r.SubjectPrefix, pubSubject), []byte(payload)))
		require.NoError(t, nc.Flush())
	}

	// Bound, not installed: the displaced connection's install is refused.
	_, installed, _ := sess.installSubscription(displaced, sub)
	require.False(t, installed, "the displaced connection must not be able to install")
	publish("before")
	select {
	case d := <-current.deliveries:
		t.Fatalf("an uninstalled subscription delivered %q to the session's connection", d.payload)
	case <-time.After(300 * time.Millisecond):
	}

	// The same subscription, installed by the connection entitled to it,
	// delivers — so the silence above was the gate and not a broken fixture.
	_, installed, _ = sess.installSubscription(current, sub)
	require.True(t, installed)
	publish("after")
	select {
	case d := <-current.deliveries:
		require.Equal(t, "after", string(d.payload))
	case <-time.After(5 * time.Second):
		t.Fatal("an installed subscription delivered nothing")
	}
}
