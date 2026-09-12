package natsmqtt5_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
)

// The tests run a real NATS server in-process with JetStream enabled, a real
// broker against it, and the Eclipse Paho v5 client over a real TCP socket.
// Nothing here is a mock: what the tests exercise is the same code path a
// deployed broker takes, so a passing suite is evidence the wire format and
// the NATS mapping actually work together.

// startNATS runs an embedded NATS server with JetStream and returns its URL.
func startNATS(t *testing.T) string {
	t.Helper()
	opts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1, // any free port
		JetStream: true,
		StoreDir:  t.TempDir(),
		NoLog:     true,
		NoSigs:    true,
	}
	srv, err := natsserver.NewServer(opts)
	require.NoError(t, err)

	go srv.Start()
	require.True(t, srv.ReadyForConnections(10*time.Second), "embedded NATS server did not start")
	t.Cleanup(srv.Shutdown)

	return srv.ClientURL()
}

// startBroker runs a broker against natsURL on an ephemeral MQTT port and
// returns its address. Any Options field the caller sets is preserved; the
// test-specific defaults fill in the rest.
func startBroker(t *testing.T, natsURL string, customise ...func(*natsmqtt5.Options)) string {
	t.Helper()

	opts := natsmqtt5.Options{
		NATSURL: natsURL,
		Listen:  "127.0.0.1:0",
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	for _, fn := range customise {
		fn(&opts)
	}

	b, err := natsmqtt5.New(opts)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- b.Serve(ctx) }()

	t.Cleanup(func() {
		cancel()
		require.NoError(t, b.Close())
		select {
		case err := <-served:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Error("Serve did not return within 10s of shutdown")
		}
	})

	return b.ListenAddr().String()
}

// received is one message a test client got, flattened into the fields the
// assertions care about.
type received struct {
	Topic      string
	Payload    string
	QoS        byte
	Retain     bool
	Properties *paho.PublishProperties
}

// testClient is a Paho v5 client wired to collect the messages it receives.
type testClient struct {
	*paho.Client
	t        *testing.T
	messages chan received
}

// connectClient dials the broker and completes the MQTT handshake, failing the
// test if the CONNACK carries an error Reason Code.
func connectClient(t *testing.T, addr string, cp *paho.Connect) (*testClient, *paho.Connack) {
	t.Helper()
	tc, connack, err := tryConnect(t, addr, cp)
	require.NoError(t, err)
	require.False(t, connack.ReasonCode >= 0x80,
		"CONNACK refused the connection with reason 0x%02X", connack.ReasonCode)
	return tc, connack
}

// tryConnect is connectClient without the success assertion, for tests that
// expect a refusal.
func tryConnect(t *testing.T, addr string, cp *paho.Connect) (*testClient, *paho.Connack, error) {
	t.Helper()

	nc, err := net.DialTimeout("tcp", addr, 5*time.Second)
	require.NoError(t, err)

	tc := &testClient{t: t, messages: make(chan received, 64)}
	tc.Client = paho.NewClient(paho.ClientConfig{
		Conn:     nc,
		ClientID: cp.ClientID,
		OnPublishReceived: []func(paho.PublishReceived) (bool, error){
			func(pr paho.PublishReceived) (bool, error) {
				select {
				case tc.messages <- received{
					Topic:      pr.Packet.Topic,
					Payload:    string(pr.Packet.Payload),
					QoS:        pr.Packet.QoS,
					Retain:     pr.Packet.Retain,
					Properties: pr.Packet.Properties,
				}:
				default:
					t.Errorf("test client %q dropped a message on %q: buffer full", cp.ClientID, pr.Packet.Topic)
				}
				return true, nil
			},
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	connack, err := tc.Client.Connect(ctx, cp)
	if err != nil {
		nc.Close()
		return nil, connack, err
	}
	t.Cleanup(func() { _ = tc.Client.Disconnect(&paho.Disconnect{ReasonCode: 0}) })
	return tc, connack, nil
}

// expectMessage waits for one message and fails the test if none arrives.
func (c *testClient) expectMessage() received {
	c.t.Helper()
	select {
	case m := <-c.messages:
		return m
	case <-time.After(5 * time.Second):
		c.t.Fatal("timed out waiting for a message")
		return received{}
	}
}

// expectNoMessage asserts nothing arrives within a short settling window. The
// window is a compromise: long enough that a real delivery would land, short
// enough not to dominate the suite.
func (c *testClient) expectNoMessage() {
	c.t.Helper()
	select {
	case m := <-c.messages:
		c.t.Fatalf("expected no message, got %q on %q", m.Payload, m.Topic)
	case <-time.After(300 * time.Millisecond):
	}
}

func (c *testClient) subscribe(subs ...paho.SubscribeOptions) *paho.Suback {
	c.t.Helper()
	ack, err := c.trySubscribe(subs...)
	require.NoError(c.t, err)
	return ack
}

// trySubscribe is subscribe without the success assertion. Paho reports a
// SUBACK Reason Code of 0x80 or above as an error while still returning the
// SUBACK, so a test that expects a refusal needs both.
func (c *testClient) trySubscribe(subs ...paho.SubscribeOptions) (*paho.Suback, error) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return c.Client.Subscribe(ctx, &paho.Subscribe{Subscriptions: subs})
}

func (c *testClient) publish(p *paho.Publish) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := c.Client.Publish(ctx, p)
	require.NoError(c.t, err)
}

// connectOpts builds a minimal clean-start CONNECT.
func connectOpts(clientID string) *paho.Connect {
	return &paho.Connect{ClientID: clientID, CleanStart: true, KeepAlive: 30}
}
