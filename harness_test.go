package natsmqtt5_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
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
	addr, _ := startStoppableBroker(t, natsURL, customise...)
	return addr
}

// startStoppableBroker is startBroker with a handle that shuts the broker down
// early, which is how a test simulates a restart or a failover. Calling stop is
// optional: the cleanup runs it if the test did not.
func startStoppableBroker(t *testing.T, natsURL string, customise ...func(*natsmqtt5.Options)) (addr string, stop func()) {
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

	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			require.NoError(t, b.Close())
			select {
			case err := <-served:
				require.NoError(t, err)
			case <-time.After(10 * time.Second):
				t.Error("Serve did not return within 10s of shutdown")
			}
		})
	}
	t.Cleanup(stop)

	return b.ListenAddr().String(), stop
}

// received is one message a test client got, flattened into the fields the
// assertions care about.
type received struct {
	Topic      string
	Payload    string
	QoS        byte
	Retain     bool
	Properties *paho.PublishProperties
	// Dup is the DUP flag, which is how a client tells a retransmission from a
	// first attempt (MQTT-5.0 §3.3.1.1).
	Dup bool
	// PacketID matters to a retransmission test: a resend "MUST use the
	// original Packet Identifier" [MQTT-4.4.0-1].
	PacketID uint16
	// Ack sends the acknowledgement this message is owed. It only does anything
	// under manualAck, where nothing is acknowledged until a test says so.
	Ack func() error
}

// testClient is a Paho v5 client wired to collect the messages it receives and
// any DISCONNECT the broker sends it.
type testClient struct {
	*paho.Client
	t        *testing.T
	nc       net.Conn
	messages chan received
	// disconnects collects server-sent DISCONNECTs. A broker disconnecting a
	// client it should be serving is invisible from the message channel alone —
	// it looks exactly like a message that never arrived.
	disconnects chan *paho.Disconnect
}

// connectClient dials the broker and completes the MQTT handshake, failing the
// test if the CONNACK carries an error Reason Code.
func connectClient(t *testing.T, addr string, cp *paho.Connect, customise ...func(*paho.ClientConfig)) (*testClient, *paho.Connack) {
	t.Helper()
	tc, connack, err := tryConnect(t, addr, cp, customise...)
	require.NoError(t, err)
	require.False(t, connack.ReasonCode >= 0x80,
		"CONNACK refused the connection with reason 0x%02X", connack.ReasonCode)
	return tc, connack
}

// tryConnect is connectClient without the success assertion, for tests that
// expect a refusal. Any customise function is applied to the Paho client
// configuration before it connects, which is how a test asks for manual
// acknowledgement.
func tryConnect(t *testing.T, addr string, cp *paho.Connect, customise ...func(*paho.ClientConfig)) (*testClient, *paho.Connack, error) {
	t.Helper()

	nc, err := net.DialTimeout("tcp", addr, 5*time.Second)
	require.NoError(t, err)

	tc := &testClient{
		t:           t,
		nc:          nc,
		messages:    make(chan received, 64),
		disconnects: make(chan *paho.Disconnect, 4),
	}
	cfg := paho.ClientConfig{
		Conn:               nc,
		ClientID:           cp.ClientID,
		OnServerDisconnect: func(d *paho.Disconnect) { tc.disconnects <- d },
		OnPublishReceived: []func(paho.PublishReceived) (bool, error){
			func(pr paho.PublishReceived) (bool, error) {
				select {
				case tc.messages <- received{
					Topic:      pr.Packet.Topic,
					Payload:    string(pr.Packet.Payload),
					QoS:        pr.Packet.QoS,
					Retain:     pr.Packet.Retain,
					Properties: pr.Packet.Properties,
					Dup:        pr.Packet.Duplicate(),
					PacketID:   pr.Packet.PacketID,
					Ack:        func() error { return pr.Client.Ack(pr.Packet) },
				}:
				default:
					t.Errorf("test client %q dropped a message on %q: buffer full", cp.ClientID, pr.Packet.Topic)
				}
				return true, nil
			},
		},
	}
	for _, fn := range customise {
		fn(&cfg)
	}
	tc.Client = paho.NewClient(cfg)

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

// dropConnection closes the socket under the client without sending a
// DISCONNECT, which is what a lost network looks like to the broker.
//
// A clean DISCONNECT with a non-zero Session Expiry Interval would leave the
// same arrears — §4.4 says nothing about how the previous connection ended —
// but it is the lost network that the feature exists for, and it is the one a
// deployment hits.
func (c *testClient) dropConnection() {
	c.t.Helper()
	require.NoError(c.t, c.nc.Close())
}

// manualAck stops the Paho client acknowledging anything by itself, so a test
// can leave a QoS 1 delivery unacknowledged. Paho only sends the PUBACK when
// Client.Ack is called, which these tests never do.
func manualAck(cfg *paho.ClientConfig) {
	cfg.EnableManualAcknowledgment = true
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

// expectServerDisconnect waits for the broker to disconnect this client and
// returns the Reason Code it gave.
func (c *testClient) expectServerDisconnect() byte {
	c.t.Helper()
	select {
	case d := <-c.disconnects:
		return d.ReasonCode
	case <-time.After(5 * time.Second):
		c.t.Fatal("the broker never disconnected this client")
		return 0
	}
}

// expectNoServerDisconnect asserts the broker leaves this client alone. The
// window has to outlast a round trip to NATS, since the disconnections worth
// catching are triggered by something arriving from there.
func (c *testClient) expectNoServerDisconnect() {
	c.t.Helper()
	select {
	case d := <-c.disconnects:
		c.t.Fatalf("the broker disconnected this client with reason 0x%02X", d.ReasonCode)
	case <-time.After(time.Second):
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
