package natsmqtt5_test

import (
	"bytes"
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// The offline queue became the default in a minor release, so a deployment
// that upgrades without touching its Options must keep starting. These tests
// pin the three ways it might not.

// startNATSWithoutJetStream runs an embedded NATS server with no JetStream, the
// server DisableRetained exists for.
func startNATSWithoutJetStream(t *testing.T) string {
	t.Helper()
	srv, err := natsserver.NewServer(&natsserver.Options{
		Host:   "127.0.0.1",
		Port:   -1,
		NoLog:  true,
		NoSigs: true,
	})
	require.NoError(t, err)
	go srv.Start()
	require.True(t, srv.ReadyForConnections(10*time.Second), "embedded NATS server did not start")
	t.Cleanup(srv.Shutdown)
	return srv.ClientURL()
}

// A broker told not to use JetStream does not start using it for the queue:
// it starts, and a session that was away gets nothing, as before.
func TestABrokerWithoutJetStreamStillStartsWithNoQueue(t *testing.T) {
	addr := startBroker(t, startNATSWithoutJetStream(t), func(o *natsmqtt5.Options) { o.DisableRetained = true })

	away := dialRaw(t, addr)
	away.connect(rawConnect("no-js", 300))
	away.subscribe("q/#", packet.QoS1)
	away.drop()

	pub, _ := connectClient(t, addr, connectOpts("pub-no-js"))
	pub.publish(&paho.Publish{Topic: "q/1", QoS: 1, Payload: []byte("1")})

	back := dialRaw(t, addr)
	require.True(t, back.connect(rawConnect("no-js", 300)).SessionPresent)
	back.expectNothing()
}

// squatQueueSubjects creates a stream that already holds the queue's subjects,
// so the broker's own queue stream cannot be created: what a deployment meets
// when its JetStream account is out of streams or storage, made reproducible.
func squatQueueSubjects(t *testing.T, natsURL string) {
	t.Helper()
	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	_, err = js.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:     "SQUATTER",
		Subjects: []string{natsmqtt5.DefaultSubjectPrefix + ".$queue.>"},
	})
	require.NoError(t, err)
}

// syncBuffer is a log sink a test can read while the broker writes to it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// When the queue is on only because it is the default and its stream cannot be
// created, the broker starts without it and says so at warn level, rather than
// an upgrade refusing to start. QoS 1 publishes go through as they did before.
func TestADefaultQueueThatCannotBeCreatedIsAWarningNotAFailure(t *testing.T) {
	natsURL := startNATS(t)
	squatQueueSubjects(t, natsURL)

	var logs syncBuffer
	addr := startBroker(t, natsURL, func(o *natsmqtt5.Options) {
		o.Logger = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn}))
	})
	assert.Contains(t, logs.String(), "offline queue")
	assert.Contains(t, logs.String(), "level=WARN")

	sub, _ := connectClient(t, addr, connectOpts("sub-squat"))
	sub.subscribe(paho.SubscribeOptions{Topic: "q/#", QoS: 1})
	pub, _ := connectClient(t, addr, connectOpts("pub-squat"))
	pub.publish(&paho.Publish{Topic: "q/1", QoS: 1, Payload: []byte("1")})
	assert.Equal(t, "1", sub.expectMessage().Payload)
}

// Asked for explicitly, the queue is a requirement: a broker that cannot have
// it does not start, as before the default changed.
func TestAnExplicitQueueThatCannotBeCreatedFailsTheBroker(t *testing.T) {
	natsURL := startNATS(t)
	squatQueueSubjects(t, natsURL)

	_, err := natsmqtt5.New(natsmqtt5.Options{NATSURL: natsURL, Listen: "127.0.0.1:0", OfflineQueue: true})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "offline queue")
}

func TestAskingForTheQueueAndDisablingItIsRefused(t *testing.T) {
	_, err := natsmqtt5.New(natsmqtt5.Options{OfflineQueue: true, DisableOfflineQueue: true})
	require.ErrorIs(t, err, natsmqtt5.ErrInvalidOptions)
}
