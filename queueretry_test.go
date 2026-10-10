package natsmqtt5_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// A default queue that could not be created at start-up is created once the
// reason goes away, without a restart, and then does its job: a session that
// was away is replayed what was published while it was.
func TestADefaultQueueThatFailedAtStartupComesUpWhenTheReasonGoes(t *testing.T) {
	natsmqtt5.SetQueueRetryEvery(t, 50*time.Millisecond, 100*time.Millisecond)
	natsURL := startNATS(t)
	squatQueueSubjects(t, natsURL)

	var logs syncBuffer
	b, addr, _ := startBrokerHandle(t, natsURL, func(o *natsmqtt5.Options) {
		o.Logger = slog.New(slog.NewTextHandler(&logs, nil))
	})
	assert.False(t, b.OfflineQueueActive(), "the squatter holds the queue's subjects")
	time.Sleep(300 * time.Millisecond)
	assert.False(t, b.OfflineQueueActive(), "still blocked, still off")

	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	require.NoError(t, js.DeleteStream(context.Background(), "SQUATTER"))

	require.Eventually(t, b.OfflineQueueActive, 10*time.Second, 20*time.Millisecond,
		"the broker never retried the queue")
	assert.Contains(t, logs.String(), "the offline queue is now active")

	away := dialRaw(t, addr)
	away.connect(rawConnect("late-queue", 300))
	away.subscribe("q/#", packet.QoS1)
	away.drop()
	waitDetached(t, &logs, "late-queue")

	pub, _ := connectClient(t, addr, connectOpts("pub-late-queue"))
	pub.publish(&paho.Publish{Topic: "q/1", QoS: 1, Payload: []byte("queued")})

	back := dialRaw(t, addr)
	require.True(t, back.connect(rawConnect("late-queue", 300)).SessionPresent)
	assert.Equal(t, "queued", string(back.expectPublish().Payload))
}

// A queue that is off by option is not retried, and a queue that started is
// reported as active.
func TestOfflineQueueActiveReportsTheQueue(t *testing.T) {
	natsURL := startNATS(t)
	on, _, _ := startBrokerHandle(t, natsURL)
	assert.True(t, on.OfflineQueueActive())

	off, _, _ := startBrokerHandle(t, natsURL, func(o *natsmqtt5.Options) { o.DisableOfflineQueue = true })
	assert.False(t, off.OfflineQueueActive())
}
