package natsmqtt5_test

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// queueConsumerCreates records every request to create a consumer on the offline queue stream that the
// NATS server at natsURL sees, as the subjects JetStream's API is called on.
type queueConsumerCreates struct {
	mu       sync.Mutex
	subjects []string
}

func watchQueueConsumerCreates(t *testing.T, natsURL string) *queueConsumerCreates {
	t.Helper()
	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	w := &queueConsumerCreates{}
	_, err = nc.Subscribe("$JS.API.CONSUMER.CREATE."+natsmqtt5.DefaultStreamPrefix+"_queue.>", func(m *nats.Msg) {
		w.mu.Lock()
		w.subjects = append(w.subjects, m.Subject)
		w.mu.Unlock()
	})
	require.NoError(t, err)
	require.NoError(t, nc.Flush())
	return w
}

// ephemeral is how many consumers without a durable name were created: the replay's ordered consumers.
func (w *queueConsumerCreates) ephemeral() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := 0
	for _, s := range w.subjects {
		// An ordered consumer is created under a generated name (<nuid>_<n>); a shared subscription's
		// backlog consumer is a durable named <stream prefix>_share_<hash>.
		if !strings.Contains(s, "_share_") {
			n++
		}
	}
	return n
}

func (w *queueConsumerCreates) all() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.subjects...)
}

// No statement of the specification is at stake (what a session is owed is unchanged, [MQTT-4.4.0-1]); this
// pins the cost of a resume. A message of a shared subscription comes from the group's backlog, never
// from the replay, so a session whose subscriptions are all shared has nothing to read from the queue
// stream, and its resume must not read the stream (the cost grows with every other session's traffic).
// A session with a plain subscription does read it, which is the control that shows the watch works.
func TestResumeOfASessionWithOnlySharedSubscriptionsDoesNotReadTheQueue(t *testing.T) {
	natsURL := startNATS(t)
	var logs syncBuffer
	addr := startBroker(t, natsURL, loggingBroker(t, &logs))
	watch := watchQueueConsumerCreates(t, natsURL)

	for _, c := range []struct{ id, filter string }{
		{"rs-shared", "$share/g/rs/#"},
		{"rs-plain", "rs/#"},
	} {
		cl := dialRaw(t, addr)
		cl.connect(rawConnect(c.id, 300))
		cl.subscribe(c.filter, packet.QoS1)
		cl.drop()
		waitDetached(t, &logs, c.id)
	}

	producer := dialRaw(t, addr)
	producer.connect(rawConnect("rs-producer", 0))
	rawPublishQoS(producer, packet.QoS1, 1, "rs/x", "m1")
	_, isAck := producer.read().(*packet.Puback)
	require.True(t, isAck)

	before := watch.ephemeral()
	shared := dialRaw(t, addr)
	ack := shared.connect(rawConnect("rs-shared", 300))
	require.True(t, ack.SessionPresent)
	time.Sleep(500 * time.Millisecond)
	assert.Equal(t, before, watch.ephemeral(), "a shared-only resume read the queue: %v", watch.all())

	plain := dialRaw(t, addr)
	ack = plain.connect(rawConnect("rs-plain", 300))
	require.True(t, ack.SessionPresent)
	got := plain.expectPublish()
	assert.Equal(t, "m1", string(got.Payload))
	assert.Greater(t, watch.ephemeral(), before, "the control resume read the queue: %v", watch.all())
}
