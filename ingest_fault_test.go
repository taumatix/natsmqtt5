package natsmqtt5_test

import (
	"context"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5/v2"
)

// Options.NATSIngest when the broker cannot keep what it is handed. Both tests inject the fault in the
// real thing: the queue stream is deleted from the JetStream server, and the broker's own NATS
// connection goes through a proxy that cuts it after the queue copy is stored. The requester is a plain
// nats.go client, and it must be told, not left with a reply that reads as success.

// With the queue stream gone the broker cannot store the copy, so the request is answered with an error
// and nothing is published live: a requester that retries would otherwise deliver twice.
func TestNATSIngestAnswersWithAnErrorWhenTheQueueCopyCannotBeStored(t *testing.T) {
	natsURL := startNATS(t)
	addr := startBroker(t, natsURL, withNATSIngest)
	sub, _ := connectClient(t, addr, connectOpts("ingest-nostore"))
	sub.subscribe(paho.SubscribeOptions{Topic: "ingest/nostore", QoS: 1})
	nc := plainNATS(t, natsURL)

	js, err := jetstream.New(nc)
	require.NoError(t, err)
	require.NoError(t, js.DeleteStream(context.Background(), natsmqtt5.DefaultStreamPrefix+"_queue"))

	reply := ingest(t, nc, "ingest.nostore", 1, "unstored")
	assert.Contains(t, reply.Header.Get("Mqtt5-Ingest-Error"), "could not be queued")
	assert.Empty(t, reply.Header.Get("Mqtt5-Queue-Seq"), "no sequence is named for a copy that was not stored")
	sub.expectNoMessage()
}

// With DurablePublish the broker flushes the live publish before it answers. When NATS stays unreachable
// after the queue copy is stored the flush times out, and the requester is told so (the reply waits in the
// broker's reconnect buffer until NATS is back) instead of being handed a sequence for a message that may
// never have been published.
func TestNATSIngestWithDurablePublishAnswersWithAnErrorWhenTheFlushCannotBeConfirmed(t *testing.T) {
	natsURL := startNATS(t)
	proxy := newCutProxy(t, natsURL)
	proxy.stayDown.Store(true)
	startBroker(t, proxy.url(), withNATSIngest, func(o *natsmqtt5.Options) { o.DurablePublish = true })
	nc := plainNATS(t, natsURL)

	msg := nats.NewMsg("mqtt.$ingest.ingest.flush")
	msg.Data = []byte("unconfirmed")
	msg.Header.Set("Mqtt5-Qos", "1")
	proxy.armed.Store(true)
	answer := make(chan *nats.Msg, 1)
	go func() {
		reply, err := nc.RequestMsg(msg, 40*time.Second)
		if err != nil {
			t.Errorf("no reply to the ingest request: %v", err)
		}
		answer <- reply
	}()

	// The cut turns the broker's reconnects away until its flush has timed out.
	require.Eventually(t, func() bool { return proxy.cut.Load() == 1 }, 10*time.Second, 5*time.Millisecond)
	time.Sleep(6 * time.Second) // offlineKeepTimeout is 5s; the flush gives up
	proxy.refuse.Store(false)
	proxy.stayDown.Store(false)

	select {
	case reply := <-answer:
		require.NotNil(t, reply)
		assert.Contains(t, reply.Header.Get("Mqtt5-Ingest-Error"), "did not confirm")
		assert.Empty(t, reply.Header.Get("Mqtt5-Queue-Seq"))
	case <-time.After(40 * time.Second):
		t.Fatal("the ingest request was never answered")
	}
	assert.Equal(t, 1, queuedCopies(t, queueStream(t, natsURL), "ingest.flush"),
		"the queue copy was stored before the flush failed")
}
