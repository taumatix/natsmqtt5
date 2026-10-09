package natsmqtt5_test

import (
	"bytes"
	"context"
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

// A payload the payload bucket already holds is not written again by the broker that
// restores the session: the record carries when it was written, so the restored entry
// is as old as the payload is and not "never written". Without it every restored
// oversize message cost one extra bucket write at the successor's first checkpoint.
func TestARestoredPayloadIsNotWrittenToTheBucketAgain(t *testing.T) {
	natsURL := startNATS(t)
	cfg := func(o *natsmqtt5.Options) {
		o.PersistentSessions, o.DisableOfflineQueue = true, true
		o.SessionCheckpointInterval = 100 * time.Millisecond
	}
	addrA, stopA := startStoppableBroker(t, natsURL, cfg)
	addrB := startBroker(t, natsURL, cfg)

	c := dialRaw(t, addrA)
	c.connect(rawConnect("big-restored", 300))
	c.subscribe("br/#", packet.QoS1)
	pub, _ := connectClient(t, addrA, connectOpts("pub-big-restored"))
	pub.publish(&paho.Publish{Topic: "br/large", QoS: 1, Payload: bytes.Repeat([]byte("r"), 200<<10)})
	require.Equal(t, "br/large", c.expectPublish().Topic)

	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	ctx := context.Background()

	var kv jetstream.KeyValue
	var key string
	require.Eventually(t, func() bool {
		var err error
		if kv, err = js.KeyValue(ctx, natsmqtt5.DefaultStreamPrefix+"_inflight"); err != nil {
			return false
		}
		keys, err := kv.Keys(ctx)
		if err != nil || len(keys) != 1 {
			return false
		}
		key = keys[0]
		return true
	}, 5*time.Second, 50*time.Millisecond, "the payload is stored while the client is connected")
	c.drop()
	time.Sleep(300 * time.Millisecond) // the detach record, with the payload's write time
	stopA()

	before, err := kv.Get(ctx, key)
	require.NoError(t, err)

	back := dialRaw(t, addrB)
	require.True(t, back.connect(rawConnect("big-restored", 300)).SessionPresent)
	assert.Equal(t, "br/large", back.expectPublish().Topic, "the payload was restored from the bucket")
	time.Sleep(time.Second) // ten checkpoints on the broker that restored it

	after, err := kv.Get(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, before.Revision(), after.Revision(), "the restored payload was not written again")
}
