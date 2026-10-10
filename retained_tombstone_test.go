package natsmqtt5_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

func retainedStreamMsgs(t *testing.T, natsURL string) uint64 {
	t.Helper()
	s, err := jetStreamOf(t, natsURL).Stream(context.Background(), natsmqtt5.DefaultStreamPrefix+"_retained")
	require.NoError(t, err)
	info, err := s.Info(context.Background())
	require.NoError(t, err)
	return info.State.Msgs
}

// Clearing a retained message leaves a zero-length marker in the stream; the
// sweep deletes it after the tombstone TTL so cleared topics do not accumulate,
// and leaves a live retained message alone.
func TestRetainedTombstoneIsPurgedAfterItsTTL(t *testing.T) {
	natsURL := startNATS(t)
	addr := startBroker(t, natsURL, func(o *natsmqtt5.Options) {
		o.SessionSweepInterval = 200 * time.Millisecond
		o.RetainedTombstoneTTL = time.Second
	})
	pub, _ := connectClient(t, addr, connectOpts("tomb-pub"))
	publishExpiring(pub, "tomb/gone", "x", nil, true)
	publishExpiring(pub, "tomb/kept", "y", nil, true)
	publishExpiring(pub, "tomb/gone", "", nil, true)
	require.EqualValues(t, 2, retainedStreamMsgs(t, natsURL), "the tombstone and the live message")

	require.Eventually(t, func() bool { return retainedStreamMsgs(t, natsURL) == 1 },
		8*time.Second, 50*time.Millisecond, "the sweep must purge the tombstone")

	sub := dialRaw(t, addr)
	sub.connect(rawConnect("tomb-sub", 0))
	sub.subscribe("tomb/#", packet.QoS1)
	p := sub.expectPublish()
	assert.Equal(t, "y", string(p.Payload))
	sub.send(&packet.Puback{Ack: packet.Ack{PacketID: p.PacketID}})
	sub.expectNothing()
}

// A tombstone is kept while the TTL has not passed, and a message retained on
// the topic again is not touched when the old marker is purged.
func TestRetainedTombstoneNotPurgedEarlyOrOverARetainedAgain(t *testing.T) {
	natsURL := startNATS(t)
	addr := startBroker(t, natsURL, func(o *natsmqtt5.Options) {
		o.SessionSweepInterval = 100 * time.Millisecond
		o.RetainedTombstoneTTL = time.Hour
	})
	pub, _ := connectClient(t, addr, connectOpts("tomb2-pub"))
	publishExpiring(pub, "tomb2/t", "x", nil, true)
	publishExpiring(pub, "tomb2/t", "", nil, true)
	time.Sleep(600 * time.Millisecond)
	assert.EqualValues(t, 1, retainedStreamMsgs(t, natsURL), "within its TTL the tombstone stays")

	publishExpiring(pub, "tomb2/t", "again", nil, true)
	sub := dialRaw(t, addr)
	sub.connect(rawConnect("tomb2-sub", 0))
	sub.subscribe("tomb2/#", packet.QoS1)
	p := sub.expectPublish()
	assert.Equal(t, "again", string(p.Payload))
}
