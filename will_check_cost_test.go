package natsmqtt5_test

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The Will check of a broker that does not own a record must not fetch it on
// every tick: a broker listed and fetched every record of the bucket each
// WillCheckInterval, so the cost grew with the connections of the whole cluster.
func TestWillCheckDoesNotReadTheBucketEveryTick(t *testing.T) {
	url := startNATS(t)
	_, addrA, _ := startBrokerHandle(t, url, fastWills)
	_ = startBroker(t, url, fastWills)

	const clients = 100
	for i := 0; i < clients; i++ {
		id := fmt.Sprintf("c%d", i)
		dialRaw(t, addrA).connect(rawConnectWithWillMessage(id, "status/"+id, "gone"))
	}

	nc, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	var gets atomic.Int64
	sub, err := nc.Subscribe("$JS.API.DIRECT.GET.>", func(*nats.Msg) { gets.Add(1) })
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	require.NoError(t, nc.Flush())

	time.Sleep(time.Second)
	gets.Store(0)
	time.Sleep(1500 * time.Millisecond) // ten checks of 150ms; the fence probes add a few dozen reads that do not depend on the record count
	assert.Less(t, gets.Load(), int64(clients), "direct gets over ten checks of %d records", clients)
}
