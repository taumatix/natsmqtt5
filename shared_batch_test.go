package natsmqtt5_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5/v2"
	"github.com/taumatix/natsmqtt5/v2/packet"
)

// A member with a backlog pulls it in batches sized to the room its client has,
// not one message per round trip to JetStream, and never holds more than it can
// send.
//
// [MQTT-3.3.4-9] "The Server MUST NOT send more than Receive Maximum QoS 1 and
// QoS 2 PUBLISH packets for which it has not received PUBACK, PUBCOMP, or PUBREC
// with a Reason Code of 128 or greater from the Client." The client here is a raw
// socket that counts what it has been sent and not acknowledged; the fetch hook
// shows the batch sizes the member asked JetStream for.
func TestASharedMemberPullsABatchNoLargerThanItsReceiveMaximum_MQTT_3_3_4_9(t *testing.T) {
	const (
		receiveMax = 10
		jobs       = 60
	)
	var (
		mu      sync.Mutex
		largest int
	)
	f := newSharedFleet(t)
	hook := func(asked, _ int) {
		mu.Lock()
		defer mu.Unlock()
		largest = max(largest, asked)
	}
	natsmqtt5.SetSharedFetchHook(f.brokerA, hook)
	natsmqtt5.SetSharedFetchHook(f.brokerB, hook)
	m := dialRaw(t, f.a)
	cp := rawConnect("batcher", 300)
	cp.Properties.ReceiveMaximum = packet.Uint16(receiveMax)
	m.connect(cp)
	m.subscribe(group, packet.QoS1)
	publishJobs(t, f.a, jobs)

	// Read without acknowledging until the quota is full, so the backlog is
	// there for the next pulls, then acknowledge one at a time.
	var order []string
	var unacked []uint16
	for i := 0; i < receiveMax; i++ {
		p := m.expectPublish()
		order = append(order, string(p.Payload))
		unacked = append(unacked, p.PacketID)
	}
	m.expectNoPublishFor(500 * time.Millisecond)
	for len(order) < jobs {
		m.send(&packet.Puback{Ack: packet.Ack{PacketID: unacked[0]}})
		unacked = unacked[1:]
		p := m.expectPublish()
		order = append(order, string(p.Payload))
		unacked = append(unacked, p.PacketID)
		require.LessOrEqual(t, len(unacked), receiveMax, "[MQTT-3.3.4-9] never more than Receive Maximum unacknowledged")
	}
	for _, id := range unacked {
		m.send(&packet.Puback{Ack: packet.Ack{PacketID: id}})
	}
	m.expectNoPublishFor(500 * time.Millisecond)

	want := make([]string, jobs)
	for i := range want {
		want[i] = fmt.Sprintf("job-%d", i)
	}
	assert.Equal(t, want, order, "every message once, in order")
	mu.Lock()
	defer mu.Unlock()
	assert.Greater(t, largest, 1, "the member pulled more than one message at a time")
	assert.LessOrEqual(t, largest, receiveMax, "and never more than its client's Receive Maximum")
}
