package natsmqtt5_test

import (
	"strings"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// A connection's end waits for its delivery goroutine and writes the session record one
// last time. When the broker's NATS server stops answering without closing a socket (a
// frozen process, a black-holed link), every JetStream call on that path runs to its
// deadline, so the end must have one bound in total and not one per message: otherwise a
// client with many unacknowledged messages keeps its slot, and its Packet Identifiers and
// send quota, for as many times the deadline as it has messages.
// [MQTT-4.4.0-1]: the broker still resends what the client had not acknowledged, from the
// record, once the server answers again; nothing here may drop that record.
//
// The messages must be ones the end has to store one by one, or the test proves nothing: with
// the offline queue an in-flight entry names its queue copy and nothing is stored, and the test
// passed before the bound existed. So the broker runs without the queue, each PUBLISH is larger
// than the record keeps inline (16 KiB), which sends it to the payload bucket, and the checkpoint
// is off, so none of them was stored before the server froze. With one deadline per payload
// (5 s each, as before #170) the twelve took 60 s; with one for all of them, 5 s.
func TestClusterAConnectionWithManyUnacknowledgedMessagesStillEndsWhenItsNATSServerFreezes(t *testing.T) {
	c := startNATSCluster(t)
	_, addr, stop := startBrokerHandle(t, c.URL(0), clusterReplicas, func(o *natsmqtt5.Options) {
		o.PersistentSessions = true
		o.DisableOfflineQueue = true
		o.SessionCheckpointInterval = -1
	})

	sub := dialRaw(t, addr)
	sub.connect(rawConnect("frozen-sub", 300))
	sub.subscribe("frozen/#", packet.QoS1)

	pub, _ := connectClient(t, addr, connectOpts("frozen-pub"))
	big := []byte(strings.Repeat("x", 32*1024))
	const unacknowledged = 12
	for range unacknowledged {
		pub.publish(&paho.Publish{Topic: "frozen/t", QoS: 1, Payload: big})
	}
	for range unacknowledged {
		sub.expectPublish() // read, never acknowledged
	}

	c.Freeze(0)
	t.Cleanup(func() { c.Thaw(0) })

	start := time.Now()
	sub.drop()
	// The broker's own shutdown waits for every connection to finish.
	done := make(chan struct{})
	go func() { defer close(done); stop() }()
	select {
	case <-done:
	case <-time.After(90 * time.Second):
		t.Fatal("the connection did not end within 90 s of its NATS server freezing")
	}
	took := time.Since(start)
	t.Logf("the connection ended %v after its NATS server froze", took.Round(time.Millisecond))
	assert.Less(t, took, 35*time.Second,
		"ending the connection took one deadline per unacknowledged message")
}
