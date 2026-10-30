package natsmqtt5_test

import (
	"strings"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"

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
func TestClusterAConnectionWithManyUnacknowledgedMessagesStillEndsWhenItsNATSServerFreezes(t *testing.T) {
	c := startNATSCluster(t)
	_, addr, stop := startBrokerHandle(t, c.URL(0), clusterReplicas)

	sub := dialRaw(t, addr)
	sub.connect(rawConnect("frozen-sub", 300))
	sub.subscribe("frozen/#", packet.QoS1)

	pub, _ := connectClient(t, addr, connectOpts("frozen-pub"))
	big := []byte(strings.Repeat("x", 200*1024))
	const unacknowledged = 8
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
	case <-time.After(60 * time.Second):
		t.Fatal("the connection did not end within 60 s of its NATS server freezing")
	}
	assert.Less(t, time.Since(start), 40*time.Second,
		"ending the connection took one deadline per unacknowledged message")
}
