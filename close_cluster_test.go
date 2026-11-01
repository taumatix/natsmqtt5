package natsmqtt5_test

import (
	"fmt"
	"os"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// Broker.Close against the NATS failures a cluster has and an embedded server does not: the server
// the broker is connected to frozen (the socket stays open and nothing answers), killed, and cut
// off from its peers. The broker has what Close must end: a connected session with messages in
// flight, a shared-subscription member whose pull goroutine is inside a JetStream fetch, a client
// that never reads, and the catch-up options. Each Close runs under a deadline well past the
// 30 s drain timeout nats.go applies, and the time it took is logged.
//
// Measured on 2026-11-01: with the client that never reads, Close hung in shutdown's DISCONNECT write
// (behind the write lock the stuck writer held) whatever the fault; conn.shutdown now sets a write deadline.
func TestClusterCloseReturnsWhenItsNATSServerFreezesDiesOrIsCutOff(t *testing.T) {
	natsmqtt5.SetSharedAckWait(t, handBackAckWait)
	faults := []struct {
		name          string
		inject, clear func(c *natsCluster)
	}{
		{"frozen", func(c *natsCluster) { c.Freeze(2) }, func(c *natsCluster) { c.Thaw(2) }},
		{"killed", func(c *natsCluster) { c.StopServer(2, false) }, func(c *natsCluster) { c.StartServer(2) }},
		{"cut off", func(c *natsCluster) { c.Isolate(2) }, func(c *natsCluster) { c.Heal(2) }},
	}
	for i, f := range faults {
		t.Run(f.name, func(t *testing.T) {
			// A cluster of its own: one that has just recovered from the previous fault is slow to
			// place the next broker's replicated buckets, which is not what this measures.
			c := startNATSCluster(t)
			pubAddr := startBroker(t, c.URL(0), clusterReplicas)
			b, addr, _ := startBrokerHandle(t, c.URL(2), clusterReplicas, persistentWithQueue, withReconnectCatchUp,
				quickReconnect, sweeping)
			topic := fmt.Sprintf("cc%d/t", i)

			// A connected session with unacknowledged messages in flight.
			cp := connectOpts(fmt.Sprintf("cc%d-sub", i))
			cp.Properties = &paho.ConnectProperties{ReceiveMaximum: packet.Uint16(2)}
			sub, _ := connectClient(t, addr, cp, manualAck)
			sub.subscribe(paho.SubscribeOptions{Topic: topic, QoS: 1})
			// A member of a shared group that holds its message: its pull goroutine is running.
			m := dialRaw(t, addr)
			m.connect(oneAtATime(fmt.Sprintf("cc%d-m", i), 0))
			m.subscribe(fmt.Sprintf("$share/g%d/%s", i, topic), packet.QoS1)
			// A client whose socket nothing reads.
			deaf := dialRaw(t, addr)
			deaf.connect(rawConnect(fmt.Sprintf("cc%d-deaf", i), 0))
			deaf.subscribe(topic, packet.QoS0)

			pub, _ := connectClient(t, pubAddr, connectOpts(fmt.Sprintf("cc%d-pub", i)))
			payload := make([]byte, 64<<10)
			for range 20 {
				pub.publish(&paho.Publish{Topic: topic, QoS: 1, Payload: payload})
			}
			m.expectPublish()

			f.inject(c)
			for range 20 { // while the broker has not noticed yet
				pub.publish(&paho.Publish{Topic: topic, QoS: 0, Payload: payload})
			}

			start := time.Now()
			done := make(chan error, 1)
			go func() { done <- b.Close() }()
			select {
			case err := <-done:
				require.NoError(t, err)
				t.Logf("Close with its server %s took %s", f.name, time.Since(start).Round(time.Millisecond))
			case <-time.After(45 * time.Second):
				_ = pprof.Lookup("goroutine").WriteTo(os.Stderr, 1)
				f.clear(c)
				t.Fatalf("Close did not return within 45s with its NATS server %s", f.name)
			}
			f.clear(c)
			c.awaitReady()
		})
	}
}
