package natsmqtt5_test

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
)

// startNATSAt starts an embedded server on a fixed port and store directory, so a
// second call with the same arguments is the same server coming back after a restart.
func startNATSAt(t *testing.T, port int, dir string) *natsserver.Server {
	t.Helper()
	srv, err := natsserver.NewServer(&natsserver.Options{
		Host: "127.0.0.1", Port: port, JetStream: true, StoreDir: dir, NoLog: true, NoSigs: true,
	})
	require.NoError(t, err)
	go srv.Start()
	require.True(t, srv.ReadyForConnections(15*time.Second), "embedded NATS server did not start")
	return srv
}

// A broker keeps its view of the Will bucket from a watcher. After the NATS server
// restarts under two brokers, a Will stored once they are back must be adopted when its
// broker is killed: a watcher that came back stale would leave it unpublished. (The
// restart takes longer than twice WillCheckInterval, so the brokers fence their clients
// meanwhile, which is the documented behaviour; the test connects new ones afterwards.)
func TestWillsAreAdoptedAfterTheNATSServerRestarts(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	require.NoError(t, ln.Close())
	dir := t.TempDir()

	srv := startNATSAt(t, port, dir)
	url := fmt.Sprintf("nats://127.0.0.1:%d", port)
	slow := func(o *natsmqtt5.Options) { fastWills(o); o.WillCheckInterval = time.Second }
	a, addrA, _ := startBrokerHandle(t, url, slow)
	addrB := startBroker(t, url, slow)

	srv.Shutdown()
	srv.WaitForShutdown()
	srv = startNATSAt(t, port, dir)
	t.Cleanup(srv.Shutdown)
	time.Sleep(3 * time.Second) // the brokers reconnect and their watchers re-establish

	watcher, _ := connectClient(t, addrB, connectOpts("watcher"))
	watcher.subscribe(paho.SubscribeOptions{Topic: "status/#", QoS: 1})
	dialRaw(t, addrA).connect(rawConnectWithWillMessage("after", "status/after", "gone"))
	time.Sleep(time.Second) // the record reaches the other broker's index

	natsmqtt5.Kill(a)

	select {
	case m := <-watcher.messages:
		assert.Equal(t, "status/after", m.Topic)
	case <-time.After(25 * time.Second):
		t.Fatal("the Will stored after the restart was not adopted")
	}
}
