package natsmqtt5_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

// A three-server NATS cluster in containers, for the failures an embedded server cannot
// produce: a server that is alive but cut off from its cluster, a restart of one member
// of a replicated stream, a long outage of the whole cluster.
//
// Gated, so `go test ./...` passes where no container runtime exists:
//
//	NATSMQTT5_CLUSTER_TESTS=1 go test -run Cluster -count=1 .
//
// The runtime is Apple's `container` or `docker`, whichever is found first (force one with
// NATSMQTT5_CLUSTER_RUNTIME=docker). NATSMQTT5_CLUSTER_IMAGE overrides the NATS image
// (default docker.io/library/nats:2-alpine, a public image).
//
// Each node is a container whose first process is `sleep`; nats-server is started inside it
// with `exec`. That is what lets a test stop and start the server without losing the
// container's address (the routes between the nodes are written with those addresses; neither
// runtime resolves container names on its default network), and what lets a test cut a node
// off: the containers get NET_ADMIN and a blackhole route to a peer makes it unreachable in
// both directions while every process stays alive, which is what a network partition is.
// Client ports are published on 127.0.0.1 only; nothing here touches any other host.

type natsCluster struct {
	t       *testing.T
	runtime string
	names   [3]string
	ips     [3]string
	ports   [3]int
}

// livePIDs expands to the pids of nats-server that are not zombies: the container's first
// process is `sleep`, which never reaps, so a killed server stays listed by pidof.
const livePIDs = `$(for p in $(pidof nats-server); do grep -q '^State:.*Z' /proc/$p/status || echo $p; done)`

const clusterImageDefault = "docker.io/library/nats:2-alpine"

// startNATSCluster starts the cluster and waits until JetStream can place a three-replica
// stream, or skips the test when the gate is closed.
func startNATSCluster(t *testing.T) *natsCluster {
	t.Helper()
	if os.Getenv("NATSMQTT5_CLUSTER_TESTS") == "" {
		t.Skip("set NATSMQTT5_CLUSTER_TESTS=1 to run the tests that need a 3-server NATS cluster in containers")
	}
	rt := os.Getenv("NATSMQTT5_CLUSTER_RUNTIME")
	if rt == "" {
		for _, cand := range []string{"container", "docker"} {
			if _, err := exec.LookPath(cand); err == nil {
				rt = cand
				break
			}
		}
	}
	if rt == "" {
		t.Skip("NATSMQTT5_CLUSTER_TESTS is set but neither `container` nor `docker` is on PATH")
	}
	image := os.Getenv("NATSMQTT5_CLUSTER_IMAGE")
	if image == "" {
		image = clusterImageDefault
	}

	var id [4]byte
	_, _ = rand.Read(id[:])
	c := &natsCluster{t: t, runtime: rt}
	t.Cleanup(c.remove)
	for i := range c.names {
		c.names[i] = fmt.Sprintf("natsmqtt5-ct-%s-%d", hex.EncodeToString(id[:]), i)
		c.ports[i] = freePort(t)
		c.run("run", "-d", "--name", c.names[i], "--cap-add", "NET_ADMIN",
			"-p", fmt.Sprintf("127.0.0.1:%d:4222", c.ports[i]), "--entrypoint", "sleep", image, "86400")
	}
	for i := range c.names {
		c.ips[i] = c.address(i)
	}
	var routes []string
	for _, ip := range c.ips {
		routes = append(routes, "nats://"+ip+":6222")
	}
	for i := range c.names {
		c.startServer(i, strings.Join(routes, ","))
	}
	c.awaitReady()
	return c
}

// freePort asks the kernel for a port nothing listens on; the container publishes it next.
func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

// run executes the runtime's CLI and fails the test when it fails.
func (c *natsCluster) run(args ...string) string {
	c.t.Helper()
	out, err := c.try(args...)
	require.NoError(c.t, err, "%s %s", c.runtime, strings.Join(args, " "))
	return out
}

func (c *natsCluster) try(args ...string) (string, error) {
	cmd := exec.Command(c.runtime, args...)
	var buf bytes.Buffer
	cmd.Stdout, cmd.Stderr = &buf, &buf
	done := make(chan error, 1)
	if err := cmd.Start(); err != nil {
		return "", err
	}
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			return buf.String(), fmt.Errorf("%w: %s", err, buf.String())
		}
	case <-time.After(60 * time.Second):
		_ = cmd.Process.Kill()
		return buf.String(), fmt.Errorf("timed out")
	}
	return strings.TrimSpace(buf.String()), nil
}

// sh runs a shell command inside node i.
func (c *natsCluster) sh(i int, script string) string {
	c.t.Helper()
	return c.run("exec", c.names[i], "sh", "-c", script)
}

// address reads node i's address on the runtime's default network.
func (c *natsCluster) address(i int) string {
	c.t.Helper()
	if c.runtime == "docker" {
		return c.run("inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", c.names[i])
	}
	var info []struct {
		Status struct {
			Networks []struct {
				IPv4 string `json:"ipv4Address"`
			} `json:"networks"`
		} `json:"status"`
	}
	require.NoError(c.t, json.Unmarshal([]byte(c.run("inspect", c.names[i])), &info))
	require.NotEmpty(c.t, info)
	require.NotEmpty(c.t, info[0].Status.Networks, "container %s has no network", c.names[i])
	return strings.SplitN(info[0].Status.Networks[0].IPv4, "/", 2)[0]
}

func (c *natsCluster) routes() string {
	var r []string
	for _, ip := range c.ips {
		r = append(r, "nats://"+ip+":6222")
	}
	return strings.Join(r, ",")
}

// startServer launches nats-server in node i, detached from the exec session that starts it.
func (c *natsCluster) startServer(i int, routes string) {
	c.t.Helper()
	c.sh(i, fmt.Sprintf("nohup setsid nats-server -js -n n%d -sd /data --cluster_name ct "+
		"--cluster nats://0.0.0.0:6222 --routes %s -l /tmp/nats.log >/dev/null 2>&1 &", i, routes))
}

// awaitReady waits until every node accepts a client and a three-replica stream can be made.
func (c *natsCluster) awaitReady() {
	c.t.Helper()
	require.Eventually(c.t, func() bool {
		nc, err := nats.Connect(c.URL(0), nats.MaxReconnects(0), nats.Timeout(2*time.Second))
		if err != nil {
			return false
		}
		defer nc.Close()
		js, err := nc.JetStream()
		if err != nil {
			return false
		}
		_, err = js.AddStream(&nats.StreamConfig{Name: "probe", Subjects: []string{"probe.>"}, Replicas: 3})
		if err != nil {
			return false
		}
		_ = js.DeleteStream("probe")
		return true
	}, 90*time.Second, time.Second, "the cluster never placed a 3-replica stream:\n%s", c.logs(0))
}

// URL is node i's client URL on the host (loopback only).
func (c *natsCluster) URL(i int) string { return fmt.Sprintf("nats://127.0.0.1:%d", c.ports[i]) }

// URLs lists every node's client URL, comma-separated, for a client that may fail over.
func (c *natsCluster) URLs() string {
	return strings.Join([]string{c.URL(0), c.URL(1), c.URL(2)}, ",")
}

// StopServer ends nats-server in node i; graceful sends SIGTERM, otherwise SIGKILL.
func (c *natsCluster) StopServer(i int, graceful bool) {
	c.t.Helper()
	sig := "KILL"
	if graceful {
		sig = "TERM"
	}
	c.sh(i, "kill -"+sig+" "+livePIDs)
	require.Eventually(c.t, func() bool {
		out, err := c.try("exec", c.names[i], "sh", "-c", "echo x"+livePIDs)
		return err == nil && out == "x"
	}, 30*time.Second, 100*time.Millisecond, "nats-server in node %d did not stop", i)
}

// StartServer starts nats-server in node i again, on the data it left.
func (c *natsCluster) StartServer(i int) {
	c.t.Helper()
	c.startServer(i, c.routes())
	require.Eventually(c.t, func() bool {
		nc, err := nats.Connect(c.URL(i), nats.MaxReconnects(0), nats.Timeout(time.Second))
		if err != nil {
			return false
		}
		nc.Close()
		return true
	}, 60*time.Second, 200*time.Millisecond, "nats-server in node %d did not come back:\n%s", i, c.logs(i))
}

// Freeze stops node i's server without closing a socket (SIGSTOP); Thaw resumes it.
func (c *natsCluster) Freeze(i int) { c.t.Helper(); c.sh(i, "kill -STOP "+livePIDs) }
func (c *natsCluster) Thaw(i int)   { c.t.Helper(); c.sh(i, "kill -CONT "+livePIDs) }

// Isolate cuts node i off from the other two, in both directions, leaving every process and
// its client port alive. Heal restores the routes.
func (c *natsCluster) Isolate(i int) { c.t.Helper(); c.cut(i, "add") }
func (c *natsCluster) Heal(i int)    { c.t.Helper(); c.cut(i, "del") }

func (c *natsCluster) cut(i int, verb string) {
	c.t.Helper()
	for j := range c.names {
		if j == i {
			continue
		}
		// Idempotent: a route that is already (not) there is not an error.
		c.sh(i, fmt.Sprintf("ip route %s blackhole %s/32 2>/dev/null || true", verb, c.ips[j]))
		c.sh(j, fmt.Sprintf("ip route %s blackhole %s/32 2>/dev/null || true", verb, c.ips[i]))
	}
}

// logs is the tail of node i's server log, for a failure message.
func (c *natsCluster) logs(i int) string {
	out, _ := c.try("exec", c.names[i], "tail", "-n", "30", "/tmp/nats.log")
	return out
}

// remove deletes the containers; it runs from t.Cleanup and tolerates ones already gone.
func (c *natsCluster) remove() {
	for _, name := range c.names {
		if name == "" {
			continue
		}
		if c.runtime == "docker" {
			_, _ = c.try("rm", "-f", name)
			continue
		}
		_, _ = c.try("kill", name)
		_, _ = c.try("delete", name)
	}
}

// LeadStream makes node i the Raft leader of the named stream (a key-value bucket "B" is the stream "KV_B"), by
// asking whichever node leads to step down until the election picks node i, which takes a few tries since the
// choice among the others is the cluster's. A test that isolates a node needs to say whether that node leads.
func (c *natsCluster) LeadStream(stream string, i int) {
	c.t.Helper()
	nc, err := nats.Connect(c.URLs())
	require.NoError(c.t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(c.t, err)
	want := fmt.Sprintf("n%d", i)
	for try := 0; try < 30; try++ {
		s, err := js.Stream(context.Background(), stream)
		require.NoError(c.t, err)
		info, err := s.Info(context.Background())
		require.NoError(c.t, err)
		if info.Cluster != nil && info.Cluster.Leader == want {
			return
		}
		_, _ = nc.Request("$JS.API.STREAM.LEADER.STEPDOWN."+stream, nil, 5*time.Second)
		time.Sleep(time.Second) // the election
	}
	c.t.Fatalf("node %d never became the leader of %s", i, stream)
}
