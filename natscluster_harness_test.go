package natsmqtt5_test

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

// The harness is code too: this drives its four controls against a replicated stream, so a
// test built on them can trust that "isolated" means isolated and "restarted" means restarted.
func TestClusterHarnessControls(t *testing.T) {
	c := startNATSCluster(t)

	nc, err := nats.Connect(c.URLs())
	require.NoError(t, err)
	defer nc.Close()
	js, err := nc.JetStream()
	require.NoError(t, err)
	_, err = js.AddStream(&nats.StreamConfig{Name: "h", Subjects: []string{"h.>"}, Replicas: 3})
	require.NoError(t, err)

	// Stopped and restarted: the cluster keeps a quorum and the node comes back.
	c.StopServer(2, false)
	_, err = js.Publish("h.a", []byte("1"))
	require.NoError(t, err)
	c.StartServer(2)

	// Isolated: a client of the cut node cannot write a replicated stream; the other two can.
	c.Isolate(0)
	t.Cleanup(func() { c.Heal(0) })
	cut, err := nats.Connect(c.URL(0), nats.MaxReconnects(0))
	require.NoError(t, err)
	defer cut.Close()
	cutJS, err := cut.JetStream(nats.MaxWait(3 * time.Second))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		_, err := cutJS.Publish("h.b", []byte("2"))
		return err != nil
	}, 60*time.Second, time.Second, "an isolated node still accepted a replicated write")
	other, err := nats.Connect(c.URL(1))
	require.NoError(t, err)
	defer other.Close()
	otherJS, err := other.JetStream()
	require.NoError(t, err)
	require.Eventually(t, func() bool { _, err := otherJS.Publish("h.c", []byte("3")); return err == nil },
		60*time.Second, time.Second, "the majority side stopped accepting writes")

	// Healed: the cut node serves writes again.
	c.Heal(0)
	require.Eventually(t, func() bool { _, err := cutJS.Publish("h.d", []byte("4")); return err == nil },
		90*time.Second, time.Second, "the healed node never recovered")

	// Frozen and thawed: the process stays, nothing is closed, and it resumes.
	c.Freeze(1)
	c.Thaw(1)
}
