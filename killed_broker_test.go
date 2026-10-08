package natsmqtt5_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net"
	"os/exec"
	"path/filepath"
	"syscall"
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

// Tests that end a broker with a real SIGKILL. cmd/natsmqtt5 runs as a
// subprocess against the embedded NATS server (JetStream on), so nothing of it
// runs afterwards: no handler, no deferred release of the session record, no
// goodbye to NATS. A second broker in this process takes the session over.
//
// Statement wording, read from the OASIS MQTT 5.0 specification (os edition):
//
//	[MQTT-3.1.2-5]  "If a CONNECT packet is received with Clean Start set to 0 and
//	  there is a Session associated with the Client Identifier, the Server MUST
//	  resume communications with the Client based on state from the existing
//	  Session"
//	[MQTT-3.1.2-23] "The Client and Server MUST store the Session State after the
//	  Network Connection is closed if the Session Expiry Interval is greater
//	  than 0"
//	[MQTT-4.5.0-1]  "When a Server takes ownership of an incoming Application
//	  Message it MUST add it to the Session State for those Clients that have
//	  matching Subscriptions"
//	[MQTT-4.4.0-1]  "Clients and Servers MUST NOT resend messages at any other
//	  time" (than a reconnect with a session present, and then only the
//	  unacknowledged PUBLISH and PUBREL packets)

// killableBroker is a cmd/natsmqtt5 process.
type killableBroker struct {
	addr   string
	cmd    *exec.Cmd
	exited chan struct{}
}

// startBrokerBinary builds and starts cmd/natsmqtt5 against natsURL with the
// given extra flags, and waits until it accepts connections.
func startBrokerBinary(t *testing.T, natsURL string, flags ...string) *killableBroker {
	t.Helper()
	if testing.Short() {
		t.Skip("builds and runs the broker binary")
	}
	bin := filepath.Join(t.TempDir(), "natsmqtt5")
	if out, err := exec.Command("go", "build", "-o", bin, "./cmd/natsmqtt5").CombinedOutput(); err != nil {
		t.Fatalf("building cmd/natsmqtt5: %v\n%s", err, out)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())

	args := append([]string{"-nats", natsURL, "-listen", addr, "-log-level", "warn"}, flags...)
	k := &killableBroker{addr: addr, cmd: exec.Command(bin, args...), exited: make(chan struct{})}
	require.NoError(t, k.cmd.Start())
	go func() { _ = k.cmd.Wait(); close(k.exited) }()
	t.Cleanup(func() { _ = k.cmd.Process.Kill(); <-k.exited })

	require.Eventually(t, func() bool {
		c, err := net.Dial("tcp", addr)
		if err != nil {
			return false
		}
		_ = c.Close()
		return true
	}, 10*time.Second, 50*time.Millisecond, "the broker binary did not start listening")
	return k
}

// kill9 ends the process with SIGKILL and waits for it to be gone.
func (k *killableBroker) kill9(t *testing.T) {
	t.Helper()
	require.NoError(t, k.cmd.Process.Signal(syscall.SIGKILL))
	<-k.exited
}

// storedSession reads a session's record from the session bucket.
func storedSession(t *testing.T, natsURL, clientID string) map[string]any {
	t.Helper()
	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	kv, err := js.KeyValue(context.Background(), natsmqtt5.DefaultStreamPrefix+"_sessions")
	require.NoError(t, err)
	entry, err := kv.Get(context.Background(), base64.RawURLEncoding.EncodeToString([]byte(clientID)))
	require.NoError(t, err)
	var rec map[string]any
	require.NoError(t, json.Unmarshal(entry.Value(), &rec))
	return rec
}

// receiveQoS2 reads one QoS 2 PUBLISH and completes the exchange.
func receiveQoS2(t *testing.T, c *rawClient, wantTopic string) {
	t.Helper()
	p := c.expectPublish()
	require.Equal(t, wantTopic, p.Topic)
	require.Equal(t, packet.QoS2, p.QoS)
	c.send(&packet.Pubrec{Ack: packet.Ack{PacketID: p.PacketID}})
	rel := c.expectPubrel()
	require.Equal(t, p.PacketID, rel.PacketID)
	c.send(&packet.Pubcomp{Ack: packet.Ack{PacketID: p.PacketID}})
}

const killedBrokerCheckpoint = "100ms"

// [MQTT-4.4.0-1], [MQTT-3.1.2-5], [MQTT-3.1.2-23], [MQTT-4.5.0-1]: a client that
// had been sent two QoS 2 messages and had completed both exchanges when its
// broker was killed is, on the broker that takes the session over, sent only
// what was published after that: the two are not delivered a second time, and
// the one published while no broker served the client is not lost. Before the
// connection wrote its position, the successor replayed everything the queue
// held (up to OfflineQueueMaxAge).
func TestAKilledBrokersClientIsNotSentWhatItAlreadyGotAgain(t *testing.T) {
	natsURL := startNATS(t)
	child := startBrokerBinary(t, natsURL, "-persistent-sessions", "-session-checkpoint-interval", killedBrokerCheckpoint)
	survivor := startBroker(t, natsURL, persistentWithQueue)

	victim := dialRaw(t, child.addr)
	require.False(t, victim.connect(rawConnect("killed-caught-up", 300)).SessionPresent)
	victim.subscribe("kc/#", packet.QoS2)

	pub, _ := connectClient(t, survivor, connectOpts("kc-pub"))
	for _, topic := range []string{"kc/1", "kc/2"} {
		pub.publish(&paho.Publish{Topic: topic, QoS: 2, Payload: []byte(topic)})
		receiveQoS2(t, victim, topic)
	}

	// Several checkpoint intervals: the connection has written a position after
	// the last exchange finished.
	time.Sleep(700 * time.Millisecond)
	rec := storedSession(t, natsURL, "killed-caught-up")
	require.NotEmpty(t, rec["AwayAt"], "the broker did not checkpoint the session")
	require.Equal(t, true, rec["Attached"], "the record is still the live connection's")

	child.kill9(t)
	pub.publish(&paho.Publish{Topic: "kc/3", QoS: 2, Payload: []byte("kc/3")})

	back := dialRaw(t, survivor)
	require.True(t, back.connect(rawConnect("killed-caught-up", 300)).SessionPresent, "[MQTT-3.1.2-5]")
	receiveQoS2(t, back, "kc/3")
	back.expectNothing()
}

// [MQTT-4.5.0-1], [MQTT-3.1.2-23]: a client that was behind when its broker
// was killed, with a message sent and unacknowledged and more waiting, gets the
// ones it had not acknowledged and the rest, and not the ones it had finished.
// The unacknowledged one is a resend after the reconnect, which
// [MQTT-4.4.0-1] allows.
func TestAKilledBrokersBehindClientGetsWhatItHadNotAcknowledged(t *testing.T) {
	natsURL := startNATS(t)
	child := startBrokerBinary(t, natsURL, "-persistent-sessions", "-session-checkpoint-interval", killedBrokerCheckpoint)
	survivor := startBroker(t, natsURL, persistentWithQueue)

	victim := dialRaw(t, child.addr)
	victim.connect(rawConnect("killed-behind", 300))
	victim.subscribe("kb/#", packet.QoS1)

	pub, _ := connectClient(t, survivor, connectOpts("kb-pub"))
	pub.publish(&paho.Publish{Topic: "kb/1", QoS: 1, Payload: []byte("1")})
	first := victim.expectPublish()
	require.Equal(t, "kb/1", first.Topic)
	victim.send(&packet.Puback{Ack: packet.Ack{PacketID: first.PacketID}})

	// Sent and not acknowledged.
	pub.publish(&paho.Publish{Topic: "kb/2", QoS: 1, Payload: []byte("2")})
	second := victim.expectPublish()
	require.Equal(t, "kb/2", second.Topic)
	time.Sleep(1200 * time.Millisecond) // several checkpoints with kb/2 in flight

	child.kill9(t)
	pub.publish(&paho.Publish{Topic: "kb/3", QoS: 1, Payload: []byte("3")})

	back := dialRaw(t, survivor)
	require.True(t, back.connect(rawConnect("killed-behind", 300)).SessionPresent)
	var got []string
	for range 2 {
		p := back.expectPublish()
		got = append(got, p.Topic)
		back.send(&packet.Puback{Ack: packet.Ack{PacketID: p.PacketID}})
	}
	assert.Equal(t, []string{"kb/2", "kb/3"}, got)
	back.expectNothing()
}
