package natsmqtt5_test

import (
	"bufio"
	"context"
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

// Durable Will Messages: the Will of a broker that was killed outright.
//
// Each test runs brokers over one real NATS server with JetStream and talks to
// them over TCP with Paho. A broker is killed with natsmqtt5.Kill, which closes
// its NATS connection and sockets without running anything graceful.
//
// The statements, with their wording from MQTT-5.0 §3.1.2.5. The identifier
// follows the sentence it numbers, which is not the order this repository's
// earlier notes had them in:
//
//	[MQTT-3.1.2-7]  "If the Will Flag is set to 1 this indicates that a Will
//	                 Message MUST be stored on the Server and associated with
//	                 the Session."
//	[MQTT-3.1.2-8]  "The Will Message MUST be published after the Network
//	                 Connection is subsequently closed and either the Will Delay
//	                 Interval has elapsed or the Session ends, unless the Will
//	                 Message has been deleted by the Server on receipt of a
//	                 DISCONNECT packet with Reason Code 0x00 ... or a new Network
//	                 Connection for the ClientID is opened before the Will Delay
//	                 Interval has elapsed."
//	[MQTT-3.1.2-10] "The Will Message MUST be removed from the stored Session
//	                 State in the Server once it has been published or the Server
//	                 has received a DISCONNECT packet with a Reason Code of 0x00."
//	[MQTT-3.1.3-9]  "If a new Network Connection to this Session is made before
//	                 the Will Delay Interval has passed, the Server MUST NOT send
//	                 the Will Message."
//
// The wording is quoted from memory of the OASIS text and not re-checked
// against it in this session.

// fastWills makes a broker look for orphaned Wills every 150ms instead of 5s.
func fastWills(o *natsmqtt5.Options) {
	o.DurableWills = true
	o.WillCheckInterval = 150 * time.Millisecond
}

func fastWillsWithSessions(o *natsmqtt5.Options) {
	fastWills(o)
	o.PersistentSessions = true
}

func durableWillConnect(id, topicName, payload string, delaySeconds uint32, expiry uint32) *paho.Connect {
	cp := &paho.Connect{
		ClientID:    id,
		CleanStart:  false,
		KeepAlive:   30,
		WillMessage: &paho.WillMessage{Topic: topicName, Payload: []byte(payload), QoS: 1},
	}
	if delaySeconds > 0 {
		cp.WillProperties = &paho.WillProperties{WillDelayInterval: natsmqtt5.Ptr(delaySeconds)}
	}
	if expiry > 0 {
		cp.Properties = &paho.ConnectProperties{SessionExpiryInterval: natsmqtt5.Ptr(expiry)}
	}
	return cp
}

// expectNoMessageFor is expectNoMessage with a window the caller chooses: the
// Will check runs on a timer, so a Will that should not appear needs several
// ticks to have had its chance.
func (c *testClient) expectNoMessageFor(d time.Duration) {
	c.t.Helper()
	select {
	case m := <-c.messages:
		c.t.Fatalf("expected no message, got %q on %q", m.Payload, m.Topic)
	case <-time.After(d):
	}
}

// [MQTT-3.1.2-8]: the Will of a client whose broker was killed is published,
// by a broker that survived, with the Will's own topic, payload, QoS and
// properties.
func TestMQTT_3_1_2_8_WillOfAKilledBrokerIsPublishedByASurvivor(t *testing.T) {
	url := startNATS(t)
	a, addrA, _ := startBrokerHandle(t, url, fastWills)
	addrB := startBroker(t, url, fastWills)

	watcher, _ := connectClient(t, addrB, connectOpts("watcher"))
	watcher.subscribe(paho.SubscribeOptions{Topic: "status/#", QoS: 1})

	// Paho v0.23 packs the Will Properties as CONNECT properties and so drops
	// Content Type and Message Expiry Interval before they reach the wire;
	// this test writes the CONNECT itself.
	raw, err := net.Dial("tcp", addrA)
	require.NoError(t, err)
	t.Cleanup(func() { _ = raw.Close() })
	b, err := packet.Encode(&packet.Connect{
		ClientID: "victim", ProtocolVersion: 5, CleanStart: true, KeepAlive: 30,
		Properties: &packet.Properties{},
		Will: &packet.Will{
			Topic: "status/victim", Payload: []byte("offline"), QoS: packet.QoS1,
			Properties: &packet.Properties{
				ContentType:           "text/plain",
				MessageExpiryInterval: packet.Uint32(60),
			},
		},
	})
	require.NoError(t, err)
	_, err = raw.Write(b)
	require.NoError(t, err)
	_, err = packet.Read(bufio.NewReader(raw), 1<<20)
	require.NoError(t, err)

	natsmqtt5.Kill(a)

	got := watcher.expectMessage()
	assert.Equal(t, "status/victim", got.Topic)
	assert.Equal(t, "offline", got.Payload)
	assert.Equal(t, byte(1), got.QoS)
	require.NotNil(t, got.Properties)
	assert.Equal(t, "text/plain", got.Properties.ContentType)
	// Message Expiry Interval travels with the Will and counts from its
	// publication (MQTT-5.0 §3.1.3.2.2 and §3.3.2.3.3): never above what was set.
	require.NotNil(t, got.Properties.MessageExpiry)
	assert.LessOrEqual(t, *got.Properties.MessageExpiry, uint32(60))
	assert.Greater(t, *got.Properties.MessageExpiry, uint32(0))
	watcher.expectNoMessageFor(time.Second)
}

// [MQTT-3.1.2-8] negative half: a Will is published only after the connection
// is closed. A connection that is open on a live broker keeps its Will, however
// many times other brokers look.
func TestMQTT_3_1_2_8_LiveConnectionKeepsItsWill(t *testing.T) {
	url := startNATS(t)
	startBroker(t, url, fastWills)
	addrB := startBroker(t, url, fastWills)
	_, addrA, _ := startBrokerHandle(t, url, fastWills)

	watcher, _ := connectClient(t, addrB, connectOpts("watcher"))
	watcher.subscribe(paho.SubscribeOptions{Topic: "status/#", QoS: 1})
	cp := durableWillConnect("alive", "status/alive", "offline", 0, 0)
	cp.CleanStart = true
	live, _ := connectClient(t, addrA, cp)

	watcher.expectNoMessageFor(1500 * time.Millisecond)

	// And it is still the broker's own Will to publish when the connection does
	// end, once and only once.
	require.NoError(t, live.Client.Disconnect(&paho.Disconnect{ReasonCode: 0x04}))
	assert.Equal(t, "offline", watcher.expectMessage().Payload)
	watcher.expectNoMessageFor(time.Second)
}

// [MQTT-3.1.2-9] and [MQTT-3.1.2-10]: a DISCONNECT with Reason Code 0x00
// deletes the Will, and it stays deleted even when the broker is then killed.
func TestMQTT_3_1_2_10_NormalDisconnectLeavesNothingForASurvivor(t *testing.T) {
	url := startNATS(t)
	a, addrA, _ := startBrokerHandle(t, url, fastWills)
	addrB := startBroker(t, url, fastWills)

	watcher, _ := connectClient(t, addrB, connectOpts("watcher"))
	watcher.subscribe(paho.SubscribeOptions{Topic: "status/#", QoS: 1})
	cp := durableWillConnect("leaving", "status/leaving", "offline", 0, 0)
	cp.CleanStart = true
	leaving, _ := connectClient(t, addrA, cp)

	require.NoError(t, leaving.Client.Disconnect(&paho.Disconnect{ReasonCode: 0}))
	// Wait until the broker has processed the DISCONNECT and removed the
	// stored Will; killing it earlier would be a different test.
	require.Eventually(t, func() bool { return willKeys(t, url) == 0 }, 5*time.Second, 20*time.Millisecond)
	natsmqtt5.Kill(a)

	watcher.expectNoMessageFor(1500 * time.Millisecond)
}

// [MQTT-3.1.2-10]: once published the Will is removed, so a second survivor
// does not publish it again. Three brokers, one dead, two survivors racing.
func TestMQTT_3_1_2_10_WillIsPublishedOnceAcrossBrokers(t *testing.T) {
	url := startNATS(t)
	a, addrA, _ := startBrokerHandle(t, url, fastWills)
	startBroker(t, url, fastWills)
	addrC := startBroker(t, url, fastWills)

	watcher, _ := connectClient(t, addrC, connectOpts("watcher"))
	watcher.subscribe(paho.SubscribeOptions{Topic: "status/#", QoS: 1})
	for _, id := range []string{"one", "two", "three"} {
		cp := durableWillConnect(id, "status/"+id, "offline-"+id, 0, 0)
		cp.CleanStart = true
		connectClient(t, addrA, cp)
	}

	natsmqtt5.Kill(a)

	seen := map[string]int{}
	deadline := time.After(4 * time.Second)
	for len(seen) < 3 {
		select {
		case m := <-watcher.messages:
			seen[m.Payload]++
		case <-deadline:
			t.Fatalf("only %v were published", seen)
		}
	}
	// Further ticks of both survivors must find nothing left.
	watcher.expectNoMessageFor(1500 * time.Millisecond)
	assert.Equal(t, map[string]int{"offline-one": 1, "offline-two": 1, "offline-three": 1}, seen)
}

// [MQTT-3.1.2-8]: the Will Delay Interval is honoured across a broker's death.
// The Will is not published before the delay, and is after it.
func TestMQTT_3_1_2_8_WillDelaySurvivesTheBrokerDying(t *testing.T) {
	url := startNATS(t)
	a, addrA, _ := startBrokerHandle(t, url, fastWillsWithSessions)
	addrB := startBroker(t, url, fastWillsWithSessions)

	watcher, _ := connectClient(t, addrB, connectOpts("watcher"))
	watcher.subscribe(paho.SubscribeOptions{Topic: "status/#", QoS: 1})
	connectClient(t, addrA, durableWillConnect("delayed", "status/delayed", "late", 2, 300))

	killed := time.Now()
	natsmqtt5.Kill(a)

	watcher.expectNoMessageFor(1200 * time.Millisecond)
	got := watcher.expectMessage()
	assert.Equal(t, "late", got.Payload)
	assert.GreaterOrEqual(t, time.Since(killed), 2*time.Second, "published before the Will Delay Interval of 2s")
	watcher.expectNoMessageFor(time.Second)
}

// [MQTT-3.1.3-9]: a new Network Connection to the Session, made on another
// broker before the delay has passed, cancels the Will of the dead one.
func TestMQTT_3_1_3_9_ReconnectCancelsTheWillOfAKilledBroker(t *testing.T) {
	url := startNATS(t)
	a, addrA, _ := startBrokerHandle(t, url, fastWillsWithSessions)
	addrB := startBroker(t, url, fastWillsWithSessions)

	watcher, _ := connectClient(t, addrB, connectOpts("watcher"))
	watcher.subscribe(paho.SubscribeOptions{Topic: "status/#", QoS: 1})
	connectClient(t, addrA, durableWillConnect("comeback", "status/comeback", "late", 3, 300))

	natsmqtt5.Kill(a)
	// Long enough for B to have noticed and adopted the Will.
	time.Sleep(700 * time.Millisecond)

	_, ack := connectClient(t, addrB, &paho.Connect{
		ClientID: "comeback", CleanStart: false, KeepAlive: 30,
		Properties: &paho.ConnectProperties{SessionExpiryInterval: natsmqtt5.Ptr(uint32(300))},
	})
	require.True(t, ack.SessionPresent, "the session must have been resumed for this to be a resumption")

	watcher.expectNoMessageFor(4 * time.Second)
}

// [MQTT-3.1.2-8]: "or the Session ends". A client of a dead broker that comes
// back asking for a clean start has ended the session, so its Will goes out
// now rather than after the delay.
func TestMQTT_3_1_2_8_CleanStartAfterAKillEndsTheSessionAndPublishes(t *testing.T) {
	url := startNATS(t)
	a, addrA, _ := startBrokerHandle(t, url, fastWillsWithSessions)
	addrB := startBroker(t, url, fastWillsWithSessions)

	watcher, _ := connectClient(t, addrB, connectOpts("watcher"))
	watcher.subscribe(paho.SubscribeOptions{Topic: "status/#", QoS: 1})
	connectClient(t, addrA, durableWillConnect("restart", "status/restart", "ended", 60, 300))

	natsmqtt5.Kill(a)

	connectClient(t, addrB, &paho.Connect{ClientID: "restart", CleanStart: true, KeepAlive: 30})
	got := watcher.expectMessage()
	assert.Equal(t, "ended", got.Payload)
	watcher.expectNoMessageFor(time.Second)
}

// willKeys counts the Will records in the default broker bucket.
func willKeys(t *testing.T, natsURL string) int {
	t.Helper()
	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	kv, err := js.KeyValue(context.Background(), "MQTT5_wills")
	require.NoError(t, err)
	keys, err := kv.Keys(context.Background())
	if err != nil {
		return 0 // no keys
	}
	return len(keys)
}

// [MQTT-3.1.2-7], [MQTT-3.1.2-8]: the real thing. cmd/natsmqtt5 runs as a
// subprocess against the embedded NATS server and is sent SIGKILL, so nothing
// of it runs afterwards: no handler, no deferred close, no goodbye to NATS.
// A broker in this process publishes the Will it left behind.
func TestMQTT_3_1_2_8_KillNineOfTheBrokerBinaryPublishesItsWill(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs the broker binary")
	}
	url := startNATS(t)

	bin := filepath.Join(t.TempDir(), "natsmqtt5")
	build := exec.Command("go", "build", "-o", bin, "./cmd/natsmqtt5")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building cmd/natsmqtt5: %v\n%s", err, out)
	}

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addrChild := l.Addr().String()
	require.NoError(t, l.Close())

	child := exec.Command(bin, "-nats", url, "-listen", addrChild,
		"-durable-wills", "-will-check-interval", "150ms", "-log-level", "warn")
	require.NoError(t, child.Start())
	exited := make(chan struct{})
	go func() { _ = child.Wait(); close(exited) }()
	t.Cleanup(func() { _ = child.Process.Kill(); <-exited })

	require.Eventually(t, func() bool {
		c, err := net.Dial("tcp", addrChild)
		if err != nil {
			return false
		}
		_ = c.Close()
		return true
	}, 10*time.Second, 50*time.Millisecond, "the broker binary did not start listening")

	addrB := startBroker(t, url, fastWills)
	watcher, _ := connectClient(t, addrB, connectOpts("watcher"))
	watcher.subscribe(paho.SubscribeOptions{Topic: "status/#", QoS: 1})

	cp := durableWillConnect("victim", "status/victim", "offline", 0, 0)
	cp.CleanStart = true
	connectClient(t, addrChild, cp)

	require.NoError(t, child.Process.Signal(syscall.SIGKILL))
	<-exited

	got := watcher.expectMessage()
	assert.Equal(t, "status/victim", got.Topic)
	assert.Equal(t, "offline", got.Payload)
	watcher.expectNoMessageFor(time.Second)
}
