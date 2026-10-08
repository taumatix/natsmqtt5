package natsmqtt5_test

import (
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// Whose Will is whose when a client reconnects while its old connection is
// still ending. The wording is from MQTT-5.0 §3.1.2.5 and §3.1.3.2, read from
// the OASIS text on 2026-10-08:
//
//	[MQTT-3.1.2-8]  "The Will Message MUST be published after the Network
//	                 Connection is subsequently closed and either the Will Delay
//	                 Interval has elapsed or the Session ends, unless the Will
//	                 Message has been deleted by the Server on receipt of a
//	                 DISCONNECT packet with Reason Code 0x00 (Normal
//	                 disconnection) or a new Network Connection for the ClientID
//	                 is opened before the Will Delay Interval has elapsed."
//	[MQTT-3.1.2-10] "The Will Message MUST be removed from the stored Session
//	                 State in the Server once it has been published or the
//	                 Server has received a DISCONNECT packet with a Reason Code
//	                 of 0x00 (Normal disconnection) from the Client."
//	[MQTT-3.1.3-9]  "If a new Network Connection to this Session is made before
//	                 the Will Delay Interval has passed, the Server MUST NOT send
//	                 the Will Message."
//
// A client whose new connection is open is live: neither the Will of the
// connection it replaced nor its own is published, and the one it set is the
// one that goes out when it ends.

func rawConnectWithWillMessage(id, willTopic, payload string) *packet.Connect {
	cp := rawConnect(id, 300)
	cp.Will = &packet.Will{Topic: willTopic, Payload: []byte(payload), QoS: packet.QoS1}
	return cp
}

func newWillWatcher(t *testing.T, addr string) *testClient {
	t.Helper()
	w, _ := connectClient(t, addr, connectOpts("watcher"))
	w.subscribe(paho.SubscribeOptions{Topic: "status/#", QoS: 1})
	return w
}

// The old connection's end must not publish or cancel the new connection's Will.
func TestMQTT_3_1_2_8_TheOldConnectionsEndLeavesTheNewConnectionsWill(t *testing.T) {
	addr := startBroker(t, startNATS(t))
	watcher := newWillWatcher(t, addr)

	old := dialRaw(t, addr)
	old.connect(rawConnectWithWillMessage("swap", "status/swap", "old"))

	neu := dialRaw(t, addr)
	neu.connect(rawConnectWithWillMessage("swap", "status/swap", "new"))
	old.expectDisconnect(0x8E) // Session taken over

	watcher.expectNoMessageFor(1500 * time.Millisecond) // [MQTT-3.1.3-9]: the client is live

	neu.drop()
	got := watcher.expectMessage()
	assert.Equal(t, "new", got.Payload, "[MQTT-3.1.2-8] the Will of the connection that ended")
	watcher.expectNoMessageFor(time.Second)
}

// A new connection with no Will of its own leaves no Will behind, and the old
// connection's end publishes none.
func TestMQTT_3_1_2_8_ANewConnectionWithoutAWillCancelsTheOldOne(t *testing.T) {
	addr := startBroker(t, startNATS(t))
	watcher := newWillWatcher(t, addr)

	old := dialRaw(t, addr)
	old.connect(rawConnectWithWillMessage("swap2", "status/swap2", "old"))
	neu := dialRaw(t, addr)
	neu.connect(rawConnect("swap2", 300))
	old.expectDisconnect(0x8E)

	watcher.expectNoMessageFor(1500 * time.Millisecond)
	neu.drop()
	watcher.expectNoMessageFor(1500 * time.Millisecond) // it had no Will
}

// [MQTT-3.1.2-10]: replacing a Will removes the stored one, and a broker that
// dies afterwards leaves exactly one for a survivor to publish.
func TestMQTT_3_1_2_10_AReplacedWillLeavesNoStoredRecordBehind(t *testing.T) {
	url := startNATS(t)
	a, addrA, _ := startBrokerHandle(t, url, fastWillsWithSessions)
	addrB := startBroker(t, url, fastWillsWithSessions)
	watcher := newWillWatcher(t, addrB)

	first := dialRaw(t, addrA)
	first.connect(rawConnectWithWillMessage("replace", "status/replace", "first"))
	second := dialRaw(t, addrA)
	second.connect(rawConnectWithWillMessage("replace", "status/replace", "second"))
	first.expectDisconnect(0x8E)
	time.Sleep(300 * time.Millisecond) // the old connection's end has run

	assert.Equal(t, 1, willKeys(t, url), "[MQTT-3.1.2-10] one stored Will for one live connection")

	natsmqtt5.Kill(a)
	got := watcher.expectMessage()
	assert.Equal(t, "second", got.Payload)
	watcher.expectNoMessageFor(2 * time.Second) // "first" is not published by the survivor
}

// A clean start ends the old session, and "the Session ends" is one of the two
// moments [MQTT-3.1.2-8] publishes the Will, so the displaced connection's Will
// goes out and the new connection's does not.
func TestMQTT_3_1_2_8_ACleanStartTakeoverPublishesTheOldWillOnly(t *testing.T) {
	addr := startBroker(t, startNATS(t))
	watcher := newWillWatcher(t, addr)

	old := dialRaw(t, addr)
	old.connect(rawConnectWithWillMessage("swap3", "status/swap3", "old"))
	neu := dialRaw(t, addr)
	cp := rawConnectWithWillMessage("swap3", "status/swap3", "new")
	cp.CleanStart = true
	neu.connect(cp)
	old.expectDisconnect(0x8E)

	got := watcher.expectMessage()
	assert.Equal(t, "old", got.Payload)
	watcher.expectNoMessageFor(1500 * time.Millisecond)
}
