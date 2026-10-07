package natsmqtt5_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
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

// A client that was behind when its connection ended is owed what was waiting
// for it, which was published before the connection ended. A session restored on
// another broker used to replay from the moment of release, so it lost them; the
// record now carries the lowest queue sequence the client had not been sent.
func TestARestoredSessionReplaysFromWhatItWasOwedNotFromTheDisconnect(t *testing.T) {
	natsURL := startNATS(t)
	addrA, stopA := startStoppableBroker(t, natsURL, persistentWithQueue)
	addrB := startBroker(t, natsURL, persistentWithQueue)

	c := dialRaw(t, addrA)
	cp := rawConnect("rewind-moving", 300)
	cp.Properties.ReceiveMaximum = natsmqtt5.Ptr(uint16(1))
	c.connect(cp)
	c.subscribe("w/#", packet.QoS1)

	pub, _ := connectClient(t, addrA, connectOpts("pub-rewind-moving"))
	for _, n := range []string{"1", "2", "3", "4"} {
		pub.publish(&paho.Publish{Topic: "w/" + n, QoS: 1, Payload: []byte(n)})
	}
	first := c.expectPublish()
	require.Equal(t, "w/1", first.Topic)
	c.send(&packet.Puback{Ack: packet.Ack{PacketID: first.PacketID}})
	// w/2 is now in flight and unacknowledged; w/3 and w/4 wait behind the quota.
	require.Equal(t, "w/2", c.expectPublish().Topic)
	time.Sleep(100 * time.Millisecond)
	c.drop()
	time.Sleep(100 * time.Millisecond)

	// Broker A goes away too, so everything B knows comes from the record.
	stopA()

	back := dialRaw(t, addrB)
	require.True(t, back.connect(rawConnect("rewind-moving", 300)).SessionPresent)
	// w/2 was in flight, which a restored session does not carry ("Retransmission
	// that survives a broker restart" in ROADMAP.md); w/3 and w/4 were only
	// waiting, and are what the record now brings back, once each.
	for _, want := range []string{"w/3", "w/4"} {
		got := back.expectPublish()
		assert.Equal(t, want, got.Topic)
		back.send(&packet.Puback{Ack: packet.Ack{PacketID: got.PacketID}})
	}
	back.expectNothing()
}

// A broker killed outright records no release, so the record still says
// Attached with no AwayAt. Its session is resumed elsewhere from the start of
// what the queue holds, which may repeat messages the dead connection delivered
// but loses none. The killed broker is simulated by rewriting the record of a
// released session into what a killed broker leaves, since the harness has no
// way to end a broker without its cleanup.
func TestASessionLeftAttachedByAKilledBrokerIsReplayedFromTheQueue(t *testing.T) {
	natsURL := startNATS(t)
	addrA, stopA := startStoppableBroker(t, natsURL, persistentWithQueue)

	c := dialRaw(t, addrA)
	c.connect(rawConnect("rewind-killed", 300))
	c.subscribe("k/#", packet.QoS1)
	c.send(&packet.Disconnect{})
	time.Sleep(100 * time.Millisecond)

	pub, _ := connectClient(t, addrA, connectOpts("pub-rewind-killed"))
	pub.publish(&paho.Publish{Topic: "k/1", QoS: 1, Payload: []byte("1")})
	pub.publish(&paho.Publish{Topic: "k/2", QoS: 1, Payload: []byte("2")})
	stopA()

	leaveAsAKilledBrokerWould(t, natsURL, "rewind-killed")

	addrB := startBroker(t, natsURL, persistentWithQueue)
	back := dialRaw(t, addrB)
	require.True(t, back.connect(rawConnect("rewind-killed", 300)).SessionPresent)
	for _, want := range []string{"k/1", "k/2"} {
		got := back.expectPublish()
		assert.Equal(t, want, got.Topic)
		back.send(&packet.Puback{Ack: packet.Ack{PacketID: got.PacketID}})
	}
	back.expectNothing()
}

// leaveAsAKilledBrokerWould rewrites a released session's record into the one a
// broker killed while serving it leaves: attached, owned by an instance that
// answers nothing, with no release time and no replay position.
func leaveAsAKilledBrokerWould(t *testing.T, natsURL, clientID string) {
	t.Helper()
	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	ctx := context.Background()
	kv, err := js.KeyValue(ctx, natsmqtt5.DefaultStreamPrefix+"_sessions")
	require.NoError(t, err)
	key := base64.RawURLEncoding.EncodeToString([]byte(clientID))
	entry, err := kv.Get(ctx, key)
	require.NoError(t, err)
	var rec map[string]any
	require.NoError(t, json.Unmarshal(entry.Value(), &rec))
	rec["Attached"] = true
	rec["Owner"] = "a-broker-that-was-killed"
	delete(rec, "AwayAt")
	delete(rec, "AwayFromSeq")
	delete(rec, "Delivered")
	delete(rec, "ExpiresAt")
	b, err := json.Marshal(rec)
	require.NoError(t, err)
	_, err = kv.Update(ctx, key, b, entry.Revision())
	require.NoError(t, err)
}
