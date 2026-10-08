package natsmqtt5_test

import (
	"sync/atomic"
	"testing"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// A Packet Identifier the broker took back on resume (the filter that earned it
// was denied) stays unavailable while the client may still acknowledge it, so
// the acknowledgement cannot complete a different message: a Server assigns a
// new QoS > 0 PUBLISH a non-zero Packet Identifier that is currently unused
// [MQTT-2.2.1-4] (CONFORMANCE.md row; the specification's wording is not in this
// repository, so it is not quoted). It is not unavailable forever: a client that
// has been through two more CONNECTs without sending the acknowledgement is not
// going to, and a session narrowed again and again would otherwise run out of
// identifiers (nextID false, the connection closed on every reconnect).
//
// Identifiers count up, so reuse is only observable after a wrap; the test sets
// the allocator just below the withdrawn identifier instead of sending 65535
// messages, and says so in SetNextPacketID.
func TestAWithdrawnPacketIdentifierIsReusableAfterTheSecondResumption_MQTT_2_2_1_4(t *testing.T) {
	// The second variant keeps the session record in JetStream. The withdrawn set
	// is not part of the record, so the record must not change what is forgotten.
	t.Run("in memory", func(t *testing.T) { withdrawnIdentifierIsReusable(t, nil) })
	t.Run("persistent sessions", func(t *testing.T) { withdrawnIdentifierIsReusable(t, persistent) })
}

func withdrawnIdentifierIsReusable(t *testing.T, customise func(*natsmqtt5.Options)) {
	var deny atomic.Bool
	natsURL := startNATS(t)
	opts := []func(*natsmqtt5.Options){narrowingAuthorizer(&deny)}
	if customise != nil {
		opts = append(opts, customise)
	}
	broker, addr, _ := startBrokerHandle(t, natsURL, opts...)
	const id = "withdrawn-expiry"

	// Connection 1: receives a message on secret/# and drops without acknowledging.
	c1 := dialRaw(t, addr)
	require.False(t, c1.connect(rawConnect(id, 300)).SessionPresent)
	c1.subscribe("secret/#", packet.QoS1)
	c1.subscribe("public/#", packet.QoS1)
	pub, _ := connectClient(t, addr, connectOpts("withdrawn-expiry-pub"))
	pub.publish(&paho.Publish{Topic: "secret/a", QoS: 1, Payload: []byte("classified")})
	owed := c1.expectPublish().PacketID
	c1.drop()

	// Connection 2: secret/# is denied, so the message is withdrawn; the client
	// does not acknowledge it.
	deny.Store(true)
	c2 := dialRaw(t, addr)
	require.True(t, c2.connect(rawConnect(id, 300)).SessionPresent)
	c2.expectNothing()
	c2.drop()
	deny.Store(false)

	// allocated publishes to the session with its allocator just below owed and
	// returns the identifier the broker chose.
	allocated := func(c *rawClient) uint16 {
		natsmqtt5.SetNextPacketID(broker, id, owed-1)
		pub.publish(&paho.Publish{Topic: "public/a", QoS: 1, Payload: []byte("p")})
		p := c.expectPublish()
		c.send(&packet.Puback{Ack: packet.Ack{PacketID: p.PacketID}})
		return p.PacketID
	}

	// Connection 3, the first resumption after the withdrawal: still owed, so
	// the identifier is skipped.
	c3 := dialRaw(t, addr)
	require.True(t, c3.connect(rawConnect(id, 300)).SessionPresent)
	assert.Equal(t, owed+1, allocated(c3), "the withdrawn identifier must not be handed out yet")
	c3.drop()

	// Connection 4, the second: the client has had two chances to acknowledge
	// and the identifier is free again.
	c4 := dialRaw(t, addr)
	require.True(t, c4.connect(rawConnect(id, 300)).SessionPresent)
	assert.Equal(t, owed, allocated(c4), "the withdrawn identifier must be reusable after the second resumption")
}
