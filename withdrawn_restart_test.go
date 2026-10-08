package natsmqtt5_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// A Packet Identifier the broker took back on resume is still owed an
// acknowledgement, and that has to survive the broker that took it back being
// replaced: the session record carries the withdrawn set (sessionRecord.Withdrawn).
// Three brokers share one NATS server and JetStream, one after another, the
// first two stopped cleanly:
//
//	A  the client is sent a QoS 1 message on secret/# and drops without acknowledging;
//	B  secret/# is denied, so the message is withdrawn; the client drops again;
//	C  the client's PUBACK for the withdrawn identifier must be ignored, not
//	   answered with 0x82 [MQTT-4.4.0-1], and the identifier must not be handed to a new
//	   message while it is owed [MQTT-2.2.1-4].
//
// Both statements are quoted by CONFORMANCE.md rows rather than here: the
// specification's wording is not in this repository.
func TestAWithdrawnIdentifierSurvivesABrokerRestart_MQTT_4_4_0_1_MQTT_2_2_1_4(t *testing.T) {
	const id = "withdrawn-restart"

	// withdrawnAcrossTwoBrokers leaves the session record as broker B released
	// it, with owed the withdrawn identifier, and returns the shared NATS URL.
	withdrawnAcrossTwoBrokers := func(t *testing.T, deny *atomic.Bool, kill bool) (natsURL string, owed uint16) {
		natsURL = startNATS(t)
		addrA, stopA := startStoppableBroker(t, natsURL, narrowingAuthorizer(deny), persistent)
		c1 := dialRaw(t, addrA)
		require.False(t, c1.connect(rawConnect(id, 300)).SessionPresent)
		c1.subscribe("secret/#", packet.QoS1)
		c1.subscribe("public/#", packet.QoS1)
		pub, _ := connectClient(t, addrA, connectOpts("withdrawn-restart-pub"))
		pub.publish(&paho.Publish{Topic: "secret/a", QoS: 1, Payload: []byte("classified")})
		owed = c1.expectPublish().PacketID
		c1.drop()
		time.Sleep(200 * time.Millisecond)
		stopA()

		deny.Store(true)
		bB, addrB, stopB := startBrokerHandle(t, natsURL, narrowingAuthorizer(deny), persistent)
		c2 := dialRaw(t, addrB)
		require.True(t, c2.connect(rawConnect(id, 300)).SessionPresent)
		c2.expectNothing()
		if kill {
			// The broker dies with the client attached: no release, so the
			// record is the one the withdrawal itself wrote.
			natsmqtt5.Kill(bB)
		} else {
			c2.drop()
			time.Sleep(200 * time.Millisecond)
			stopB()
		}
		deny.Store(false)
		return natsURL, owed
	}

	t.Run("the acknowledgement is ignored, not a Protocol Error", func(t *testing.T) {
		var deny atomic.Bool
		natsURL, owed := withdrawnAcrossTwoBrokers(t, &deny, false)
		addrC := startBroker(t, natsURL, narrowingAuthorizer(&deny), persistent)

		c3 := dialRaw(t, addrC)
		require.True(t, c3.connect(rawConnect(id, 300)).SessionPresent)
		c3.send(&packet.Puback{Ack: packet.Ack{PacketID: owed}})
		c3.expectNothing()
	})

	t.Run("the acknowledgement is ignored after the broker that withdrew was killed", func(t *testing.T) {
		var deny atomic.Bool
		natsURL, owed := withdrawnAcrossTwoBrokers(t, &deny, true)
		addrC := startBroker(t, natsURL, narrowingAuthorizer(&deny), persistent)

		c3 := dialRaw(t, addrC)
		require.True(t, c3.connect(rawConnect(id, 300)).SessionPresent)
		c3.send(&packet.Puback{Ack: packet.Ack{PacketID: owed}})
		c3.expectNothing()
	})

	t.Run("the identifier is skipped, then reusable after two more connections", func(t *testing.T) {
		var deny atomic.Bool
		natsURL, owed := withdrawnAcrossTwoBrokers(t, &deny, false)

		// allocated publishes to the session on broker b with its allocator just
		// below owed (SetNextPacketID) and returns the identifier the broker chose.
		allocated := func(t *testing.T, b *natsmqtt5.Broker, addr string, c *rawClient) uint16 {
			pub, _ := connectClient(t, addr, connectOpts("withdrawn-restart-pub2"))
			natsmqtt5.SetNextPacketID(b, id, owed-1)
			pub.publish(&paho.Publish{Topic: "public/a", QoS: 1, Payload: []byte("p")})
			p := c.expectPublish()
			c.send(&packet.Puback{Ack: packet.Ack{PacketID: p.PacketID}})
			return p.PacketID
		}

		// Broker C: the first resumption after the withdrawal. Still owed.
		bC, addrC, stopC := startBrokerHandle(t, natsURL, narrowingAuthorizer(&deny), persistent)
		c3 := dialRaw(t, addrC)
		require.True(t, c3.connect(rawConnect(id, 300)).SessionPresent)
		assert.Equal(t, owed+1, allocated(t, bC, addrC, c3),
			"a withdrawn identifier must stay unavailable across a broker restart")
		c3.drop()
		time.Sleep(200 * time.Millisecond)
		stopC()

		// Broker D: the second resumption. The client has had two connections to
		// acknowledge, so the identifier is free, as it is in one process.
		bD, addrD, _ := startBrokerHandle(t, natsURL, narrowingAuthorizer(&deny), persistent)
		c4 := dialRaw(t, addrD)
		require.True(t, c4.connect(rawConnect(id, 300)).SessionPresent)
		assert.Equal(t, owed, allocated(t, bD, addrD, c4),
			"the two-resumptions rule must count the connections made before the restart")
	})
}
