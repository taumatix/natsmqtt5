package natsmqtt5_test

import (
	"context"
	"testing"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// Options.EscapeNATSWildcards: over real sockets, a topic with '*', '>' or a
// space is carried intact, and a literal '*' is not a NATS wildcard.

func withEscapes(o *natsmqtt5.Options) { o.EscapeNATSWildcards = true }

func publishNamed(t *testing.T, pub *testClient, topic string, retain bool) {
	t.Helper()
	_, err := pub.Publish(context.Background(), &paho.Publish{Topic: topic, QoS: 1, Retain: retain, Payload: []byte("p")})
	require.NoError(t, err)
}

func TestTopicsWithNATSWildcardsAndSpacesAreRefusedByDefault(t *testing.T) {
	addr := startBroker(t, startNATS(t))
	pub, _ := connectClient(t, addr, connectOpts("plain-pub"))
	for _, topic := range []string{"a*", "a b", "a>"} {
		_, err := pub.Publish(context.Background(), &paho.Publish{Topic: topic, QoS: 1, Payload: []byte("p")})
		assert.Error(t, err, topic)
	}
}

func TestAStarOrASpaceInATopicIsCarriedIntactAndIsNotAWildcard(t *testing.T) {
	addr := startBroker(t, startNATS(t), withEscapes)
	pub, _ := connectClient(t, addr, connectOpts("esc-pub"))

	literal := dialRaw(t, addr)
	literal.connect(rawConnect("esc-literal", 300))
	literal.subscribe("esc/*", packet.QoS1) // a level that is the character '*', not a wildcard
	spaced := dialRaw(t, addr)
	spaced.connect(rawConnect("esc-spaced", 300))
	spaced.subscribe("esc/with space/+", packet.QoS1)
	all := dialRaw(t, addr)
	all.connect(rawConnect("esc-all", 300))
	all.subscribe("esc/#", packet.QoS1)

	// A sibling of the literal level: only the wildcard subscriber may see it.
	publishNamed(t, pub, "esc/other", false)
	got := all.expectPublish()
	assert.Equal(t, "esc/other", got.Topic)
	all.send(&packet.Puback{Ack: packet.Ack{PacketID: got.PacketID}})

	publishNamed(t, pub, "esc/*", false)
	for name, c := range map[string]*rawClient{"literal": literal, "all": all} {
		got := c.expectPublish()
		assert.Equal(t, "esc/*", got.Topic, name)
		c.send(&packet.Puback{Ack: packet.Ack{PacketID: got.PacketID}})
	}

	publishNamed(t, pub, "esc/with space/>", false)
	for name, c := range map[string]*rawClient{"spaced": spaced, "all": all} {
		got := c.expectPublish()
		assert.Equal(t, "esc/with space/>", got.Topic, name)
		c.send(&packet.Puback{Ack: packet.Ack{PacketID: got.PacketID}})
	}
}

func TestARetainedMessageOnATopicWithAStarKeepsItsName(t *testing.T) {
	addr := startBroker(t, startNATS(t), withEscapes)
	pub, _ := connectClient(t, addr, connectOpts("esc-ret-pub"))
	publishNamed(t, pub, "ret/a b/*", true)

	late := dialRaw(t, addr)
	late.connect(rawConnect("esc-ret-late", 300))
	late.subscribe("ret/#", packet.QoS1)
	got := late.expectPublish()
	assert.Equal(t, "ret/a b/*", got.Topic)
	assert.True(t, got.Retain)
}

func TestAMessageQueuedOfflineForAStarTopicKeepsItsName(t *testing.T) {
	addr := startBroker(t, startNATS(t), withEscapes)
	persistent := func() *packet.Connect {
		cp := rawConnect("esc-offline", 300)
		cp.CleanStart = false
		cp.Properties = &packet.Properties{SessionExpiryInterval: packet.Uint32(300)}
		return cp
	}
	sub := dialRaw(t, addr)
	sub.connect(persistent())
	sub.subscribe("off/*/a b", packet.QoS1)
	sub.write(rawDisconnect)

	pub, _ := connectClient(t, addr, connectOpts("esc-offline-pub"))
	publishNamed(t, pub, "off/other/a b", false) // not for this session
	publishNamed(t, pub, "off/*/a b", false)

	back := dialRaw(t, addr)
	back.connect(persistent())
	got := back.expectPublish()
	assert.Equal(t, "off/*/a b", got.Topic)
}
