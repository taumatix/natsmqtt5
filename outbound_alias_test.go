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

// Outbound Topic Aliases (MQTT-5.0 §3.3.2.3.4): over a real socket, a subscriber that
// offers a Topic Alias Maximum is sent a repeated long topic as an alias and an empty
// name, up to its maximum, least recently used first.

func aliasConnect(clientID string, max uint16) *packet.Connect {
	cp := rawConnect(clientID, 300)
	cp.CleanStart = true
	cp.Properties.TopicAliasMaximum = packet.Uint16(max)
	return cp
}

func withOutboundAliases(o *natsmqtt5.Options) { o.OutboundTopicAliases = true }

func publishQoS1(t *testing.T, pub *testClient, topic string) {
	t.Helper()
	_, err := pub.Publish(context.Background(), &paho.Publish{Topic: topic, QoS: 1, Payload: []byte("p")})
	require.NoError(t, err)
}

// aliasOf returns the Topic Alias on p, or 0.
func aliasOf(p *packet.Publish) uint16 {
	if p.Properties == nil || p.Properties.TopicAlias == nil {
		return 0
	}
	return *p.Properties.TopicAlias
}

func TestARepeatedTopicIsSentAsAnAliasAndTheLeastRecentlyUsedIsReplaced(t *testing.T) {
	addr := startBroker(t, startNATS(t), withOutboundAliases)
	sub := dialRaw(t, addr)
	sub.connect(aliasConnect("alias-sub", 2))
	sub.subscribe("alias/#", packet.QoS1)
	pub, _ := connectClient(t, addr, connectOpts("alias-pub"))

	type step struct {
		topic     string
		wantName  string // "" when the name is left out
		wantAlias uint16
	}
	for _, s := range []step{
		{"alias/topic-one", "alias/topic-one", 1},     // first use: name and alias
		{"alias/topic-one", "", 1},                    // again: alias alone
		{"alias/topic-two", "alias/topic-two", 2},     // second slot
		{"alias/topic-one", "", 1},                    // refreshes one, so two is now the oldest
		{"alias/topic-three", "alias/topic-three", 2}, // table full: two's slot is remapped
		{"alias/topic-two", "alias/topic-two", 1},     // one is now the oldest
		{"alias/topic-two", "", 1},
	} {
		publishQoS1(t, pub, s.topic)
		got := sub.expectPublish()
		assert.Equal(t, s.wantName, got.Topic, "name sent for %s", s.topic)
		assert.Equal(t, s.wantAlias, aliasOf(got), "alias sent for %s", s.topic)
		sub.send(&packet.Puback{Ack: packet.Ack{PacketID: got.PacketID}})
	}
}

func TestNoAliasIsSentUnlessTheOptionIsOnAndTheClientOfferedAMaximum(t *testing.T) {
	t.Run("option off", func(t *testing.T) {
		addr := startBroker(t, startNATS(t), func(o *natsmqtt5.Options) { o.PinOutboundTopicAliases = pin(false) })
		sub := dialRaw(t, addr)
		sub.connect(aliasConnect("off-sub", 5))
		sub.subscribe("alias/#", packet.QoS1)
		pub, _ := connectClient(t, addr, connectOpts("off-pub"))
		for range 2 {
			publishQoS1(t, pub, "alias/topic-one")
			got := sub.expectPublish()
			assert.Equal(t, "alias/topic-one", got.Topic)
			assert.Zero(t, aliasOf(got))
		}
	})
	t.Run("client offered no maximum", func(t *testing.T) {
		addr := startBroker(t, startNATS(t), withOutboundAliases)
		sub := dialRaw(t, addr)
		cp := rawConnect("none-sub", 300)
		cp.CleanStart = true
		sub.connect(cp)
		sub.subscribe("alias/#", packet.QoS1)
		pub, _ := connectClient(t, addr, connectOpts("none-pub"))
		for range 2 {
			publishQoS1(t, pub, "alias/topic-one")
			got := sub.expectPublish()
			assert.Equal(t, "alias/topic-one", got.Topic)
			assert.Zero(t, aliasOf(got))
		}
	})
	t.Run("a short topic gains nothing", func(t *testing.T) {
		addr := startBroker(t, startNATS(t), withOutboundAliases)
		sub := dialRaw(t, addr)
		sub.connect(aliasConnect("short-sub", 5))
		sub.subscribe("s/#", packet.QoS1)
		pub, _ := connectClient(t, addr, connectOpts("short-pub"))
		for range 2 {
			publishQoS1(t, pub, "s/a")
			got := sub.expectPublish()
			assert.Equal(t, "s/a", got.Topic)
			assert.Zero(t, aliasOf(got))
		}
	})
}

// The table belongs to the network connection [MQTT-3.3.2-7]: a message resent to a
// resumed session carries its name, and the new connection builds its own mapping.
func TestAResentMessageCarriesItsNameOnANewConnection(t *testing.T) {
	addr := startBroker(t, startNATS(t), withOutboundAliases, func(o *natsmqtt5.Options) { o.PersistentSessions = true })
	first := dialRaw(t, addr)
	first.connect(aliasConnect("alias-resume", 3))
	first.subscribe("alias/#", packet.QoS1)
	pub, _ := connectClient(t, addr, connectOpts("alias-resume-pub"))

	publishQoS1(t, pub, "alias/topic-one")
	got := first.expectPublish()
	require.Equal(t, uint16(1), aliasOf(got))
	publishQoS1(t, pub, "alias/topic-one")
	got = first.expectPublish()
	require.Empty(t, got.Topic, "the second is sent by alias alone")
	// Neither is acknowledged; the connection is lost.
	_ = first.nc.Close()

	cp := aliasConnect("alias-resume", 3)
	cp.CleanStart = false
	second := dialRaw(t, addr)
	ack := second.connect(cp)
	require.True(t, ack.SessionPresent)
	again := second.expectPublish()
	assert.True(t, again.Dup, "a resend")
	assert.Equal(t, "alias/topic-one", again.Topic, "the new connection has no alias to rely on")
	assert.Equal(t, uint16(1), aliasOf(again), "it starts its own table")
	again = second.expectPublish()
	assert.True(t, again.Dup, "a resend")
	assert.Empty(t, again.Topic, "and the second resend uses the alias just set")
	assert.Equal(t, uint16(1), aliasOf(again))
}

// A new alias adds three bytes; a message that fits the client's Maximum Packet Size
// plain is sent plain rather than discarded.
func TestAnAliasIsNotSetWhereItWouldTakeAPacketOverTheClientsLimit(t *testing.T) {
	addr := startBroker(t, startNATS(t), withOutboundAliases)
	plain, err := packet.Encode(&packet.Publish{Topic: "alias/topic-one", QoS: packet.QoS1, PacketID: 1, Payload: []byte("p")})
	require.NoError(t, err)

	sub := dialRaw(t, addr)
	cp := aliasConnect("alias-limit", 5)
	cp.Properties.MaximumPacketSize = packet.Uint32(uint32(len(plain)))
	sub.connect(cp)
	sub.subscribe("alias/#", packet.QoS1)
	pub, _ := connectClient(t, addr, connectOpts("alias-limit-pub"))

	publishQoS1(t, pub, "alias/topic-one")
	got := sub.expectPublish()
	assert.Equal(t, "alias/topic-one", got.Topic)
	assert.Zero(t, aliasOf(got), "no alias, since naming one would not fit")
}

// A client whose Topic Alias Maximum is smaller than the set of names it receives must not be
// sent more bytes than with aliases off: giving each new name the single alias in turn costs
// three bytes a PUBLISH and saves nothing, since no name returns before the next displaces it
// (ROADMAP.md, "Outbound topic aliases thrash against a small client maximum").
func TestASmallClientMaximumIsNotSentMoreBytesThanNoAliases(t *testing.T) {
	topics := []string{"fleet/site-1/device-a/telemetry", "fleet/site-1/device-b/telemetry", "fleet/site-1/device-c/telemetry", "fleet/site-1/device-d/telemetry"}
	const rounds = 15

	// bytesReceived is the size of every PUBLISH a subscriber with a maximum of 1 is sent for a
	// round robin over topics, and the highest alias it was given.
	bytesReceived := func(t *testing.T, aliasesOn bool) (total int, highest uint16) {
		addr := startBroker(t, startNATS(t), func(o *natsmqtt5.Options) { o.PinOutboundTopicAliases = pin(aliasesOn) })
		sub := dialRaw(t, addr)
		sub.connect(aliasConnect("thrash-sub", 1))
		sub.subscribe("fleet/#", packet.QoS1)
		pub, _ := connectClient(t, addr, connectOpts("thrash-pub"))
		for range rounds {
			for _, topic := range topics {
				publishQoS1(t, pub, topic)
				got := sub.expectPublish()
				wire, err := packet.Encode(got)
				require.NoError(t, err)
				total += len(wire)
				highest = max(highest, aliasOf(got))
				sub.send(&packet.Puback{Ack: packet.Ack{PacketID: got.PacketID}})
			}
		}
		return total, highest
	}

	off, _ := bytesReceived(t, false)
	on, highest := bytesReceived(t, true)
	assert.LessOrEqual(t, highest, uint16(1), "no alias above the client's maximum [MQTT-3.3.2-9]")
	assert.LessOrEqual(t, on, off+3, "aliases on: %d bytes, off: %d", on, off)
}
