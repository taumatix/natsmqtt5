package natsmqtt5_test

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// Keeping clients off "$" topics (MQTT-5.0 §4.7.2): "The Server SHOULD prevent
// Clients from using such Topic Names to exchange messages with other Clients."
// The sentence carries no statement id, and the broker follows it only when
// Options.RestrictDollarTopics is set. Each test runs a real nats-server and a
// broker on TCP and speaks raw packets; what leaks past the broker is seen by a
// plain NATS subscriber on the broker's whole subject space.

func restrictDollar(o *natsmqtt5.Options) { o.RestrictDollarTopics = true }

// subscribeCodes sends a SUBSCRIBE for one filter and returns the SUBACK's
// Reason Codes without asserting success.
func (c *rawClient) subscribeCodes(filter string, qos packet.QoS) []packet.ReasonCode {
	c.t.Helper()
	c.subID++
	c.send(&packet.Subscribe{
		PacketID:      c.subID,
		Subscriptions: []packet.Subscription{{Filter: filter, QoS: qos}},
	})
	ack, ok := c.read().(*packet.Suback)
	require.True(c.t, ok, "a SUBSCRIBE must be answered with a SUBACK")
	return ack.ReasonCodes
}

// publishCode sends a QoS 1 or 2 PUBLISH and returns the Reason Code of the
// PUBACK or PUBREC that answers it.
func (c *rawClient) publishCode(topic string, qos packet.QoS, id uint16) packet.ReasonCode {
	c.t.Helper()
	c.send(&packet.Publish{Topic: topic, QoS: qos, PacketID: id, Payload: []byte("x")})
	switch p := c.read().(type) {
	case *packet.Puback:
		require.Equal(c.t, packet.QoS1, qos)
		require.Equal(c.t, id, p.PacketID)
		return p.ReasonCode
	case *packet.Pubrec:
		require.Equal(c.t, packet.QoS2, qos)
		require.Equal(c.t, id, p.PacketID)
		return p.ReasonCode
	default:
		c.t.Fatalf("expected a PUBACK or PUBREC, got %T", p)
		return 0
	}
}

// natsTap records every message that reaches NATS under the broker's subject
// prefix, which is where a message a client published would be seen by anyone.
func natsTap(t *testing.T, natsURL string) (got func() []string) {
	t.Helper()
	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	ch := make(chan string, 64)
	_, err = nc.Subscribe(natsmqtt5.DefaultSubjectPrefix+".>", func(m *nats.Msg) { ch <- m.Subject })
	require.NoError(t, err)
	require.NoError(t, nc.Flush())
	return func() []string {
		time.Sleep(300 * time.Millisecond)
		var out []string
		for {
			select {
			case s := <-ch:
				out = append(out, s)
			default:
				return out
			}
		}
	}
}

func TestRestrictedBrokerRefusesSubscriptionsOnDollarTopics_MQTT_5_0_4_7_2(t *testing.T) {
	addr := startBroker(t, startNATS(t), restrictDollar)
	c := dialRaw(t, addr)
	c.connect(rawConnect("dollar-sub", 0))

	for _, filter := range []string{
		"$SYS/#",
		"$SYS/broker/uptime",
		"$app/x",
		"$app/+",
		"$share/g/$app/x", // a Shared Subscription is not a way round it
		"$share/g/$SYS/#",
	} {
		assert.Equal(t, []packet.ReasonCode{packet.TopicFilterInvalid}, c.subscribeCodes(filter, packet.QoS1),
			"%s: Topic Filter invalid (0x8F)", filter)
	}

	// Everything else is untouched: a plain filter, a wildcard-first one (which
	// does not match "$" topics [MQTT-4.7.2-1]) and Shared Subscriptions.
	for _, filter := range []string{"app/x", "#", "+/x", "$share/g/app/x", "$share/g/jobs/#"} {
		assert.Equal(t, []packet.ReasonCode{packet.GrantedQoS1}, c.subscribeCodes(filter, packet.QoS1),
			"%s is granted", filter)
	}
}

func TestRestrictedBrokerRefusesPublishesToDollarTopics_MQTT_5_0_4_7_2(t *testing.T) {
	natsURL := startNATS(t)
	addr := startBroker(t, natsURL, restrictDollar)
	tap := natsTap(t, natsURL)
	c := dialRaw(t, addr)
	c.connect(rawConnect("dollar-pub", 0))

	assert.Equal(t, packet.TopicNameInvalid, c.publishCode("$app/x", packet.QoS1, 1), "QoS 1: PUBACK 0x90")
	assert.Equal(t, packet.TopicNameInvalid, c.publishCode("$SYS/broker", packet.QoS2, 2), "QoS 2: PUBREC 0x90")
	// QoS 0 has no acknowledgement to carry a reason: the PUBLISH is dropped.
	c.send(&packet.Publish{Topic: "$app/q0", QoS: packet.QoS0, Payload: []byte("x")})
	c.expectNothing()
	assert.Empty(t, tap(), "nothing reached NATS")

	// An allowed topic still works, over the same connection.
	assert.Equal(t, packet.Success, c.publishCode("app/x", packet.QoS1, 3))
	assert.NotEmpty(t, tap())
}

func TestRestrictedBrokerRefusesAWillOnADollarTopic_MQTT_5_0_4_7_2(t *testing.T) {
	addr := startBroker(t, startNATS(t), restrictDollar)
	c := dialRaw(t, addr)
	cp := rawConnect("dollar-will", 0)
	cp.Will = &packet.Will{Topic: "$app/will", Payload: []byte("bye")}
	c.send(cp)
	ack, ok := c.read().(*packet.Connack)
	require.True(t, ok, "expected a CONNACK")
	assert.Equal(t, packet.TopicNameInvalid, ack.ReasonCode, "Topic Name invalid (0x90) [MQTT-3.2.2-8]")
}

// Left at its default, the broker does what it did before: clients exchange
// messages on "$app/…", and the filters above are granted.
func TestUnrestrictedBrokerLeavesDollarTopicsToApplications_MQTT_5_0_4_7_2(t *testing.T) {
	addr := startBroker(t, startNATS(t))
	other := dialRaw(t, addr)
	other.connect(rawConnect("dollar-other", 0))
	for _, filter := range []string{"$SYS/#", "$share/g/$app/x"} {
		assert.Equal(t, []packet.ReasonCode{packet.GrantedQoS1}, other.subscribeCodes(filter, packet.QoS1),
			"%s is granted", filter)
	}
	sub := dialRaw(t, addr)
	sub.connect(rawConnect("dollar-sub", 0))
	sub.subscribe("$app/x", packet.QoS1)

	pub := dialRaw(t, addr)
	pub.connect(rawConnect("dollar-pub", 0))
	assert.Equal(t, packet.Success, pub.publishCode("$app/x", packet.QoS1, 1))
	assert.Equal(t, []string{"x"}, sub.collectPublishes(1))

	will := dialRaw(t, addr)
	cp := rawConnect("dollar-will", 0)
	cp.Will = &packet.Will{Topic: "$app/will", Payload: []byte("bye")}
	will.connect(cp)
}

// The two levels the broker keeps its own data under stay refused whichever way
// the option is set (v0.9.1).
func TestReservedLevelsAreRefusedWithAndWithoutTheOption(t *testing.T) {
	for name, opts := range map[string][]func(*natsmqtt5.Options){
		"restricted": {restrictDollar},
		"default":    nil,
	} {
		t.Run(name, func(t *testing.T) {
			c := dialRaw(t, startBroker(t, startNATS(t), opts...))
			c.connect(rawConnect("reserved", 0))
			assert.Equal(t, packet.TopicNameInvalid, c.publishCode("$retained/news", packet.QoS1, 1))
			assert.Equal(t, []packet.ReasonCode{packet.TopicFilterInvalid}, c.subscribeCodes("$queue/#", packet.QoS1))
		})
	}
}
