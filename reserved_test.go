package natsmqtt5_test

import (
	"testing"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5/packet"
)

// The retained store and the offline queue keep their messages under
// <prefix>.$retained.> and <prefix>.$queue.>, inside the subject space MQTT
// topics map onto. A client could publish to "$retained/x" and replace the
// retained message for "x", with the Authorizer asked only about
// "$retained/x"; or subscribe to "$queue/#" and read every QoS 1 and 2
// message on the broker. Both first levels are reserved now.

func TestAClientCannotForgeARetainedMessageThroughTheStoresSubject(t *testing.T) {
	addr := startBroker(t, startNATS(t))

	pub, _ := connectClient(t, addr, connectOpts("pub-reserved"))
	pub.publish(&paho.Publish{Topic: "news", QoS: 1, Retain: true, Payload: []byte("genuine")})

	forger := dialRaw(t, addr)
	forger.connect(rawConnect("forger", 0))
	forger.send(&packet.Publish{Topic: "$retained/news", QoS: packet.QoS1, PacketID: 1, Payload: []byte("forged")})
	ack, ok := forger.read().(*packet.Puback)
	require.True(t, ok, "expected a PUBACK")
	assert.Equal(t, packet.TopicNameInvalid, ack.Ack.ReasonCode, "a publish into the retained store's subject was accepted")

	reader := dialRaw(t, addr)
	reader.connect(rawConnect("reader-reserved", 0))
	reader.subscribe("news", packet.QoS1)
	got := reader.expectPublish()
	assert.Equal(t, "genuine", string(got.Payload), "the retained message for news was replaced")
}

func TestAClientCannotSubscribeToTheBrokersInternalSubjects(t *testing.T) {
	addr := startBroker(t, startNATS(t))
	c := dialRaw(t, addr)
	c.connect(rawConnect("snooper", 0))

	for i, filter := range []string{"$retained/#", "$queue/#", "$queue/news", "$share/g/$queue/#"} {
		c.send(&packet.Subscribe{
			PacketID:      uint16(100 + i),
			Subscriptions: []packet.Subscription{{Filter: filter, QoS: packet.QoS1}},
		})
		ack, ok := c.read().(*packet.Suback)
		require.True(t, ok, "expected a SUBACK for %s", filter)
		assert.Equal(t, []packet.ReasonCode{packet.TopicFilterInvalid}, ack.ReasonCodes, filter)
	}
}

func TestAWillCannotTargetTheBrokersInternalSubjects(t *testing.T) {
	addr := startBroker(t, startNATS(t))
	c := dialRaw(t, addr)
	cp := rawConnect("will-reserved", 0)
	cp.Will = &packet.Will{Topic: "$retained/news", Payload: []byte("forged"), Retain: true}
	c.send(cp)
	ack, ok := c.read().(*packet.Connack)
	require.True(t, ok, "expected a CONNACK")
	assert.Equal(t, packet.TopicNameInvalid, ack.ReasonCode)
}

// Other "$" topics are left alone: applications that use them are outside the
// spec's advice (MQTT-5.0 §4.7.2) but not this fix's business.
func TestOtherDollarTopicsStillWork(t *testing.T) {
	addr := startBroker(t, startNATS(t))
	sub := dialRaw(t, addr)
	sub.connect(rawConnect("dollar-sub", 0))
	sub.subscribe("$app/x", packet.QoS1)

	pub, _ := connectClient(t, addr, connectOpts("dollar-pub"))
	pub.publish(&paho.Publish{Topic: "$app/x", QoS: 1, Payload: []byte("ok")})
	assert.Equal(t, "$app/x", sub.expectPublish().Topic)
}
