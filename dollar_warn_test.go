package natsmqtt5_test

import (
	"strings"
	"testing"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

const dollarWarning = "a client used a topic starting with $"

func dollarWarnings(logs *syncBuffer) int {
	return strings.Count(logs.String(), dollarWarning)
}

// MQTT-5.0 §4.7.2: "The Server SHOULD prevent Clients from using such Topic Names to exchange
// messages with other Clients." An unrestricted broker does not, so its first "$" use is logged
// as a warning, once, naming the client; ordinary topics, the $share prefix and the reserved
// levels do not trigger it. The statement has no id (dollar.go).
func TestAnUnrestrictedBrokerWarnsOnceTheFirstTimeAClientUsesADollarTopic_MQTT_5_0_4_7_2(t *testing.T) {
	var logs syncBuffer
	addr := startBroker(t, startNATS(t), loggingBroker(t, &logs))
	c := dialRaw(t, addr)
	c.connect(rawConnect("dollar-user", 0))

	c.subscribeCodes("plain/#", packet.QoS1)
	c.subscribeCodes("$share/g/plain/#", packet.QoS1)
	assert.Equal(t, packet.TopicNameInvalid, c.publishCode("$retained/x", packet.QoS1, 1))
	assert.Equal(t, 0, dollarWarnings(&logs), "no warning for a use that is not a $ topic, or for a reserved level")

	assert.Equal(t, []packet.ReasonCode{packet.GrantedQoS1}, c.subscribeCodes("$app/x", packet.QoS1), "still allowed")
	assert.Equal(t, 1, dollarWarnings(&logs))
	assert.Contains(t, logs.String(), "client_id=dollar-user")
	assert.Contains(t, logs.String(), "topic=$app/x")

	assert.Equal(t, packet.Success, c.publishCode("$app/q", packet.QoS1, 2))
	assert.Equal(t, packet.Success, c.publishCode("$app/y", packet.QoS1, 3))
	other := dialRaw(t, addr)
	other.connect(rawConnect("dollar-other", 0))
	other.subscribeCodes("$share/g/$app/z", packet.QoS1)
	assert.Equal(t, 1, dollarWarnings(&logs), "once per broker, not per use or per client")
}

// The first use may be a publish or a Will as well.
func TestTheDollarWarningCoversAPublishAndAWill(t *testing.T) {
	var logs syncBuffer
	addr := startBroker(t, startNATS(t), loggingBroker(t, &logs))
	c := dialRaw(t, addr)
	c.connect(rawConnect("dollar-pub", 0))
	assert.Equal(t, packet.Success, c.publishCode("$app/x", packet.QoS1, 1))
	assert.Equal(t, 1, dollarWarnings(&logs))
	assert.Contains(t, logs.String(), "action=publish")

	var willLogs syncBuffer
	w := dialRaw(t, startBroker(t, startNATS(t), loggingBroker(t, &willLogs)))
	cp := rawConnect("dollar-will", 0)
	cp.Will = &packet.Will{Topic: "$app/will", Payload: []byte("bye")}
	w.connect(cp)
	assert.Equal(t, 1, dollarWarnings(&willLogs))
	assert.Contains(t, willLogs.String(), "action=will")
}

// A restricted broker refuses, so it has nothing to warn about.
func TestARestrictedBrokerDoesNotWarnAboutDollarTopics(t *testing.T) {
	var logs syncBuffer
	addr := startBroker(t, startNATS(t), loggingBroker(t, &logs), restrictDollar)
	c := dialRaw(t, addr)
	c.connect(rawConnect("dollar-restricted", 0))
	assert.Equal(t, packet.TopicNameInvalid, c.publishCode("$app/x", packet.QoS1, 1))
	assert.Equal(t, []packet.ReasonCode{packet.TopicFilterInvalid}, c.subscribeCodes("$app/#", packet.QoS1))
	assert.Equal(t, 0, dollarWarnings(&logs))
}

// A message a "$" filter earned on an unrestricted broker is in the session's in-flight set when
// a restricted broker resumes it. The resume drops the filter (see the test for stored
// subscriptions) and must withdraw the message with it, the way a denied filter's are
// [MQTT-4.4.0-1]: the client is not sent a message on a topic the broker would now refuse it,
// still gets the one a surviving filter earned, and its late PUBACK for the withdrawn Packet
// Identifier is ignored, not answered with a Protocol Error. Two brokers share one NATS server
// and JetStream, the first stopped cleanly.
func TestRestrictedBrokerWithdrawsWhatADollarFilterHadInFlight_MQTT_4_4_0_1(t *testing.T) {
	const id = "dollar-inflight"
	natsURL := startNATS(t)
	var logsA syncBuffer
	addrA, stopA := startStoppableBroker(t, natsURL, persistent, loggingBroker(t, &logsA))

	c1 := dialRaw(t, addrA)
	require.False(t, c1.connect(rawConnect(id, 300)).SessionPresent)
	c1.subscribe("$app/x", packet.QoS1)
	c1.subscribe("public/#", packet.QoS1)
	pub, _ := connectClient(t, addrA, connectOpts("dollar-inflight-pub"))
	pub.publish(&paho.Publish{Topic: "$app/x", QoS: 1, Payload: []byte("dollar")})
	dollar := c1.expectPublish()
	require.Equal(t, "$app/x", dollar.Topic)
	// Published only once the first has arrived: two NATS subscriptions are not ordered.
	pub.publish(&paho.Publish{Topic: "public/a", QoS: 1, Payload: []byte("fine")})
	require.Equal(t, "public/a", c1.expectPublish().Topic)
	c1.drop()
	waitDetached(t, &logsA, id)
	stopA()

	addrB := startBroker(t, natsURL, persistent, func(o *natsmqtt5.Options) { o.RestrictDollarTopics = true })
	c2 := dialRaw(t, addrB)
	require.True(t, c2.connect(rawConnect(id, 300)).SessionPresent)
	again := c2.expectPublish()
	require.Equal(t, "public/a", again.Topic, "the surviving filter's arrears come back")
	assert.True(t, again.Dup)
	c2.expectNothing()

	c2.send(&packet.Puback{Ack: packet.Ack{PacketID: dollar.PacketID}})
	c2.expectNothing()
}
