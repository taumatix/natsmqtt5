package natsmqtt5_test

import (
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5/v2"
	"github.com/taumatix/natsmqtt5/v2/packet"
)

// Options.NATSIngest: a plain NATS client publishes through a broker.
//
// A message a plain NATS client publishes on a live subject has no queue copy: a session that
// was away never gets it, and a shared subscription gets it only through its NATS queue group,
// which can pick a member whose client is away (MQTT-5.0 §4.8.2; the member's share is then
// dropped). Published on <prefix>.$ingest.<subject> instead, the message is taken by one broker
// (a NATS queue group of all brokers with the option on), which does what it does for an MQTT
// PUBLISH: stores the queue copy, publishes the live copy with its id and sequence, and answers
// the request. The statements at stake are [MQTT-4.5.0-1] (a session's messages are part of its
// state) and §4.8.2's shared delivery; the broker's own record in CONFORMANCE.md, the
// specification text was not re-read for this change.

func withNATSIngest(o *natsmqtt5.Options) { o.NATSIngest = true }

// ingest sends payload to topic's ingest subject as a NATS request at qos, and returns the reply.
func ingest(t *testing.T, nc *nats.Conn, subject string, qos byte, payload string, headers ...string) *nats.Msg {
	t.Helper()
	msg := nats.NewMsg("mqtt.$ingest." + subject)
	msg.Data = []byte(payload)
	msg.Header.Set("Mqtt5-Qos", string('0'+rune(qos)))
	for i := 0; i+1 < len(headers); i += 2 {
		msg.Header.Set(headers[i], headers[i+1])
	}
	reply, err := nc.RequestMsg(msg, 5*time.Second)
	require.NoError(t, err)
	return reply
}

func plainNATS(t *testing.T, natsURL string) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	return nc
}

// A QoS 1 message a plain NATS client sent through the ingest subject while a session was away
// is replayed to it when it comes back. Two brokers take ingest messages; whichever takes this
// one, the session's broker finds its copy in the queue.
func TestNATSIngestQueuesAPlainNATSMessageForASessionThatWasAway(t *testing.T) {
	natsURL := startNATS(t)
	var logs syncBuffer
	addr := startBroker(t, natsURL, withNATSIngest, loggingBroker(t, &logs))
	startBroker(t, natsURL, withNATSIngest)

	away := dialRaw(t, addr)
	away.connect(rawConnect("ingest-away", 300))
	away.subscribe("ingest/away", packet.QoS1)
	away.drop()
	waitDetached(t, &logs, "ingest-away")

	reply := ingest(t, plainNATS(t, natsURL), "ingest.away", 1, "kept")
	assert.Empty(t, reply.Header.Get("Mqtt5-Ingest-Error"))
	assert.NotEmpty(t, reply.Header.Get("Mqtt5-Queue-Seq"), "the reply names the queue copy")

	back := dialRaw(t, addr)
	back.connect(rawConnect("ingest-away", 300))
	p := back.expectPublish()
	assert.Equal(t, "ingest/away", p.Topic)
	assert.Equal(t, "kept", string(p.Payload))
	assert.Equal(t, packet.QoS1, p.QoS)
	back.send(&packet.Puback{Ack: packet.Ack{PacketID: p.PacketID}})
	back.expectNothing()
}

// A connected subscriber, on any broker, gets an ingested message once.
func TestNATSIngestDeliversToAConnectedSubscriberOnce(t *testing.T) {
	natsURL := startNATS(t)
	startBroker(t, natsURL, withNATSIngest)
	addr := startBroker(t, natsURL)

	sub, _ := connectClient(t, addr, connectOpts("ingest-sub"))
	sub.subscribe(paho.SubscribeOptions{Topic: "ingest/live", QoS: 2})

	ingest(t, plainNATS(t, natsURL), "ingest.live", 2, "once", "Mqtt5-Content-Type", "text/plain")
	got := sub.expectMessage()
	assert.Equal(t, "once", got.Payload)
	assert.Equal(t, byte(2), got.QoS)
	assert.Equal(t, "text/plain", got.Properties.ContentType, "the MQTT properties in the headers are kept")
	sub.expectNoMessage()
}

// A Nats-Msg-Id header is not passed to the queue copy, where JetStream would take a second
// message with the same value for a duplicate and give back the first one's sequence.
func TestNATSIngestDoesNotHandNatsHeadersToJetStream(t *testing.T) {
	natsURL := startNATS(t)
	startBroker(t, natsURL, withNATSIngest)
	nc := plainNATS(t, natsURL)

	first := ingest(t, nc, "ingest.natshdr", 1, "a", "Nats-Msg-Id", "reused")
	second := ingest(t, nc, "ingest.natshdr", 1, "b", "Nats-Msg-Id", "reused")
	require.NotEmpty(t, first.Header.Get("Mqtt5-Queue-Seq"))
	assert.NotEqual(t, first.Header.Get("Mqtt5-Queue-Seq"), second.Header.Get("Mqtt5-Queue-Seq"),
		"each message has its own queue copy")
}

// A shared subscription with one member away: every ingested QoS 1 message reaches the member
// that is connected, through the group's backlog, and none is dropped on the away one.
func TestNATSIngestHandsSharedWorkOnlyToConnectedMembers(t *testing.T) {
	natsURL := startNATS(t)
	var logs syncBuffer
	addrStays := startBroker(t, natsURL, withNATSIngest)
	addrAway := startBroker(t, natsURL, withNATSIngest, loggingBroker(t, &logs))

	stays := dialRaw(t, addrStays)
	stays.connect(rawConnect("ingest-stays", 300))
	stays.subscribe("$share/ingesters/ingest/jobs/#", packet.QoS1)

	away := dialRaw(t, addrAway)
	away.connect(rawConnect("ingest-gone", 300))
	away.subscribe("$share/ingesters/ingest/jobs/#", packet.QoS1)
	away.drop()
	waitDetached(t, &logs, "ingest-gone")

	nc := plainNATS(t, natsURL)
	const jobs = 30
	for _, n := range numbered(jobs) {
		ingest(t, nc, "ingest.jobs.x", 1, n)
	}

	assert.Equal(t, numbered(jobs), stays.collectPublishes(jobs),
		"every job reaches the connected member [MQTT-4.5.0-1]")
	stays.expectNothing()
}

// QoS 0 is passed on live and not queued, as for an MQTT client: a session that was away does
// not get it.
func TestNATSIngestQoS0IsLiveOnly(t *testing.T) {
	natsURL := startNATS(t)
	var logs syncBuffer
	addr := startBroker(t, natsURL, withNATSIngest, loggingBroker(t, &logs))

	live, _ := connectClient(t, addr, connectOpts("ingest-q0-live"))
	live.subscribe(paho.SubscribeOptions{Topic: "ingest/q0", QoS: 1})
	away := dialRaw(t, addr)
	away.connect(rawConnect("ingest-q0-away", 300))
	away.subscribe("ingest/q0", packet.QoS1)
	away.drop()
	waitDetached(t, &logs, "ingest-q0-away")

	reply := ingest(t, plainNATS(t, natsURL), "ingest.q0", 0, "fleeting")
	assert.Empty(t, reply.Header.Get("Mqtt5-Queue-Seq"), "a QoS 0 message has no queue copy")
	assert.Equal(t, "fleeting", live.expectMessage().Payload)

	back := dialRaw(t, addr)
	back.connect(rawConnect("ingest-q0-away", 300))
	back.expectNothing()
}

// A retained message sent through the ingest subject is stored, as an MQTT one is.
func TestNATSIngestStoresARetainedMessage(t *testing.T) {
	natsURL := startNATS(t)
	addr := startBroker(t, natsURL, withNATSIngest)

	ingest(t, plainNATS(t, natsURL), "ingest.retained", 1, "last-known", "Mqtt5-Retain", "1")

	sub, _ := connectClient(t, addr, connectOpts("ingest-ret"))
	sub.subscribe(paho.SubscribeOptions{Topic: "ingest/retained", QoS: 1})
	got := sub.expectMessage()
	assert.Equal(t, "last-known", got.Payload)
	assert.True(t, got.Retain)
}

// What the broker would refuse from an MQTT client it refuses here, in the reply, and nothing
// is published.
func TestNATSIngestRefusesWhatAnMQTTPublishWouldBeRefused(t *testing.T) {
	natsURL := startNATS(t)
	addr := startBroker(t, natsURL, withNATSIngest, func(o *natsmqtt5.Options) {
		o.MaximumQoS = natsmqtt5.Ptr(uint8(1))
	})
	sub, _ := connectClient(t, addr, connectOpts("ingest-refused"))
	sub.subscribe(paho.SubscribeOptions{Topic: "ingest/qos", QoS: 1})
	nc := plainNATS(t, natsURL)

	reply := ingest(t, nc, "ingest.qos", 2, "too-high")
	assert.Contains(t, reply.Header.Get("Mqtt5-Ingest-Error"), "QoS")
	sub.expectNoMessage()
}

// A topic starting with $ is refused even with RestrictDollarTopics off: $retained and $queue
// are the broker's own subjects, and the ingest subject must not be a way to write them.
func TestNATSIngestRefusesDollarTopics(t *testing.T) {
	natsURL := startNATS(t)
	addr := startBroker(t, natsURL, withNATSIngest)
	nc := plainNATS(t, natsURL)

	reply := ingest(t, nc, "$retained.ingest.forged", 1, "forged", "Mqtt5-Retain", "1")
	assert.Contains(t, reply.Header.Get("Mqtt5-Ingest-Error"), "$")

	sub, _ := connectClient(t, addr, connectOpts("ingest-forged"))
	sub.subscribe(paho.SubscribeOptions{Topic: "ingest/forged", QoS: 1})
	sub.expectNoMessage()
}

// Off by default: no broker answers on the ingest subject.
func TestNATSIngestIsOffByDefault(t *testing.T) {
	natsURL := startNATS(t)
	startBroker(t, natsURL)

	msg := nats.NewMsg("mqtt.$ingest.ingest.off")
	_, err := plainNATS(t, natsURL).RequestMsg(msg, 2*time.Second)
	require.ErrorIs(t, err, nats.ErrNoResponders)
}
