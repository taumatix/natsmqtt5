package natsmqtt5_test

import (
	"io"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// Options.QueuePlainPublishes (opt-in): a message a plain NATS client
// publishes straight on a live subject, with an Mqtt5-Qos header of 1 or 2, is queued.
//
// Without the option such a message reached the sessions subscribed to it and no queue: a session
// that was away never got it. With it the brokers leave the first copy to NATS subscribers and
// publish it again through the path an MQTT PUBLISH takes, so the copy MQTT sessions are
// delivered has its id and queue sequence. The statements at stake are [MQTT-4.5.0-1] (a
// session's messages are part of its state) and §4.8.2's shared delivery; the specification
// text was not re-read for this change.

func queuePlain(o *natsmqtt5.Options) { o.QueuePlainPublishes = true }

// plainPublish publishes payload on mqtt.<subject> as a NATS client with no broker in the path,
// with an Mqtt5-Qos header when qos is not negative.
func plainPublish(t *testing.T, nc *nats.Conn, subject string, qos int, payload string) {
	t.Helper()
	msg := nats.NewMsg("mqtt." + subject)
	msg.Data = []byte(payload)
	if qos >= 0 {
		msg.Header.Set("Mqtt5-Qos", string(rune('0'+qos)))
	}
	require.NoError(t, nc.PublishMsg(msg))
	require.NoError(t, nc.Flush())
}

// A session that was away when a plain NATS client published at QoS 1 on its live subject gets
// the message when it comes back, from the queue copy a broker made; it gets it once.
func TestAPlainNATSPublishAtQoS1IsQueuedForASessionThatWasAway_MQTT_4_5_0_1(t *testing.T) {
	natsURL := startNATS(t)
	var logs syncBuffer
	addr := startBroker(t, natsURL, queuePlain, loggingBroker(t, &logs))
	startBroker(t, natsURL, queuePlain) // a second broker: whichever takes the publish, the first finds its copy

	away := dialRaw(t, addr)
	away.connect(rawConnect("plain-away", 300))
	away.subscribe("plain/away", packet.QoS1)
	away.drop()
	waitDetached(t, &logs, "plain-away")

	plainPublish(t, plainNATS(t, natsURL), "plain.away", 1, "kept")

	back := dialRaw(t, addr)
	back.connect(rawConnect("plain-away", 300))
	p := back.expectPublish()
	assert.Equal(t, "plain/away", p.Topic)
	assert.Equal(t, "kept", string(p.Payload))
	assert.Equal(t, packet.QoS1, p.QoS)
	back.send(&packet.Puback{Ack: packet.Ack{PacketID: p.PacketID}})
	back.expectNothing()
}

// Off (the default) is v1.1.0: the same publish is delivered live and not queued, so the
// session that was away never gets it.
func TestWithoutQueuePlainPublishesAPlainPublishIsNotQueued(t *testing.T) {
	natsURL := startNATS(t)
	var logs syncBuffer
	addr := startBroker(t, natsURL, loggingBroker(t, &logs))

	live, _ := connectClient(t, addr, connectOpts("plain-v1-live"))
	live.subscribe(paho.SubscribeOptions{Topic: "plain/v1", QoS: 1})
	away := dialRaw(t, addr)
	away.connect(rawConnect("plain-v1-away", 300))
	away.subscribe("plain/v1", packet.QoS1)
	away.drop()
	waitDetached(t, &logs, "plain-v1-away")

	plainPublish(t, plainNATS(t, natsURL), "plain.v1", 1, "unqueued")
	assert.Equal(t, "unqueued", live.expectMessage().Payload, "delivered live, as in v1")

	back := dialRaw(t, addr)
	back.connect(rawConnect("plain-v1-away", 300))
	back.expectNothing()
}

// A connected subscriber, on any broker, is delivered the message once, not once for the plain
// publish and once for the broker's copy.
func TestAPlainNATSPublishAtQoS1ReachesAConnectedSubscriberOnce(t *testing.T) {
	natsURL := startNATS(t)
	startBroker(t, natsURL, queuePlain)
	addr := startBroker(t, natsURL, queuePlain)

	sub, _ := connectClient(t, addr, connectOpts("plain-live"))
	sub.subscribe(paho.SubscribeOptions{Topic: "plain/live", QoS: 2})

	plainPublish(t, plainNATS(t, natsURL), "plain.live", 2, "once")
	got := sub.expectMessage()
	assert.Equal(t, "once", got.Payload)
	assert.Equal(t, byte(2), got.QoS)
	sub.expectNoMessage()
}

// A plain NATS subscriber sees the publish twice: the original, with no id, and the broker's
// copy, which has one. This is the wire change a plain subscriber has to know about.
func TestAPlainNATSSubscriberSeesTheOriginalAndTheBrokersCopyWithAnID(t *testing.T) {
	natsURL := startNATS(t)
	startBroker(t, natsURL, queuePlain)
	nc := plainNATS(t, natsURL)
	got := make(chan *nats.Msg, 4)
	_, err := nc.ChanSubscribe("mqtt.plain.copies", got)
	require.NoError(t, err)

	plainPublish(t, nc, "plain.copies", 1, "twice")

	var ids []string
	for range 2 {
		select {
		case m := <-got:
			assert.Equal(t, "twice", string(m.Data))
			ids = append(ids, m.Header.Get("Mqtt5-Msg-Id"))
		case <-time.After(5 * time.Second):
			require.Fail(t, "a plain subscriber should see two copies", "saw %d", len(ids))
		}
	}
	assert.ElementsMatch(t, []bool{false, true}, []bool{ids[0] != "", ids[1] != ""},
		"one copy has no id, the broker's has one")
	select {
	case m := <-got:
		require.Failf(t, "a third copy", "%q", m.Data)
	case <-time.After(300 * time.Millisecond):
	}
}

// What does not ask for a queue is not touched: no header and QoS 0 are delivered live at once,
// as in v1, and are not replayed to a session that was away.
func TestAPlainNATSPublishWithoutAQoSOfOneOrTwoIsDeliveredLiveAndNotQueued(t *testing.T) {
	natsURL := startNATS(t)
	var logs syncBuffer
	addr := startBroker(t, natsURL, queuePlain, loggingBroker(t, &logs))

	live, _ := connectClient(t, addr, connectOpts("plain-q0-live"))
	live.subscribe(paho.SubscribeOptions{Topic: "plain/q0", QoS: 1})
	away := dialRaw(t, addr)
	away.connect(rawConnect("plain-q0-away", 300))
	away.subscribe("plain/q0", packet.QoS1)
	away.drop()
	waitDetached(t, &logs, "plain-q0-away")

	nc := plainNATS(t, natsURL)
	plainPublish(t, nc, "plain.q0", -1, "no header")
	plainPublish(t, nc, "plain.q0", 0, "qos zero")
	assert.Equal(t, "no header", live.expectMessage().Payload)
	assert.Equal(t, "qos zero", live.expectMessage().Payload)
	live.expectNoMessage()

	back := dialRaw(t, addr)
	back.connect(rawConnect("plain-q0-away", 300))
	back.expectNothing()
}

// A shared group with one member away: every plain QoS 1 job reaches the member that is
// connected, through the group's backlog, as it does for a job published by an MQTT client.
func TestAPlainNATSPublishHandsSharedWorkOnlyToConnectedMembers_MQTT_4_5_0_1(t *testing.T) {
	natsURL := startNATS(t)
	var logs syncBuffer
	addrStays := startBroker(t, natsURL, queuePlain)
	addrAway := startBroker(t, natsURL, queuePlain, loggingBroker(t, &logs))

	stays := dialRaw(t, addrStays)
	stays.connect(rawConnect("plain-stays", 300))
	stays.subscribe("$share/plainers/plain/jobs/#", packet.QoS1)
	away := dialRaw(t, addrAway)
	away.connect(rawConnect("plain-gone", 300))
	away.subscribe("$share/plainers/plain/jobs/#", packet.QoS1)
	away.drop()
	waitDetached(t, &logs, "plain-gone")

	nc := plainNATS(t, natsURL)
	const jobs = 20
	for _, n := range numbered(jobs) {
		plainPublish(t, nc, "plain.jobs.x", 1, n)
	}

	assert.Equal(t, numbered(jobs), stays.collectPublishes(jobs), "every job reaches the connected member")
	stays.expectNothing()
}

// slowUplink forwards TCP to a NATS server, holding each chunk the client writes for delay before
// it goes on. Replies are not held. What a client sent is therefore registered with the server
// late, which a loaded CI machine does by accident.
type slowUplink struct{ ln net.Listener }

func newSlowUplink(t *testing.T, natsURL string, delay time.Duration) *slowUplink {
	t.Helper()
	u, err := url.Parse(natsURL)
	require.NoError(t, err)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			in, err := ln.Accept()
			if err != nil {
				return
			}
			out, err := net.Dial("tcp", u.Host)
			if err != nil {
				_ = in.Close()
				continue
			}
			t.Cleanup(func() { _ = in.Close(); _ = out.Close() })
			type chunk struct {
				at   time.Time
				data []byte
			}
			chunks := make(chan chunk, 1024)
			go func() {
				defer close(chunks)
				buf := make([]byte, 32<<10)
				for {
					n, err := in.Read(buf)
					if n > 0 {
						chunks <- chunk{time.Now().Add(delay), append([]byte(nil), buf[:n]...)}
					}
					if err != nil {
						return
					}
				}
			}()
			go func() {
				for c := range chunks {
					time.Sleep(time.Until(c.at))
					if _, err := out.Write(c.data); err != nil {
						return
					}
				}
				_ = out.Close()
			}()
			go func() { _, _ = io.Copy(in, out); _ = in.Close() }()
		}
	}()
	return &slowUplink{ln: ln}
}

func (p *slowUplink) url() string { return "nats://" + p.ln.Addr().String() }

// A publish made the moment a broker is up gets its queue copy: New returns only once the server
// has the broker's subscription to the live subjects. Without that, a plain publish just after New
// reached the plain subscriber and no broker (macOS CI saw one copy of two).
func TestAPlainPublishRightAfterTheBrokerIsUpGetsItsQueueCopy(t *testing.T) {
	natsURL := startNATS(t)
	slow := newSlowUplink(t, natsURL, 400*time.Millisecond)
	nc := plainNATS(t, natsURL)
	got := make(chan *nats.Msg, 4)
	_, err := nc.ChanSubscribe("mqtt.plain.rightaway", got)
	require.NoError(t, err)
	require.NoError(t, nc.Flush())

	startBroker(t, slow.url(), queuePlain)
	plainPublish(t, nc, "plain.rightaway", 1, "now")

	for i := range 2 {
		select {
		case <-got:
		case <-time.After(5 * time.Second):
			require.Failf(t, "the broker's copy never came", "saw %d of 2", i)
		}
	}
}
