package natsmqtt5_test

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
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

// Options.DurablePublish: a PUBACK / PUBREC that means the message is safe.
//
// The option is an extension, not a numbered MUST: no [MQTT-x.y.z-n] statement
// requires a PUBACK to mean durability. What it touches is the Reason Codes a
// PUBACK and a PUBREC may carry, whose valid set the CONFORMANCE rows
// MQTT-3.4.2-1 and MQTT-3.5.2-1 record as including 0x00 Success and 0x83
// Implementation specific error (the broker's own record; the specification
// text was not re-read for this change), and the delivery prose of MQTT-5.0
// §4.3.2 and §4.3.3: QoS 1 at least once, QoS 2 exactly once. A refusal must
// not look like a success, and a success must not deliver twice.

func durableBroker(t *testing.T, natsURL string) string {
	return startBroker(t, natsURL, func(o *natsmqtt5.Options) { o.DurablePublish = true })
}

// queueStream opens the offline queue's stream on the server the broker uses.
func queueStream(t *testing.T, natsURL string) jetstream.Stream {
	t.Helper()
	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	s, err := js.Stream(context.Background(), natsmqtt5.DefaultStreamPrefix+"_queue")
	require.NoError(t, err)
	return s
}

func queuedPayload(t *testing.T, s jetstream.Stream, subject string) string {
	t.Helper()
	m, err := s.GetLastMsgForSubject(context.Background(), natsmqtt5.DefaultSubjectPrefix+".$queue."+subject)
	require.NoError(t, err, "the acknowledged message must be in the queue stream")
	return string(m.Data)
}

func rawPublishQoS(c *rawClient, qos packet.QoS, id uint16, topic, payload string) {
	c.send(&packet.Publish{Topic: topic, QoS: qos, PacketID: id, Payload: []byte(payload)})
}

// An acknowledged QoS 1 message is in the stream and reaches a connected
// subscriber once: the live publish and the queue copy do not both deliver.
// [MQTT-4.3.2-2]: QoS 1 delivery at least once, and here also not twice.
func TestDurablePublishQoS1AckMeansStoredAndDeliveredOnce(t *testing.T) {
	natsURL := startNATS(t)
	addr := durableBroker(t, natsURL)

	sub, _ := connectClient(t, addr, connectOpts("dp-sub"))
	_, err := sub.Subscribe(context.Background(), &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: "dp/one", QoS: 1}}})
	require.NoError(t, err)

	pub := dialRaw(t, addr)
	pub.connect(rawConnect("dp-pub", 0))
	rawPublishQoS(pub, packet.QoS1, 7, "dp/one", "safe")

	ack, ok := pub.read().(*packet.Puback)
	require.True(t, ok, "a QoS 1 PUBLISH is answered with a PUBACK")
	assert.Equal(t, uint16(7), ack.PacketID)
	assert.Equal(t, packet.Success, ack.ReasonCode)

	assert.Equal(t, "safe", queuedPayload(t, queueStream(t, natsURL), "dp.one"))
	assert.Equal(t, "safe", sub.expectMessage().Payload)
	sub.expectNoMessage()
}

// The QoS 2 path: PUBREC only once the message is stored, then PUBREL / PUBCOMP
// as usual, and the subscriber gets it exactly once [MQTT-4.3.3-2].
func TestDurablePublishQoS2RecIsAcknowledgedAfterStorageAndDeliveredOnce(t *testing.T) {
	natsURL := startNATS(t)
	addr := durableBroker(t, natsURL)

	sub, _ := connectClient(t, addr, connectOpts("dp2-sub"))
	_, err := sub.Subscribe(context.Background(), &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: "dp/two", QoS: 2}}})
	require.NoError(t, err)

	pub := dialRaw(t, addr)
	pub.connect(rawConnect("dp2-pub", 0))
	rawPublishQoS(pub, packet.QoS2, 9, "dp/two", "twice-safe")

	rec, ok := pub.read().(*packet.Pubrec)
	require.True(t, ok, "a QoS 2 PUBLISH is answered with a PUBREC")
	assert.Equal(t, uint16(9), rec.PacketID)
	assert.Equal(t, packet.Success, rec.ReasonCode)
	assert.Equal(t, "twice-safe", queuedPayload(t, queueStream(t, natsURL), "dp.two"))

	pub.send(&packet.Pubrel{Ack: packet.Ack{PacketID: 9}})
	comp, ok := pub.read().(*packet.Pubcomp)
	require.True(t, ok)
	assert.Equal(t, packet.Success, comp.ReasonCode)

	assert.Equal(t, "twice-safe", sub.expectMessage().Payload)
	sub.expectNoMessage()
}

// With the stream gone the broker cannot keep what it is asked to keep, and
// must say so with an error Reason Code rather than a PUBACK / PUBREC that
// means success. Nothing is delivered live either: a refused publish that
// still reached subscribers would be delivered again when the client resends.
func TestDurablePublishWithoutTheStreamRefusesInsteadOfAcknowledging(t *testing.T) {
	natsURL := startNATS(t)
	addr := durableBroker(t, natsURL)

	sub, _ := connectClient(t, addr, connectOpts("dpx-sub"))
	_, err := sub.Subscribe(context.Background(), &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: "dp/gone", QoS: 1}}})
	require.NoError(t, err)

	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	require.NoError(t, js.DeleteStream(context.Background(), natsmqtt5.DefaultStreamPrefix+"_queue"))

	pub := dialRaw(t, addr)
	pub.connect(rawConnect("dpx-pub", 0))

	rawPublishQoS(pub, packet.QoS1, 1, "dp/gone", "lost")
	ack, ok := pub.read().(*packet.Puback)
	require.True(t, ok)
	assert.Equal(t, packet.ImplementationSpecificError, ack.ReasonCode, "PUBACK must be 0x83, not Success")

	rawPublishQoS(pub, packet.QoS2, 2, "dp/gone", "lost")
	rec, ok := pub.read().(*packet.Pubrec)
	require.True(t, ok)
	assert.Equal(t, packet.ImplementationSpecificError, rec.ReasonCode, "PUBREC must be 0x83, not Success")

	sub.expectNoMessage()
}

// QoS 0 has no acknowledgement to wait for and is not queued, so it is neither
// slowed nor stored by the option.
func TestDurablePublishLeavesQoS0Alone(t *testing.T) {
	natsURL := startNATS(t)
	addr := durableBroker(t, natsURL)

	sub, _ := connectClient(t, addr, connectOpts("dp0-sub"))
	_, err := sub.Subscribe(context.Background(), &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: "dp/zero", QoS: 0}}})
	require.NoError(t, err)
	pub, _ := connectClient(t, addr, connectOpts("dp0-pub"))
	pub.publish(&paho.Publish{Topic: "dp/zero", QoS: 0, Payload: []byte("z")})

	assert.Equal(t, "z", sub.expectMessage().Payload)
	info, err := queueStream(t, natsURL).Info(context.Background())
	require.NoError(t, err)
	assert.Zero(t, info.State.Msgs)
}

func TestDurablePublishOptionsAreValidated(t *testing.T) {
	_, err := natsmqtt5.New(natsmqtt5.Options{DurablePublish: true, DisableOfflineQueue: true})
	require.ErrorIs(t, err, natsmqtt5.ErrInvalidOptions)

	// No JetStream on the server: the queue it needs cannot exist, so the
	// broker refuses to start instead of acknowledging what it cannot keep.
	_, err = natsmqtt5.New(natsmqtt5.Options{
		NATSURL: startNATSWithoutJetStream(t), Listen: "127.0.0.1:0", DurablePublish: true,
		Logger: nil,
	})
	require.Error(t, err)
}

// The option's cost, shown rather than asserted from a timer on the broker: the
// NATS connection goes through a proxy that holds every chunk back by delay in
// each direction. The default (queue on) waits one JetStream round trip before
// the PUBACK; DurablePublish adds the flush, a second one.
func TestDurablePublishAddsOneNATSRoundTripToThePUBACK(t *testing.T) {
	const delay = 50 * time.Millisecond
	natsURL := startNATS(t)
	proxy := slowProxy(t, natsURL, delay)

	plain := startBroker(t, proxy)
	durable := startBroker(t, proxy, func(o *natsmqtt5.Options) { o.DurablePublish = true })

	fastest := func(addr, id string) time.Duration {
		c := dialRaw(t, addr)
		c.connect(rawConnect(id, 0))
		best := time.Hour
		for i := 1; i <= 5; i++ {
			start := time.Now()
			rawPublishQoS(c, packet.QoS1, uint16(i), "dp/rtt", "x")
			ack, ok := c.read().(*packet.Puback)
			require.True(t, ok)
			require.Equal(t, packet.Success, ack.ReasonCode)
			best = min(best, time.Since(start))
		}
		return best
	}
	withQueue, withFlush := fastest(plain, "rtt-plain"), fastest(durable, "rtt-durable")
	t.Logf("PUBACK after %v with the queue, %v with DurablePublish (one-way delay %v)", withQueue, withFlush, delay)
	assert.GreaterOrEqual(t, withFlush-withQueue, 3*delay/2, "the flush is a further round trip (2 x delay) to NATS")
}

// slowProxy forwards TCP to the NATS server at natsURL, holding each chunk back
// by delay in each direction, and returns the URL to dial it at.
func slowProxy(t *testing.T, natsURL string, delay time.Duration) string {
	t.Helper()
	u, err := nats.Connect(natsURL)
	require.NoError(t, err)
	target := u.ConnectedAddr()
	u.Close()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	var wg sync.WaitGroup
	t.Cleanup(func() { _ = ln.Close(); wg.Wait() })
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			in, err := ln.Accept()
			if err != nil {
				return
			}
			out, err := net.Dial("tcp", target)
			if err != nil {
				_ = in.Close()
				continue
			}
			t.Cleanup(func() { _ = in.Close(); _ = out.Close() })
			pipe := func(dst, src net.Conn) {
				buf := make([]byte, 32*1024)
				for {
					n, err := src.Read(buf)
					if n > 0 {
						chunk := append([]byte(nil), buf[:n]...)
						time.Sleep(delay)
						if _, werr := dst.Write(chunk); werr != nil {
							return
						}
					}
					if err != nil {
						if !errors.Is(err, io.EOF) {
							_ = dst.Close()
						}
						return
					}
				}
			}
			go pipe(out, in)
			go pipe(in, out)
		}
	}()
	return "nats://" + ln.Addr().String()
}
