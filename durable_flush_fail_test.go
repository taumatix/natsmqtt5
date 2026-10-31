package natsmqtt5_test

import (
	"bytes"
	"context"
	"net"
	"sync"
	"sync/atomic"
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

// cutProxy forwards TCP to NATS and, once armed, closes every connection right after it
// forwards the first JetStream publish-ack to the broker: the queue copy is stored, the
// live publish and the flush behind it have not happened.
type cutProxy struct {
	ln       net.Listener
	armed    atomic.Bool
	refuse   atomic.Bool
	stayDown atomic.Bool
	cut      atomic.Int64
	refused  atomic.Int64 // connections turned away while refuse was set
	// swallowAfterAck makes the armed cut stop forwarding client-to-NATS bytes instead of closing, so
	// whatever the broker writes next is lost on a socket that still looks alive. sever closes it later.
	swallowAfterAck atomic.Bool
	swallow         atomic.Bool
	swallowed       atomic.Int64
	// dropQueuePubs makes the proxy discard that many client-to-NATS writes that carry a publish to the
	// queue stream, so the stream never sees them and the broker's publish times out unanswered.
	dropQueuePubs atomic.Int64
	mu            sync.Mutex
	conns         []net.Conn
}

func newCutProxy(t *testing.T, natsURL string) *cutProxy {
	t.Helper()
	u, err := nats.Connect(natsURL)
	require.NoError(t, err)
	target := u.ConnectedAddr()
	u.Close()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	p := &cutProxy{ln: ln}
	t.Cleanup(func() {
		_ = ln.Close()
		p.mu.Lock()
		for _, c := range p.conns {
			_ = c.Close()
		}
		p.mu.Unlock()
	})
	go func() {
		for {
			in, err := ln.Accept()
			if err != nil {
				return
			}
			if p.refuse.Load() {
				p.refused.Add(1)
				_ = in.Close()
				continue
			}
			out, err := net.Dial("tcp", target)
			if err != nil {
				_ = in.Close()
				continue
			}
			p.mu.Lock()
			p.conns = append(p.conns, in, out)
			p.mu.Unlock()
			go func() {
				buf := make([]byte, 32*1024)
				for {
					n, err := in.Read(buf)
					if n > 0 {
						if p.dropQueuePubs.Load() > 0 && bytes.Contains(buf[:n], []byte(".$queue.")) &&
							p.dropQueuePubs.Add(-1) >= 0 {
							p.swallowed.Add(int64(n))
						} else if p.swallow.Load() {
							p.swallowed.Add(int64(n))
						} else if _, werr := out.Write(buf[:n]); werr != nil {
							break
						}
					}
					if err != nil {
						break
					}
				}
				_ = out.Close()
			}()
			go func() {
				buf := make([]byte, 32*1024)
				for {
					n, err := out.Read(buf)
					if n > 0 {
						if _, werr := in.Write(buf[:n]); werr != nil {
							return
						}
						if bytes.Contains(buf[:n], []byte(`"stream"`)) && bytes.Contains(buf[:n], []byte(`"seq"`)) &&
							p.armed.CompareAndSwap(true, false) {
							p.cut.Add(1)
							if p.swallowAfterAck.Load() {
								p.swallow.Store(true)
								continue
							}
							p.refuse.Store(p.stayDown.Load())
							_ = in.Close()
							_ = out.Close()
							return
						}
					}
					if err != nil {
						_ = in.Close()
						return
					}
				}
			}()
		}
	}()
	return p
}

func (p *cutProxy) url() string { return "nats://" + p.ln.Addr().String() }

// sever closes every connection the proxy holds and lets new ones through again.
func (p *cutProxy) sever() {
	p.swallow.Store(false)
	p.mu.Lock()
	for _, c := range p.conns {
		_ = c.Close()
	}
	p.mu.Unlock()
}

// The connection to NATS accepts the live publish and loses it: the write succeeds on a socket whose far end
// is already gone, so the NATS client has nothing buffered to send again, then reconnects and its flush is
// answered. That must not be a PUBACK: the message never reached NATS and the client has to send it again.
func TestDurablePublishIsRefusedWhenTheLivePublishWasLostWithTheConnection(t *testing.T) {
	natsURL := startNATS(t)
	proxy := newCutProxy(t, natsURL)
	proxy.swallowAfterAck.Store(true)
	broker, addr, _ := startBrokerHandle(t, proxy.url(), func(o *natsmqtt5.Options) { o.DurablePublish = true })
	other := startBroker(t, natsURL)

	natsmqtt5.SetPublishFlushGate(broker, func() {
		deadline := time.Now().Add(10 * time.Second)
		for proxy.swallowed.Load() == 0 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		proxy.sever()
		natsmqtt5.AwaitNATSReconnect(broker, 10*time.Second)
	})
	t.Cleanup(func() { natsmqtt5.SetPublishFlushGate(broker, nil) })

	sub, _ := connectClient(t, other, connectOpts("dp-lost-sub"))
	sub.subscribe(paho.SubscribeOptions{Topic: "dp/lost", QoS: 1})

	pub := dialRaw(t, addr)
	pub.connect(rawConnect("dp-lost-pub", 0))
	proxy.armed.Store(true)
	rawPublishQoS(pub, packet.QoS1, 1, "dp/lost", "gone")
	ack, ok := pub.readWithin(20 * time.Second).(*packet.Puback)
	require.True(t, ok)
	require.EqualValues(t, 1, proxy.cut.Load(), "the proxy cut the connection once")
	require.Positive(t, proxy.swallowed.Load(), "the live publish was written into the lost connection")
	assert.Equal(t, packet.ImplementationSpecificError, ack.ReasonCode,
		"an acknowledged publish whose live copy was lost would never reach the subscriber")
	sub.expectNoMessage()
}

// The connection to NATS is cut after the queue stream stored the message and before the live publish was
// flushed. The client library buffers the publish across the reconnect and the flush completes once it is
// back, so the PUBACK is a success and the subscriber on another broker gets the message, once.
func TestDurablePublishSurvivesNATSBeingCutAfterTheQueueCopyIsStored(t *testing.T) {
	natsURL := startNATS(t)
	proxy := newCutProxy(t, natsURL)
	// The cut turns the broker's reconnect attempts away until the live publish is made, so that the
	// publish happens while the NATS client is reconnecting. Left to chance, a publish made before the
	// client noticed the cut is written to a dead socket and is refused, as it must be, and one made
	// after a reconnect was never buffered: the test then proved either of two other things.
	proxy.stayDown.Store(true)
	durable, durableAddr, _ := startBrokerHandle(t, proxy.url(), func(o *natsmqtt5.Options) { o.DurablePublish = true })
	natsmqtt5.SetPublishQueuedGate(durable, func([]byte) {
		assert.True(t, natsmqtt5.AwaitNATSDisconnect(durable, 10*time.Second), "the broker's NATS client never saw the cut")
		deadline := time.Now().Add(10 * time.Second)
		for proxy.refused.Load() == 0 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		// One reconnect attempt has been turned away; the next comes a ReconnectWait later, after the
		// publish below has been buffered.
		proxy.refuse.Store(false)
		proxy.stayDown.Store(false)
	})
	t.Cleanup(func() { natsmqtt5.SetPublishQueuedGate(durable, nil) })
	other := startBroker(t, natsURL)

	sub, _ := connectClient(t, other, connectOpts("dp-cut-sub"))
	sub.subscribe(paho.SubscribeOptions{Topic: "dp/cut", QoS: 1})

	pub := dialRaw(t, durableAddr)
	pub.connect(rawConnect("dp-cut-pub", 0))
	proxy.armed.Store(true)
	rawPublishQoS(pub, packet.QoS1, 1, "dp/cut", "survived")
	ack, ok := pub.readWithin(20 * time.Second).(*packet.Puback)
	require.True(t, ok)
	require.EqualValues(t, 1, proxy.cut.Load(), "the proxy cut the connection once")
	assert.Equal(t, packet.Success, ack.ReasonCode)

	assert.Equal(t, "survived", sub.expectMessage().Payload)
	sub.expectNoMessage()
}

// When NATS stays unreachable the flush times out, so the PUBACK is refused with 0x83 and the client may send
// it again. The queue stream already holds the first copy, which is why a retry can leave a duplicate there.
func TestDurablePublishRefusesWhenNATSStaysUnreachableAfterTheQueueCopyIsStored(t *testing.T) {
	t.Parallel()
	natsURL := startNATS(t)
	proxy := newCutProxy(t, natsURL)
	proxy.stayDown.Store(true)
	durable := startBroker(t, proxy.url(), func(o *natsmqtt5.Options) { o.DurablePublish = true })

	pub := dialRaw(t, durable)
	pub.connect(rawConnect("dp-down-pub", 0))
	proxy.armed.Store(true)
	rawPublishQoS(pub, packet.QoS1, 1, "dp/down", "unconfirmed")
	ack, ok := pub.readWithin(20 * time.Second).(*packet.Puback)
	require.True(t, ok)
	require.EqualValues(t, 1, proxy.cut.Load(), "the proxy cut the connection once")
	assert.Equal(t, packet.ImplementationSpecificError, ack.ReasonCode)
	assert.Equal(t, "unconfirmed", queuedPayload(t, queueStream(t, natsURL), "dp.down"))
}

// queuedCopies is how many messages the queue stream holds for one topic's subject.
func queuedCopies(t *testing.T, s jetstream.Stream, subject string) int {
	t.Helper()
	full := natsmqtt5.DefaultSubjectPrefix + ".$queue." + subject
	info, err := s.Info(context.Background(), jetstream.WithSubjectFilter(full))
	require.NoError(t, err)
	return int(info.State.Subjects[full])
}

// The 0x83 above tells the client to send the message again, and the queue stream already holds the first
// copy, so the retry stores a second one: a session that was away is offered the message twice. QoS 1
// allows that ("at least once", [MQTT-4.3.2-2]); this test pins the behaviour and ROADMAP.md records the opt-in that avoids it
// is left: dropping the second copy needs an id the client controls per message, and the packet id is not
// one (it is reused after its PUBACK, so a later, different message would be dropped as the duplicate).
func TestDurablePublishRetryAfterARefusedFlushLeavesASecondQueueCopy(t *testing.T) {
	t.Parallel()
	natsURL := startNATS(t)
	proxy := newCutProxy(t, natsURL)
	proxy.stayDown.Store(true)
	durable := startBroker(t, proxy.url(), func(o *natsmqtt5.Options) { o.DurablePublish = true })

	pub := dialRaw(t, durable)
	pub.connect(rawConnect("dp-retry-pub", 0))
	proxy.armed.Store(true)
	rawPublishQoS(pub, packet.QoS1, 1, "dp/retry", "twice")
	ack, ok := pub.readWithin(20 * time.Second).(*packet.Puback)
	require.True(t, ok)
	require.Equal(t, packet.ImplementationSpecificError, ack.ReasonCode)
	stream := queueStream(t, natsURL)
	require.Equal(t, 1, queuedCopies(t, stream, "dp.retry"))

	// NATS is reachable again; the client sends the same message again, as the refusal asks.
	proxy.refuse.Store(false)
	require.Eventually(t, func() bool {
		pub.send(&packet.Publish{Topic: "dp/retry", QoS: packet.QoS1, PacketID: 1, Dup: true, Payload: []byte("twice")})
		a, ok := pub.readWithin(20 * time.Second).(*packet.Puback)
		return ok && a.ReasonCode == packet.Success
	}, 60*time.Second, time.Second)
	assert.Equal(t, 2, queuedCopies(t, stream, "dp.retry"),
		"the retry stored a second copy; if this is 1 the roadmap's duplicate is gone and the entry must go too")
}

// [MQTT-3.1.2-8] "The Will Message MUST be published after the Network Connection is subsequently closed ..."
// (§3.1.2.5; wording as in will_durable_test.go). A session that was away is only offered the Will if it
// is in the queue stream, and a Will has no client to refuse to: when the stream does not answer the
// broker's first try, with DurablePublish on it tries again. The proxy loses the first publish to the
// queue stream; the retry's reaches it. Without the retry the stream holds nothing and the Will reaches
// only the subscribers connected at that moment.
func TestMQTT_3_1_2_8_DurablePublishRetriesAWillsQueueCopyWhenTheQueueDoesNotAnswer(t *testing.T) {
	natsURL := startNATS(t)
	proxy := newCutProxy(t, natsURL)
	_, addr, _ := startBrokerHandle(t, proxy.url(), func(o *natsmqtt5.Options) { o.DurablePublish = true })
	other := startBroker(t, natsURL)

	sub, _ := connectClient(t, other, connectOpts("will-retry-sub"))
	sub.subscribe(paho.SubscribeOptions{Topic: "will/retry", QoS: 1})

	victim := dialRaw(t, addr)
	cp := rawConnect("will-retry-victim", 0)
	cp.Will = &packet.Will{Topic: "will/retry", Payload: []byte("gone"), QoS: packet.QoS1}
	victim.connect(cp)

	proxy.dropQueuePubs.Store(1)
	require.NoError(t, victim.nc.Close()) // no DISCONNECT: the Will is published

	// The first try waits out offlineKeepTimeout (5 s), so the live Will follows the retry.
	stream := queueStream(t, natsURL)
	require.Eventually(t, func() bool { return queuedCopies(t, stream, "will.retry") > 0 },
		30*time.Second, 100*time.Millisecond, "the retry stored the Will's copy for a session that is away")
	assert.Equal(t, "gone", queuedPayload(t, stream, "will.retry"))
	assert.Equal(t, "gone", sub.expectMessage().Payload)
	assert.Zero(t, proxy.dropQueuePubs.Load(), "the proxy lost the first queue publish")
}

// readWithin is read with a longer deadline, for a reply that waits on a reconnect.
func (c *rawClient) readWithin(d time.Duration) packet.Packet {
	c.t.Helper()
	require.NoError(c.t, c.nc.SetReadDeadline(time.Now().Add(d)))
	p, err := packet.Read(c.r, 0)
	require.NoError(c.t, err)
	return p
}
