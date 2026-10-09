package natsmqtt5_test

import (
	"bytes"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/nats-io/nats.go"
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
	mu       sync.Mutex
	conns    []net.Conn
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
			go func() { _, _ = io.Copy(out, in); _ = out.Close() }()
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

// The connection to NATS is cut after the queue stream stored the message and before the live publish was
// flushed. The client library buffers the publish across the reconnect and the flush completes once it is
// back, so the PUBACK is a success and the subscriber on another broker gets the message, once.
func TestDurablePublishSurvivesNATSBeingCutAfterTheQueueCopyIsStored(t *testing.T) {
	natsURL := startNATS(t)
	proxy := newCutProxy(t, natsURL)
	durable := startBroker(t, proxy.url(), func(o *natsmqtt5.Options) { o.DurablePublish = true })
	other := startBroker(t, natsURL)

	sub, _ := connectClient(t, other, connectOpts("dp-cut-sub"))
	sub.subscribe(paho.SubscribeOptions{Topic: "dp/cut", QoS: 1})

	pub := dialRaw(t, durable)
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

// readWithin is read with a longer deadline, for a reply that waits on a reconnect.
func (c *rawClient) readWithin(d time.Duration) packet.Packet {
	c.t.Helper()
	require.NoError(c.t, c.nc.SetReadDeadline(time.Now().Add(d)))
	p, err := packet.Read(c.r, 0)
	require.NoError(c.t, err)
	return p
}
