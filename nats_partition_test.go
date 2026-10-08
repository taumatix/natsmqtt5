package natsmqtt5_test

import (
	"io"
	"net"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
)

// A broker that is alive but cut off from NATS still holds its clients, so the
// Will of one of them must not be published while its connection is open. The
// statement, MQTT-5.0 §3.1.2.5, read from the OASIS text on 2026-10-08:
//
//	[MQTT-3.1.2-8]  "The Will Message MUST be published after the Network
//	                 Connection is subsequently closed and either the Will Delay
//	                 Interval has elapsed or the Session ends, unless ..."
//	[MQTT-3.1.3-9]  "If a new Network Connection to this Session is made before
//	                 the Will Delay Interval has passed, the Server MUST NOT send
//	                 the Will Message."
//
// The first half of -8 is the point: a Will goes out after the connection is
// closed, never while it is open. The tests cut a real broker off from a real
// NATS server with a TCP proxy that stops forwarding without closing anything
// (what a network partition looks like), and watch the client's own socket.

// partitionProxy forwards TCP to a NATS server and can stop forwarding without
// closing either side, then resume.
type partitionProxy struct {
	ln     net.Listener
	target string

	mu     sync.Mutex
	paused bool
	cond   *sync.Cond
	conns  []net.Conn
}

func newPartitionProxy(t *testing.T, natsURL string) *partitionProxy {
	t.Helper()
	u, err := url.Parse(natsURL)
	require.NoError(t, err)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	p := &partitionProxy{ln: ln, target: u.Host}
	p.cond = sync.NewCond(&p.mu)
	go p.accept()
	t.Cleanup(p.close)
	return p
}

func (p *partitionProxy) url() string { return "nats://" + p.ln.Addr().String() }

func (p *partitionProxy) accept() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		up, err := net.Dial("tcp", p.target)
		if err != nil {
			_ = c.Close()
			continue
		}
		p.mu.Lock()
		p.conns = append(p.conns, c, up)
		p.mu.Unlock()
		go p.pipe(c, up)
		go p.pipe(up, c)
	}
}

func (p *partitionProxy) pipe(from, to net.Conn) {
	buf := make([]byte, 32<<10)
	for {
		n, err := from.Read(buf)
		if n > 0 {
			p.mu.Lock()
			for p.paused {
				p.cond.Wait()
			}
			p.mu.Unlock()
			if _, werr := to.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (p *partitionProxy) setPaused(v bool) {
	p.mu.Lock()
	p.paused = v
	p.mu.Unlock()
	p.cond.Broadcast()
}

func (p *partitionProxy) close() {
	_ = p.ln.Close()
	p.setPaused(false)
	p.mu.Lock()
	for _, c := range p.conns {
		_ = c.Close()
	}
	p.mu.Unlock()
}

// closedAt reports, on the returned channel, when the client's socket ends.
func (c *rawClient) closedAt() <-chan time.Time {
	ch := make(chan time.Time, 1)
	go func() {
		_, _ = io.Copy(io.Discard, c.nc)
		ch <- time.Now()
	}()
	return ch
}

func TestMQTT_3_1_2_8_ABrokerCutOffFromNATSClosesItsClientsBeforeTheirWillIsPublished(t *testing.T) {
	url := startNATS(t)
	proxy := newPartitionProxy(t, url)
	cut, cutAddr, _ := startBrokerHandle(t, proxy.url(), fastWills)
	survivor := startBroker(t, url, fastWills)
	watcher := newWillWatcher(t, survivor)

	client := dialRaw(t, cutAddr)
	client.connect(rawConnectWithWillMessage("cutoff", "status/cutoff", "gone"))
	closed := client.closedAt()
	require.Equal(t, 1, willKeys(t, url))

	proxy.setPaused(true) // the partition: nothing flows, nothing is closed
	start := time.Now()

	var got received
	select {
	case got = <-watcher.messages:
	case <-time.After(20 * time.Second):
		t.Fatal("no Will after the broker was cut off")
	}
	willAt := time.Now()
	assert.Equal(t, "gone", got.Payload)

	select {
	case closedAt := <-closed:
		assert.False(t, closedAt.After(willAt),
			"[MQTT-3.1.2-8] the Will was published while the client's connection was still open")
		assert.Less(t, closedAt.Sub(start), 5*time.Second, "the cut-off broker fenced its clients promptly")
	case <-time.After(time.Second):
		t.Fatal("[MQTT-3.1.2-8] the Will was published and the client's connection is still open")
	}
	watcher.expectNoMessageFor(time.Second) // once
	natsmqtt5.Kill(cut)
}

// A pause shorter than the broker's patience is a blip: the client stays
// connected, nothing is published, and its Will is still its own afterwards.
func TestMQTT_3_1_3_9_ABriefPauseOfNATSClosesNothingAndPublishesNothing(t *testing.T) {
	url := startNATS(t)
	proxy := newPartitionProxy(t, url)
	_, addr, _ := startBrokerHandle(t, proxy.url(), fastWills)
	survivor := startBroker(t, url, fastWills)
	watcher := newWillWatcher(t, survivor)

	client := dialRaw(t, addr)
	client.connect(rawConnectWithWillMessage("blip", "status/blip", "gone"))
	closed := client.closedAt()

	proxy.setPaused(true)
	time.Sleep(80 * time.Millisecond)
	proxy.setPaused(false)

	watcher.expectNoMessageFor(2 * time.Second)
	select {
	case <-closed:
		t.Fatal("a pause shorter than the broker's patience closed the client")
	default:
	}
	assert.Equal(t, 1, willKeys(t, url), "the Will record is still held")

	client.drop()
	assert.Equal(t, "gone", watcher.expectMessage().Payload)
}
