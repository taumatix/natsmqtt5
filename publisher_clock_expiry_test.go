package natsmqtt5_test

import (
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5/v2"
	"github.com/taumatix/natsmqtt5/v2/packet"
)

// delayProxy forwards TCP to NATS and holds back what NATS sends to the client for `delay` while armed: a slow
// leg between the NATS server and one broker, which the embedded server cannot make by itself.
type delayProxy struct {
	ln    net.Listener
	delay time.Duration
	armed atomic.Bool
	mu    sync.Mutex
	conns []net.Conn
}

func newDelayProxy(t *testing.T, natsURL string, delay time.Duration) *delayProxy {
	t.Helper()
	u, err := nats.Connect(natsURL)
	require.NoError(t, err)
	target := u.ConnectedAddr()
	u.Close()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	p := &delayProxy{ln: ln, delay: delay}
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
			out, err := net.Dial("tcp", target)
			if err != nil {
				_ = in.Close()
				continue
			}
			p.mu.Lock()
			p.conns = append(p.conns, in, out)
			p.mu.Unlock()
			go func() { _, _ = copyAll(out, in); _ = out.Close() }()
			go p.fromServer(in, out)
		}
	}()
	return p
}

func copyAll(dst, src net.Conn) (int64, error) {
	buf := make([]byte, 32*1024)
	var total int64
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return total, werr
			}
			total += int64(n)
		}
		if err != nil {
			return total, err
		}
	}
}

// fromServer forwards server-to-client bytes, in order, each chunk no earlier than `delay` after it arrived
// while armed.
func (p *delayProxy) fromServer(in, out net.Conn) {
	type chunk struct {
		b  []byte
		at time.Time
	}
	ch := make(chan chunk, 1024)
	go func() {
		for c := range ch {
			time.Sleep(time.Until(c.at))
			if _, err := in.Write(c.b); err != nil {
				return
			}
		}
	}()
	defer close(ch)
	buf := make([]byte, 32*1024)
	for {
		n, err := out.Read(buf)
		if n > 0 {
			at := time.Now()
			if p.armed.Load() {
				at = at.Add(p.delay)
			}
			ch <- chunk{b: append([]byte(nil), buf[:n]...), at: at}
		}
		if err != nil {
			_ = in.Close()
			return
		}
	}
}

func (p *delayProxy) url() string { return "nats://" + p.ln.Addr().String() }

// publishedWith sends a PUBLISH of 1 through the broker at publisher with a 10 s Message Expiry Interval while
// the receiving broker's leg to NATS is held for the proxy's delay, and returns the interval the subscriber
// on the receiving broker is sent.
func expiryAcrossASlowLeg(t *testing.T, pubOpts, subOpts func(*natsmqtt5.Options)) int64 {
	t.Helper()
	natsURL := startNATS(t)
	slow := newDelayProxy(t, natsURL, 3*time.Second)
	pubAddr := startBroker(t, natsURL, pubOpts)
	subAddr := startBroker(t, slow.url(), subOpts)

	sub := dialRaw(t, subAddr)
	sub.connect(rawConnect("pc-sub", 0))
	sub.subscribe("pc/slow", packet.QoS1)
	pub, _ := connectClient(t, pubAddr, connectOpts("pc-pub"))

	slow.armed.Store(true)
	publishExpiring(pub, "pc/slow", "late", ptrU32(10), false)
	p := sub.readWithin(20 * time.Second).(*packet.Publish)
	return expiryOf(p)
}

func withPublisherClock(o *natsmqtt5.Options) { o.PublisherClockExpiry = true }
func withoutOptions(*natsmqtt5.Options)       {}

// Today a live message's wait starts when the receiving broker's NATS subscription gets it, so 3 s spent in
// NATS between two brokers are not counted [MQTT-3.3.2-6] and the subscriber is told the full 10.
func TestExpiryAcrossASlowNATSLegIsNotCountedByDefault(t *testing.T) {
	t.Parallel()
	assert.EqualValues(t, 10, expiryAcrossASlowLeg(t, withoutOptions, withoutOptions))
}

// With PublisherClockExpiry on both brokers the 3 s count: the publishing broker stamps its receipt time and
// the receiving one counts the wait from it.
func TestPublisherClockExpiryCountsTheTimeSpentInNATSBetweenBrokers(t *testing.T) {
	t.Parallel()
	got := expiryAcrossASlowLeg(t, withPublisherClock, withPublisherClock)
	assert.Equal(t, int64(7), got, "10 s interval, 3 s in NATS")
}

// The stamp is only trusted by a broker that asked to: one without the option ignores it.
func TestPublisherClockExpiryStampIsIgnoredByABrokerWithoutTheOption(t *testing.T) {
	t.Parallel()
	assert.EqualValues(t, 10, expiryAcrossASlowLeg(t, withPublisherClock, withoutOptions))
}

// ... and a publishing broker without the option sends no stamp, so a receiving one has nothing to count from.
func TestPublisherClockExpiryNeedsTheStampFromThePublishingBroker(t *testing.T) {
	t.Parallel()
	assert.EqualValues(t, 10, expiryAcrossASlowLeg(t, withoutOptions, withPublisherClock))
}

// The stamp is on the wire only for a message that expires, and only with the option.
func TestPublisherClockExpiryStampsOnlyMessagesWithAnInterval(t *testing.T) {
	t.Parallel()
	natsURL := startNATS(t)
	on := startBroker(t, natsURL, withPublisherClock)
	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	got := make(chan *nats.Msg, 4)
	_, err = nc.ChanSubscribe(natsmqtt5.DefaultSubjectPrefix+".pc.>", got)
	require.NoError(t, err)
	require.NoError(t, nc.Flush())

	pub, _ := connectClient(t, on, connectOpts("pc-stamp-pub"))
	before := time.Now()
	publishExpiring(pub, "pc/with", "a", ptrU32(5), false)
	publishExpiring(pub, "pc/without", "b", nil, false)

	for range 2 {
		select {
		case m := <-got:
			stamp := m.Header.Get("Mqtt5-Published-At")
			if string(m.Data) == "a" {
				ns, err := strconv.ParseInt(stamp, 10, 64)
				require.NoError(t, err)
				assert.WithinDuration(t, before, time.Unix(0, ns), 5*time.Second)
			} else {
				assert.Empty(t, stamp)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("no message on NATS")
		}
	}
}

// A stamp from the future (a publisher clock ahead of this one) counts as no wait at all: it can lengthen the
// life of a message by at most the skew, never shorten it below the interval it was sent with.
func TestPublisherClockExpiryIgnoresAStampFromTheFuture(t *testing.T) {
	t.Parallel()
	natsURL := startNATS(t)
	addr := startBroker(t, natsURL, withPublisherClock)
	sub := dialRaw(t, addr)
	sub.connect(rawConnect("pc-future-sub", 0))
	sub.subscribe("pc/future", packet.QoS1)

	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	h := nats.Header{}
	h.Set("Mqtt5-Qos", "1")
	h.Set("Mqtt5-Message-Expiry", "10")
	h.Set("Mqtt5-Published-At", strconv.FormatInt(time.Now().Add(time.Hour).UnixNano(), 10))
	require.NoError(t, nc.PublishMsg(&nats.Msg{Subject: natsmqtt5.DefaultSubjectPrefix + ".pc.future", Header: h, Data: []byte("x")}))

	p := sub.readWithin(10 * time.Second).(*packet.Publish)
	assert.EqualValues(t, 10, expiryOf(p))
}
