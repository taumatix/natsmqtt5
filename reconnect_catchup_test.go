package natsmqtt5_test

import (
	"fmt"
	"net"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
)

// Options.ReconnectCatchUp: a connected session gets what was published while its broker's
// NATS connection was down.
//
// Live delivery is core NATS. A broker whose connection to NATS drops has no interest on the
// server until it reconnects, so a message another broker publishes in between reaches none
// of its connected sessions, and nothing sends it later: only a session that was away is
// replayed. The statements at stake are delivery itself, MQTT-5.0 §4.3.2 (QoS 1, at least
// once) and §4.3.3 (QoS 2, exactly once), and order, [MQTT-4.6.0-5] (the broker's own record
// in CONFORMANCE.md; the specification text was not re-read for this change).
//
// The tests cut the subscriber's broker off with a TCP proxy that closes its NATS
// connection and refuses a new one until restored, which is what a dropped connection is to
// nats.go: it reconnects and subscribes again.

// dropProxy forwards TCP to a NATS server; drop closes every connection and refuses new ones
// until restore.
type dropProxy struct {
	ln     net.Listener
	target string

	mu    sync.Mutex
	down  bool
	conns []net.Conn
}

func newDropProxy(t *testing.T, natsURL string) *dropProxy {
	t.Helper()
	u, err := url.Parse(natsURL)
	require.NoError(t, err)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	p := &dropProxy{ln: ln, target: u.Host}
	go p.accept()
	t.Cleanup(func() {
		_ = ln.Close()
		p.drop()
	})
	return p
}

func (p *dropProxy) url() string { return "nats://" + p.ln.Addr().String() }

func (p *dropProxy) accept() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		down := p.down
		p.mu.Unlock()
		if down {
			_ = c.Close()
			continue
		}
		up, err := net.Dial("tcp", p.target)
		if err != nil {
			_ = c.Close()
			continue
		}
		p.mu.Lock()
		p.conns = append(p.conns, c, up)
		p.mu.Unlock()
		go func() { _, _ = copyConn(up, c) }()
		go func() { _, _ = copyConn(c, up) }()
	}
}

// copyConn forwards from one side to the other until either ends, and closes the other then. A
// read that times out is a stall (dropProxy.stall): forwarding stops and both sides stay open.
func copyConn(to, from net.Conn) (int64, error) {
	buf := make([]byte, 32<<10)
	var n int64
	for {
		r, err := from.Read(buf)
		if r > 0 {
			w, werr := to.Write(buf[:r])
			n += int64(w)
			if werr != nil {
				_ = to.Close()
				return n, werr
			}
		}
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			return n, err
		}
		if err != nil {
			_ = to.Close()
			return n, err
		}
	}
}

func (p *dropProxy) drop() {
	p.mu.Lock()
	p.down = true
	conns := p.conns
	p.conns = nil
	p.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
}

// stall stops forwarding on the open connections without closing them, and refuses new ones
// until restore: a server that died behind a socket that stays open, which the client finds out
// only when its pings go unanswered.
func (p *dropProxy) stall() {
	p.mu.Lock()
	p.down = true
	for _, c := range p.conns {
		_ = c.SetReadDeadline(time.Now())
	}
	p.mu.Unlock()
}

func (p *dropProxy) restore() {
	p.mu.Lock()
	p.down = false
	p.mu.Unlock()
}

// quickReconnect makes the broker's NATS connection notice a dead server within a second or two
// (a container runtime's port forward keeps the socket open when the server inside dies, so only
// the pings tell) and reconnect within tens of milliseconds of it being back.
func quickReconnect(o *natsmqtt5.Options) {
	o.NATSOptions = append(o.NATSOptions, nats.ReconnectWait(50*time.Millisecond), nats.MaxReconnects(-1),
		nats.ReconnectJitter(0, 0), nats.PingInterval(500*time.Millisecond), nats.MaxPingsOutstanding(2))
}

func withReconnectCatchUp(o *natsmqtt5.Options) { o.ReconnectCatchUp = true }

// cutOffAndPublish cuts the subscriber's broker off NATS, publishes each payload on topic from
// pub while it is cut off, and restores it, returning once its NATS connection is back.
func cutOffAndPublish(t *testing.T, proxy *dropProxy, subBroker *natsmqtt5.Broker, pub *testClient,
	topic string, qos byte, payloads ...string) {
	t.Helper()
	proxy.drop()
	require.Eventually(t, func() bool { return !subBroker.NATS().IsConnected() }, 10*time.Second,
		5*time.Millisecond, "the subscriber's broker never noticed it was cut off")
	for _, p := range payloads {
		pub.publish(&paho.Publish{Topic: topic, QoS: qos, Payload: []byte(p)})
	}
	proxy.restore()
	require.Eventually(t, func() bool { return subBroker.NATS().IsConnected() }, 10*time.Second,
		5*time.Millisecond, "the subscriber's broker never reconnected")
}

// expectPayloads asserts the client gets exactly these payloads, in this order, and then nothing.
func (c *testClient) expectPayloads(want ...string) {
	c.t.Helper()
	got := make([]string, 0, len(want))
	for range want {
		select {
		case m := <-c.messages:
			got = append(got, m.Payload)
		case <-time.After(15 * time.Second):
			c.t.Fatalf("got %q, then nothing; want %q", got, want)
		}
	}
	assert.Equal(c.t, want, got)
	c.expectNoMessage()
}

// The message published while the subscriber's broker was off NATS reaches its connected
// QoS 1 subscriber after the reconnect, before what was published afterwards [MQTT-4.6.0-5],
// and nothing is sent twice.
func TestReconnectCatchUpSendsAConnectedSessionWhatWasPublishedWhileItsBrokerWasOffNATS(t *testing.T) {
	natsURL := startNATS(t)
	proxy := newDropProxy(t, natsURL)
	subBroker, subAddr, _ := startBrokerHandle(t, proxy.url(), quickReconnect, withReconnectCatchUp)
	pubAddr := startBroker(t, natsURL)

	sub, _ := connectClient(t, subAddr, connectOpts("rc-sub"))
	sub.subscribe(paho.SubscribeOptions{Topic: "rc/one", QoS: 1})
	pub, _ := connectClient(t, pubAddr, connectOpts("rc-pub"))

	pub.publish(&paho.Publish{Topic: "rc/one", QoS: 1, Payload: []byte("before")})
	assert.Equal(t, "before", sub.expectMessage().Payload)

	cutOffAndPublish(t, proxy, subBroker, pub, "rc/one", 1, "during-1", "during-2")
	pub.publish(&paho.Publish{Topic: "rc/one", QoS: 1, Payload: []byte("after")})

	sub.expectPayloads("during-1", "during-2", "after")
}

// At QoS 2 the catch-up reads the stream from a little before the connection dropped, which
// holds a message the session already has; it is not sent again [MQTT-4.3.3-2].
func TestReconnectCatchUpDoesNotSendAQoS2MessageTwice(t *testing.T) {
	natsURL := startNATS(t)
	proxy := newDropProxy(t, natsURL)
	subBroker, subAddr, _ := startBrokerHandle(t, proxy.url(), quickReconnect, withReconnectCatchUp)
	pubAddr := startBroker(t, natsURL)

	sub, _ := connectClient(t, subAddr, connectOpts("rc2-sub"))
	sub.subscribe(paho.SubscribeOptions{Topic: "rc/two", QoS: 2})
	pub, _ := connectClient(t, pubAddr, connectOpts("rc2-pub"))

	pub.publish(&paho.Publish{Topic: "rc/two", QoS: 2, Payload: []byte("before")})
	assert.Equal(t, "before", sub.expectMessage().Payload)

	cutOffAndPublish(t, proxy, subBroker, pub, "rc/two", 2, "during")

	sub.expectPayloads("during")
}

// The catch-up reads from before the drop, so it meets messages stored before one of the
// session's subscriptions existed. Those were never owed to it and are not sent.
func TestReconnectCatchUpDoesNotSendWhatWasStoredBeforeTheSubscription(t *testing.T) {
	natsURL := startNATS(t)
	proxy := newDropProxy(t, natsURL)
	subBroker, subAddr, _ := startBrokerHandle(t, proxy.url(), quickReconnect, withReconnectCatchUp)
	pubAddr := startBroker(t, natsURL)

	// The first subscription sets the session's replay cursor, below the message the second
	// one was made after; the catch-up reads from the cursor.
	sub, _ := connectClient(t, subAddr, connectOpts("rcb-sub"))
	sub.subscribe(paho.SubscribeOptions{Topic: "rc/first", QoS: 1})
	pub, _ := connectClient(t, pubAddr, connectOpts("rcb-pub"))
	pub.publish(&paho.Publish{Topic: "rc/late", QoS: 1, Payload: []byte("before-the-subscription")})
	sub.subscribe(paho.SubscribeOptions{Topic: "rc/late", QoS: 1})

	cutOffAndPublish(t, proxy, subBroker, pub, "rc/late", 1, "during")

	sub.expectPayloads("during")
}

// A server that dies behind an open socket is noticed only when the pings go unanswered, which
// can be minutes after the messages the broker missed (two by default) and so further back than
// the rewind. The catch-up also reaches back to the newest message the broker was handed before
// the drop, so what was published between the death and the notice is sent.
func TestReconnectCatchUpReachesBackPastTheRewindToTheLastMessageTheBrokerSaw(t *testing.T) {
	natsURL := startNATS(t)
	proxy := newDropProxy(t, natsURL)
	shortRewind := func(o *natsmqtt5.Options) { o.OfflineQueueRewind = 100 * time.Millisecond }
	subBroker, subAddr, _ := startBrokerHandle(t, proxy.url(), quickReconnect, withReconnectCatchUp, shortRewind)
	pubAddr := startBroker(t, natsURL, shortRewind)

	sub, _ := connectClient(t, subAddr, connectOpts("rcs-sub"))
	sub.subscribe(paho.SubscribeOptions{Topic: "rc/stall", QoS: 1})
	pub, _ := connectClient(t, pubAddr, connectOpts("rcs-pub"))
	pub.publish(&paho.Publish{Topic: "rc/stall", QoS: 1, Payload: []byte("before")})
	assert.Equal(t, "before", sub.expectMessage().Payload)

	proxy.stall()
	pub.publish(&paho.Publish{Topic: "rc/stall", QoS: 1, Payload: []byte("during")})
	stalledAt := time.Now()
	require.Eventually(t, func() bool { return !subBroker.NATS().IsConnected() }, 10*time.Second,
		5*time.Millisecond, "the subscriber's broker never noticed the stall")
	require.Greater(t, time.Since(stalledAt), 500*time.Millisecond, "the stall was noticed within the rewind")
	proxy.restore()
	require.Eventually(t, func() bool { return subBroker.NATS().IsConnected() }, 10*time.Second,
		5*time.Millisecond, "the subscriber's broker never reconnected")
	pub.publish(&paho.Publish{Topic: "rc/stall", QoS: 1, Payload: []byte("after")})

	sub.expectPayloads("during", "after")
}

// Many messages and several drops: every one arrives once and in order.
func TestReconnectCatchUpAcrossRepeatedDropsKeepsEveryMessageOnceAndInOrder(t *testing.T) {
	natsURL := startNATS(t)
	proxy := newDropProxy(t, natsURL)
	subBroker, subAddr, _ := startBrokerHandle(t, proxy.url(), quickReconnect, withReconnectCatchUp)
	pubAddr := startBroker(t, natsURL)

	sub, _ := connectClient(t, subAddr, connectOpts("rcn-sub"))
	sub.subscribe(paho.SubscribeOptions{Topic: "rc/many", QoS: 1})
	pub, _ := connectClient(t, pubAddr, connectOpts("rcn-pub"))

	var want []string
	n := 0
	next := func() string { n++; s := fmt.Sprintf("m%03d", n); want = append(want, s); return s }
	for round := 0; round < 3; round++ {
		for i := 0; i < 5; i++ {
			pub.publish(&paho.Publish{Topic: "rc/many", QoS: 1, Payload: []byte(next())})
		}
		cutOffAndPublish(t, proxy, subBroker, pub, "rc/many", 1, next(), next(), next())
	}
	pub.publish(&paho.Publish{Topic: "rc/many", QoS: 1, Payload: []byte(next())})

	sub.expectPayloads(want...)
}

// Off by default: the message published while the broker was off NATS is not sent to the
// connected session, as before the option existed.
func TestWithoutReconnectCatchUpAConnectedSessionMissesWhatWasPublishedWhileItsBrokerWasOffNATS(t *testing.T) {
	natsURL := startNATS(t)
	proxy := newDropProxy(t, natsURL)
	subBroker, subAddr, _ := startBrokerHandle(t, proxy.url(), quickReconnect)
	pubAddr := startBroker(t, natsURL)

	sub, _ := connectClient(t, subAddr, connectOpts("rcoff-sub"))
	sub.subscribe(paho.SubscribeOptions{Topic: "rc/off", QoS: 1})
	pub, _ := connectClient(t, pubAddr, connectOpts("rcoff-pub"))

	cutOffAndPublish(t, proxy, subBroker, pub, "rc/off", 1, "during")
	pub.publish(&paho.Publish{Topic: "rc/off", QoS: 1, Payload: []byte("after")})

	sub.expectPayloads("after")
}

func TestReconnectCatchUpWithTheQueueDisabledIsRefused(t *testing.T) {
	_, err := natsmqtt5.New(natsmqtt5.Options{ReconnectCatchUp: true, DisableOfflineQueue: true})
	require.ErrorIs(t, err, natsmqtt5.ErrInvalidOptions)
}
