package natsmqtt5_test

import (
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"

	"github.com/taumatix/natsmqtt5"
)

// A SUBACK is a promise: what is published next arrives. nats.go buffers the
// SUB protocol and writes it from its flusher goroutine, so a broker that
// answers the SUBACK without waiting for that write can promise interest the
// NATS server has not registered yet — and a publisher on a second broker then
// wins the race and the message is dropped with nothing to show for it.
//
// On a real network that window is microseconds wide, which made it a CI flake
// (TestTwoBrokersShareTraffic, macOS, run 6) rather than a reproducible bug.
// Holding broker A's writes to NATS back for holdWrites makes losing the race
// certain instead of rare.
func TestSubackMeansTheInterestIsLiveOnNATS(t *testing.T) {
	const holdWrites = 500 * time.Millisecond

	natsURL := startNATS(t)

	slow := &slowDialer{}
	addrA := startBroker(t, natsURL, func(o *natsmqtt5.Options) {
		o.NATSOptions = append(o.NATSOptions, nats.SetCustomDialer(slow))
	})
	addrB := startBroker(t, natsURL)

	sub, _ := connectClient(t, addrA, connectOpts("sub-on-a"))

	// Armed only now, so the broker's own start-up traffic — the NATS
	// handshake and the JetStream set-up for retained messages — runs at full
	// speed and only the SUBSCRIBE is delayed.
	slow.hold(holdWrites)
	sub.subscribe(paho.SubscribeOptions{Topic: "race/#", QoS: 1})
	slow.hold(0)

	pub, _ := connectClient(t, addrB, connectOpts("pub-on-b"))
	pub.publish(&paho.Publish{Topic: "race/hello", QoS: 1, Payload: []byte("across")})

	got := sub.expectMessage()
	assert.Equal(t, "race/hello", got.Topic)
	assert.Equal(t, "across", got.Payload)
}

// slowDialer is a nats.CustomDialer whose connections delay every write to the
// NATS server by a duration the test controls.
type slowDialer struct {
	delay atomic.Int64 // nanoseconds
}

// hold sets the delay applied to writes from now on. Zero restores full speed.
func (d *slowDialer) hold(delay time.Duration) {
	d.delay.Store(int64(delay))
}

func (d *slowDialer) Dial(network, address string) (net.Conn, error) {
	c, err := net.DialTimeout(network, address, 5*time.Second)
	if err != nil {
		return nil, err
	}
	return &slowConn{Conn: c, dialer: d}, nil
}

type slowConn struct {
	net.Conn
	dialer *slowDialer
}

func (c *slowConn) Write(b []byte) (int, error) {
	if d := c.dialer.delay.Load(); d > 0 {
		time.Sleep(time.Duration(d))
	}
	return c.Conn.Write(b)
}
