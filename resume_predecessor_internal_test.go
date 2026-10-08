package natsmqtt5

import (
	"bufio"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5/packet"
)

// A resumption resends what the session holds unacknowledged [MQTT-4.4.0-1],
// so what it holds has to include what the connection it replaces sent. That
// connection may still be running when the new CONNECT arrives, its delivery
// goroutine about to send, and track, one more message. This holds the
// predecessor's delivery goroutine in that state, with a message tracked only
// after the successor started, and requires the successor to resend it. The same race end to end,
// over a real broker, is TestAResumeAfterADropNeitherRepeatsNorSkipsWhatTheLastConnectionHandled.
func TestAResumeWaitsForItsPredecessorBeforeResending(t *testing.T) {
	b := &Broker{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	sess := newSession("pred")

	oldSrv, oldClient := net.Pipe()
	newSrv, newClient := net.Pipe()
	t.Cleanup(func() {
		for _, c := range []net.Conn{oldSrv, oldClient, newSrv, newClient} {
			_ = c.Close()
		}
	})
	old := newConn(b, oldSrv)
	old.sess = sess
	old.loopStarted.Store(true)
	sess.attach(old)
	next := newConn(b, newSrv)
	next.sess = sess
	next.quota = make(chan struct{}, 5)
	sess.attach(next)
	t.Cleanup(next.close)

	go next.deliverLoop()

	// The predecessor, still running, sends and tracks one more message after
	// the successor's delivery goroutine has started.
	time.Sleep(100 * time.Millisecond)
	sess.trackInflight(&outbound{packetID: 11, qos: packet.QoS1, quotaHeld: true,
		publish: &packet.Publish{Topic: "t", QoS: packet.QoS1, PacketID: 11, Payload: []byte("late")}})
	close(old.loopDone)

	require.NoError(t, newClient.SetReadDeadline(time.Now().Add(5*time.Second)))
	got, err := packet.Read(bufio.NewReader(newClient), 0)
	require.NoError(t, err, "the resumed connection resent nothing")
	pub, ok := got.(*packet.Publish)
	require.True(t, ok, "expected a PUBLISH, got %T", got)
	assert.Equal(t, "late", string(pub.Payload))
	assert.Equal(t, uint16(11), pub.PacketID)
	assert.True(t, pub.Dup, "[MQTT-3.3.1-1]: a resend sets DUP")
}
