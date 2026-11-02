package natsmqtt5_test

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// [MQTT-3.3.4-7] (Server half) and [MQTT-4.13.1-1]: the Server answers a Receive Maximum overrun with DISCONNECT
// 0x93 and closes the connection. A close that leaves unread data in the socket's receive buffer makes the kernel
// send a reset instead of a FIN, and the reset discards the DISCONNECT the client had not yet read: the client never
// learns the reason. A client that is still writing, as the Eclipse Paho suite's test_flow_control2 is, is exactly
// that case. The broker therefore half-closes, reads what the client still sends for a short time, and only then
// closes.
func TestMQTT_3_3_4_TheDisconnectSurvivesAClientThatKeepsWriting(t *testing.T) {
	c := p3Client(t, rmBroker(t), "rm-lingers")

	// Three QoS 2 PUBLISHes take the third over the limit of two; the burst behind them is far more than the broker
	// reads before it closes, so it is unread when the socket goes.
	var burst bytes.Buffer
	for id := uint16(1); id <= 3; id++ {
		burst.Write(p3PublishBytes(0x04, id, "a/b", "x"))
	}
	filler := strings.Repeat("y", 1000)
	for id := uint16(4); id < 200; id++ {
		burst.Write(p3PublishBytes(0x04, id, "a/b", filler))
	}
	c.writeRaw(burst.Bytes())

	// Keep writing while the broker answers, as the Paho suite's client does. A client whose write fails with a
	// reset has no way to read the DISCONNECT: the test reads it only after the writer has had time to hit one.
	stop := make(chan struct{})
	writeErr := make(chan error, 1)
	go func() {
		for id := uint16(300); ; id++ {
			select {
			case <-stop:
				writeErr <- nil
				return
			default:
			}
			_ = c.nc.SetWriteDeadline(time.Now().Add(time.Second))
			if _, err := c.nc.Write(p3PublishBytes(0x04, id, "a/b", filler)); err != nil {
				writeErr <- err
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	time.Sleep(300 * time.Millisecond)
	close(stop)
	require.NoError(t, <-writeErr, "the broker closed on a client that was still writing: it gets a reset, not the DISCONNECT")
	c.rmExpect(rmPubRec, 1)
	c.rmExpect(rmPubRec, 2)
	c.expectDisconnect(0x93)
}
