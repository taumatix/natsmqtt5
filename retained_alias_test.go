package natsmqtt5_test

import (
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5/packet"
)

// readRawPacket returns the first byte of the next packet and its body, read
// off the socket without decoding, so a test can judge the bytes themselves.
func (c *rawClient) readRawPacket() (first byte, body []byte) {
	c.t.Helper()
	require.NoError(c.t, c.nc.SetReadDeadline(time.Now().Add(5*time.Second)))
	first, err := c.r.ReadByte()
	require.NoError(c.t, err)
	// Remaining Length is a variable byte integer (MQTT-5.0 §1.5.5).
	length, shift := 0, 0
	for {
		b, err := c.r.ReadByte()
		require.NoError(c.t, err)
		length |= int(b&0x7F) << shift
		if b&0x80 == 0 {
			break
		}
		shift += 7
	}
	body = make([]byte, length)
	_, err = io.ReadFull(c.r, body)
	require.NoError(c.t, err)
	return first, body
}

// expectClosed asserts the broker closed the connection: the next read ends
// with EOF (or a reset) rather than a timeout or another packet.
func (c *rawClient) expectClosed() {
	c.t.Helper()
	require.NoError(c.t, c.nc.SetReadDeadline(time.Now().Add(5*time.Second)))
	_, err := c.r.ReadByte()
	require.Error(c.t, err, "the broker must close the connection")
	if ne, ok := err.(interface{ Timeout() bool }); ok {
		require.False(c.t, ne.Timeout(), "the broker left the connection open")
	}
}

// A retained message served to a new subscriber must not carry the Topic Alias
// its publisher sent. Statements, worded as in the OASIS MQTT 5.0 text read on
// 2026-10-08:
//   - [MQTT-3.1.2-26] "The Server MUST NOT send a Topic Alias in a PUBLISH
//     packet to the Client greater than Topic Alias Maximum."
//   - [MQTT-3.1.2-27] "If Topic Alias Maximum is absent or zero, the Server
//     MUST NOT send any Topic Aliases to the Client."
//   - [MQTT-3.3.2-11] "A Server MUST NOT send a PUBLISH packet with a Topic
//     Alias greater than the Topic Alias Maximum value sent by the Client in the
//     CONNECT packet."
//
// The subscriber never sends a Topic Alias Maximum, so any alias at all breaks
// all three, and its PUBLISH is asserted byte for byte: Property Length 0.
//
// The defect was a race between the broker's own copy of the message (written
// when the PUBLISH is handled, with the publisher's properties) and the copy
// the stream consumer reads back (which has no alias). Subscribing straight
// after each PUBACK, forty times, loses that race on the old code on every run.
func TestARetainedMessageServedToASubscriberCarriesNoTopicAlias(t *testing.T) {
	addr := startBroker(t, startNATS(t))

	pub := dialRaw(t, addr)
	pub.connect(rawConnect("alias-pub", 0))
	sub := dialRaw(t, addr)
	sub.connect(rawConnect("alias-sub", 0))

	for i := 0; i < 40; i++ {
		name := fmt.Sprintf("alias/retained/%d", i)
		pub.send(&packet.Publish{
			QoS: packet.QoS1, Retain: true, Topic: name, PacketID: 1,
			Payload:    []byte("kept"),
			Properties: &packet.Properties{TopicAlias: packet.Uint16(1)},
		})
		_, ok := pub.read().(*packet.Puback)
		require.True(t, ok, "a QoS 1 PUBLISH is answered with a PUBACK")

		sub.subscribe(name, packet.QoS0)
		first, body := sub.readRawPacket()

		// PUBLISH, DUP 0, QoS 0, RETAIN 1 (§3.3.1): the retained message is sent
		// with the RETAIN flag set at subscribe time [MQTT-3.3.1-8].
		assert.Equal(t, byte(0x31), first, "%s: a retained QoS 0 PUBLISH", name)
		want := append([]byte{0x00, byte(len(name))}, name...)
		want = append(want, 0x00) // Property Length 0: no Topic Alias [MQTT-3.1.2-27]
		want = append(want, "kept"...)
		assert.Equal(t, want, body, "%s: the retained PUBLISH carries no properties", name)

		sub.send(&packet.Unsubscribe{PacketID: 100, Filters: []string{name}})
		_, ok = sub.read().(*packet.Unsuback)
		require.True(t, ok)
	}
}
