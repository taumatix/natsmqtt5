package natsmqtt5_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
)

// The Receive Maximum the broker advertises in CONNACK limits the QoS 1 and
// QoS 2 PUBLISH packets a client may have unacknowledged (OASIS MQTT 5.0 text
// read 2026-10-09):
//
//   - §3.3.4: "If it receives more than Receive Maximum QoS 1 and QoS 2 PUBLISH
//     packets where it has not sent a PUBACK or PUBCOMP in response, the Server
//     uses a DISCONNECT packet with Reason Code 0x93 (Receive Maximum exceeded)
//     as described in section 4.13 Handling errors." The client side of it is
//     [MQTT-3.3.4-7].
//   - §4.13.1 lists 0x93 among the Protocol Error Reason Codes, and "When a
//     Server detects a Malformed Packet or Protocol Error, and a Reason Code is
//     given in the specification, it MUST close the Network Connection"
//     [MQTT-4.13.1-1].
//   - §4.9: the quota is given back by a PUBACK, a PUBCOMP, or a PUBREC with a
//     Reason Code of 0x80 or more; QoS 0 is never counted (§3.2.2.3.3).
//
// A QoS 1 PUBLISH is answered before the next packet is read, so only QoS 2
// can pile up: its PUBREC leaves the slot held until the PUBCOMP.

const (
	rmPubAck   = 0x40
	rmPubRec   = 0x50
	rmPubRel   = 0x62
	rmPubComp  = 0x70
	rmPingReq  = 0xC0
	rmPingResp = 0xD0
)

func rmBroker(t *testing.T) string {
	return p3Broker(t, func(o *natsmqtt5.Options) { o.ReceiveMaximum = 2 })
}

func (c *rawClient) rmExpect(first byte, id uint16) {
	c.t.Helper()
	got, body := c.readRawPacket()
	require.Equal(c.t, first, got)
	require.GreaterOrEqual(c.t, len(body), 2)
	require.Equal(c.t, []byte{byte(id >> 8), byte(id)}, body[:2])
}

func (c *rawClient) rmExpectAlive() {
	c.t.Helper()
	c.writeRaw(p3Frame(rmPingReq, nil))
	first, _ := c.readRawPacket()
	require.Equal(c.t, byte(rmPingResp), first, "the connection must still be open")
}

func TestConnackStatesTheReceiveMaximumTheBrokerEnforces(t *testing.T) {
	addr := rmBroker(t)
	c := dialRaw(t, addr)
	cp := rawConnect("rm-connack", 0)
	cp.CleanStart = true
	ack := c.connect(cp)
	require.NotNil(t, ack.Properties.ReceiveMaximum)
	assert.Equal(t, uint16(2), *ack.Properties.ReceiveMaximum)
}

// [MQTT-4.13.1-1] with the §3.3.4 sentence ([MQTT-3.3.4-7] for the Server half): a third QoS 2 PUBLISH while two are
// unacknowledged is answered with DISCONNECT 0x93 and the connection closes.
func TestMQTT_3_3_4_ReceiveMaximumExceededIsDisconnected0x93(t *testing.T) {
	c := p3Client(t, rmBroker(t), "rm-over")
	c.writeRaw(p3PublishBytes(0x04, 1, "a/b", "x"))
	c.rmExpect(rmPubRec, 1)
	c.writeRaw(p3PublishBytes(0x04, 2, "a/b", "x"))
	c.rmExpect(rmPubRec, 2)
	c.writeRaw(p3PublishBytes(0x04, 3, "a/b", "x"))
	c.expectDisconnect(0x93)
}

// A client that respects the limit is never disconnected: exactly Receive
// Maximum unacknowledged is allowed.
//
// Evidence for the Server half of [MQTT-3.3.4-7]: no false positive.
func TestMQTT_3_3_4_ExactlyReceiveMaximumUnacknowledgedIsAllowed(t *testing.T) {
	c := p3Client(t, rmBroker(t), "rm-exact")
	c.writeRaw(p3PublishBytes(0x04, 1, "a/b", "x"))
	c.writeRaw(p3PublishBytes(0x04, 2, "a/b", "x"))
	c.rmExpect(rmPubRec, 1)
	c.rmExpect(rmPubRec, 2)
	c.rmExpectAlive()
}

// §4.9: the PUBCOMP gives the slot back, so a third PUBLISH after the exchange
// of the first completes is within the limit; the PUBREC alone does not.
//
// Evidence for the Server half of [MQTT-3.3.4-7]: no false positive.
func TestMQTT_4_9_APubcompFreesTheSlot(t *testing.T) {
	c := p3Client(t, rmBroker(t), "rm-freed")
	c.writeRaw(p3PublishBytes(0x04, 1, "a/b", "x"))
	c.writeRaw(p3PublishBytes(0x04, 2, "a/b", "x"))
	c.rmExpect(rmPubRec, 1)
	c.rmExpect(rmPubRec, 2)
	c.writeRaw(p3AckBytes(rmPubRel, 1))
	c.rmExpect(rmPubComp, 1)
	c.writeRaw(p3PublishBytes(0x04, 3, "a/b", "x"))
	c.rmExpect(rmPubRec, 3)
	c.rmExpectAlive()
}

// A PUBLISH the broker refuses with a PUBREC of 0x80 or more is complete for
// quota purposes (§4.9), so any number of them is allowed.
//
// Evidence for the Server half of [MQTT-3.3.4-7]: no false positive.
func TestMQTT_4_9_APubrecWithAnErrorFreesTheSlot(t *testing.T) {
	c := p3Client(t, rmBroker(t), "rm-refused")
	for id := uint16(1); id <= 5; id++ {
		c.writeRaw(p3PublishBytes(0x04, id, "a/+", "x")) // a wildcard in a Topic Name is invalid
		first, body := c.readRawPacket()
		require.Equal(t, byte(rmPubRec), first)
		require.Equal(t, byte(0x90), body[2], "Topic Name invalid")
	}
	c.rmExpectAlive()
}

// QoS 1 is acknowledged before the next packet is read: any number in a row is
// within the limit.
//
// Evidence for the Server half of [MQTT-3.3.4-7]: no false positive.
func TestMQTT_3_3_4_QoS1InARowIsNeverOverTheLimit(t *testing.T) {
	c := p3Client(t, rmBroker(t), "rm-qos1")
	for id := uint16(1); id <= 10; id++ {
		c.writeRaw(p3PublishBytes(0x02, id, "a/b", "x"))
	}
	for id := uint16(1); id <= 10; id++ {
		c.rmExpect(rmPubAck, id)
	}
}

// §3.2.2.3.3: Receive Maximum "does not provide a mechanism to limit the QoS 0
// publications"; they are not counted, and do not give a QoS 2 slot back.
//
// Evidence for the Server half of [MQTT-3.3.4-7]: no false positive.
func TestMQTT_3_2_2_3_3_QoS0IsNeverCounted(t *testing.T) {
	c := p3Client(t, rmBroker(t), "rm-qos0")
	for i := 0; i < 20; i++ {
		c.writeRaw(p3PublishBytes(0x00, 0, "a/b", "x"))
	}
	c.writeRaw(p3PublishBytes(0x04, 1, "a/b", "x"))
	c.writeRaw(p3PublishBytes(0x04, 2, "a/b", "x"))
	c.rmExpect(rmPubRec, 1)
	c.rmExpect(rmPubRec, 2)
	c.rmExpectAlive()
}

// §4.3.3: a QoS 2 PUBLISH with an identifier still outstanding is a resend of
// the same message, not a new one, so it takes no further slot.
//
// Evidence for the Server half of [MQTT-3.3.4-7]: no false positive.
func TestMQTT_3_3_4_AResentQoS2PublishTakesNoSlot(t *testing.T) {
	c := p3Client(t, rmBroker(t), "rm-resend")
	c.writeRaw(p3PublishBytes(0x04, 1, "a/b", "x"))
	c.writeRaw(p3PublishBytes(0x04, 2, "a/b", "x"))
	c.writeRaw(p3PublishBytes(0x0C, 1, "a/b", "x")) // DUP
	c.rmExpect(rmPubRec, 1)
	c.rmExpect(rmPubRec, 2)
	c.rmExpect(rmPubRec, 1)
	c.rmExpectAlive()
}
