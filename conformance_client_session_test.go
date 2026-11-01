package natsmqtt5_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// The rest of the statements that bind a Client alone, the ones a Client breaks
// after the CONNACK (or in the middle of enhanced authentication). See
// conformance_client_violations_test.go for the argument; the Server answers a
// Malformed Packet or Protocol Error with a DISCONNECT and closes (§4.13.1).

// disconnectedWith requires the broker to end the connection with a DISCONNECT
// whose Reason Code is one of codes (§4.13.1: 0x81 or 0x82 when the spec names
// nothing more specific), and nothing else.
func (c *rawClient) disconnectedWith(codes ...packet.ReasonCode) {
	c.t.Helper()
	d := c.closedWith()
	require.NotNil(c.t, d, "the broker closed without telling the Client why [MQTT-4.13.1-1]")
	assert.Contains(c.t, codes, d.ReasonCode, "DISCONNECT Reason Code 0x%02X", byte(d.ReasonCode))
}

// "Each time a Client sends a new SUBSCRIBE, UNSUBSCRIBE, or PUBLISH (where QoS
// > 0) MQTT Control Packet it MUST assign it a non-zero Packet Identifier that is
// currently unused" [MQTT-2.2.1-3]. A Packet Identifier of 0 is the one case a
// Server can see by itself: it is a Protocol Error (§2.2.1, §4.13.1).
func TestPacketIdentifier_MQTT_2_2_1_3_APacketIdentifierOfZeroClosesTheConnection(t *testing.T) {
	addr := startConformanceBroker(t)
	for name, pkt := range map[string][]byte{
		"SUBSCRIBE":     p3SubscribeBytes(0, "a/b", 0),
		"UNSUBSCRIBE":   p3UnsubscribeBytes(0, "a/b"),
		"PUBLISH QoS 1": rawQoSPublish(1, 0, "a/b", "x"),
		"PUBLISH QoS 2": rawQoSPublish(2, 0, "a/b", "x"),
	} {
		t.Run(name, func(t *testing.T) {
			c := connectedRaw(t, addr, "cv-pid0")
			c.write(pkt)
			c.disconnectedWith(packet.MalformedPacket, packet.ProtocolError)
		})
	}
}

// "A Client MUST NOT send a PUBLISH packet with a Topic Alias greater than the
// Topic Alias Maximum value returned by the Server in the CONNACK packet"
// [MQTT-3.3.2-9], "greater than this value" [MQTT-3.2.2-17], and with a Topic
// Alias Maximum of 0 or absent, "MUST NOT send any Topic Aliases" at all
// [MQTT-3.2.2-18]. The Server answers with DISCONNECT 0x94 (Topic Alias invalid,
// §3.3.2.3.4).
func TestPublish_MQTT_3_3_2_9_3_2_2_17_18_ATopicAliasBeyondTheMaximumClosesTheConnection(t *testing.T) {
	three := uint16(3)
	addr := startConformanceBroker(t, func(o *natsmqtt5.Options) { o.TopicAliasMaximum = &three })
	for _, alias := range []uint16{4, 65535} {
		c := connectedRaw(t, addr, "cv-alias-high")
		c.send(&packet.Publish{Topic: "a/b", Properties: &packet.Properties{TopicAlias: packet.Uint16(alias)}})
		c.disconnectedWith(packet.TopicAliasInvalid)
	}

	zero := uint16(0)
	none := startConformanceBroker(t, func(o *natsmqtt5.Options) { o.TopicAliasMaximum = &zero })
	c := connectedRaw(t, none, "cv-alias-none")
	c.send(&packet.Publish{Topic: "a/b", Properties: &packet.Properties{TopicAlias: packet.Uint16(1)}})
	c.disconnectedWith(packet.TopicAliasInvalid)
}

// "A Client MUST accept all Topic Alias values greater than 0 and less than or
// equal to the Topic Alias Maximum value that it sent in the CONNECT packet"
// [MQTT-3.3.2-10]. The Server's side of the same line: it never sends one above
// the maximum the Client announced, however many topics it has to alias.
func TestPublish_MQTT_3_3_2_10_TheBrokerNeverAssignsAnAliasAboveTheClientsMaximum(t *testing.T) {
	addr := startBroker(t, startNATS(t), withOutboundAliases)
	sub := dialRaw(t, addr)
	sub.connect(aliasConnect("cv-alias-sub", 2))
	sub.subscribe("cv/#", packet.QoS1)
	pub, _ := connectClient(t, addr, connectOpts("cv-alias-pub"))

	sawAlias := false
	for round := 0; round < 2; round++ {
		for _, topic := range []string{"cv/one", "cv/two", "cv/three", "cv/four", "cv/five"} {
			publishQoS1(t, pub, topic)
			p := sub.expectPublish()
			if a := aliasOf(p); a != 0 {
				sawAlias = true
				assert.LessOrEqual(t, a, uint16(2), "Topic Alias on %s", topic)
			}
			sub.send(&packet.Puback{Ack: packet.Ack{PacketID: p.PacketID}})
		}
	}
	assert.True(t, sawAlias, "the premise: this broker did use aliases for this Client")
}

// "If a Client receives a Maximum QoS from a Server, it MUST NOT send PUBLISH
// packets at a QoS level exceeding the Maximum QoS level specified"
// [MQTT-3.2.2-11]. The Server answers with DISCONNECT 0x9B (QoS not supported,
// §3.2.2.3.4).
func TestConnack_MQTT_3_2_2_11_APublishAboveTheMaximumQoSClosesTheConnection(t *testing.T) {
	for _, tc := range []struct {
		max uint8
		qos byte
	}{{1, 2}, {0, 1}, {0, 2}} {
		addr := startConformanceBroker(t, func(o *natsmqtt5.Options) { o.MaximumQoS = natsmqtt5.Ptr(tc.max) })
		c := connectedRaw(t, addr, "cv-maxqos")
		c.write(rawQoSPublish(tc.qos, 1, "a/b", "x"))
		c.disconnectedWith(packet.QoSNotSupported)
	}
}

// "A Client receiving Retain Available set to 0 from the Server MUST NOT send a
// PUBLISH packet with the RETAIN flag set to 1" [MQTT-3.2.2-14]. The Server
// answers with DISCONNECT 0x9A (Retain not supported).
func TestConnack_MQTT_3_2_2_14_ARetainedPublishToABrokerWithoutRetainClosesTheConnection(t *testing.T) {
	addr := startConformanceBroker(t, func(o *natsmqtt5.Options) { o.DisableRetained = true })
	c := connectedRaw(t, addr, "cv-noretain")
	c.write(p3PublishBytes(0x01, 0, "a/b", "x"))
	c.disconnectedWith(packet.RetainNotSupported)
}

// "The Client MUST NOT send packets exceeding Maximum Packet Size to the
// Server" [MQTT-3.2.2-15]. The Server answers with DISCONNECT 0x95 (Packet too
// large).
func TestConnack_MQTT_3_2_2_15_APacketBeyondTheMaximumPacketSizeClosesTheConnection(t *testing.T) {
	addr := startConformanceBroker(t, func(o *natsmqtt5.Options) { o.MaximumPacketSize = 100 })
	c := connectedRaw(t, addr, "cv-big")
	c.write(p3PublishBytes(0x00, 0, "a/b", string(make([]byte, 200))))
	c.disconnectedWith(packet.PacketTooLarge)
}

// "The Topic Filters MUST be a UTF-8 Encoded String" [MQTT-3.8.3-1] and "The
// Topic Filters in an UNSUBSCRIBE packet MUST be UTF-8 Encoded Strings"
// [MQTT-3.10.3-1]: one that is not is a Malformed Packet.
func TestSubscribe_MQTT_3_8_3_1_3_10_3_1_AFilterThatIsNotUTF8IsMalformed(t *testing.T) {
	addr := startConformanceBroker(t)
	for name, filter := range map[string]string{
		"overlong": "a/\xC0\x80", "surrogate": "a/\xED\xA0\x80", "NUL": "a/\x00", "truncated": "a/\xE2\x82",
	} {
		t.Run("SUBSCRIBE "+name, func(t *testing.T) {
			c := connectedRaw(t, addr, "cv-badsub")
			c.write(p3SubscribeBytes(1, filter, 0))
			c.disconnectedWith(packet.MalformedPacket)
		})
		t.Run("UNSUBSCRIBE "+name, func(t *testing.T) {
			c := connectedRaw(t, addr, "cv-badunsub")
			c.write(p3UnsubscribeBytes(1, filter))
			c.disconnectedWith(packet.MalformedPacket)
		})
	}
}

// "If a Client sets an Authentication Method in the CONNECT, the Client MUST NOT
// send any packets other than AUTH or DISCONNECT packets until it has received a
// CONNACK packet" [MQTT-3.1.2-30]. A PUBLISH, SUBSCRIBE or PINGREQ in the middle
// of the exchange is refused and the connection closed, with no CONNACK Success.
func TestEnhancedAuth_MQTT_3_1_2_30_APacketOtherThanAuthBeforeTheConnackIsRefused(t *testing.T) {
	addr := p3Broker(t, withAuther)
	for name, pkt := range map[string][]byte{
		"PUBLISH":   rawQoS0Publish("alice/x", "x"),
		"SUBSCRIBE": p3SubscribeBytes(1, "alice/#", 0),
		"PINGREQ":   {0xC0, 0x00},
	} {
		t.Run(name, func(t *testing.T) {
			c := dialRaw(t, addr)
			c.send(authConnect("cv-early", "alice"))
			c.expectAuth(packet.ContinueAuthentication)
			c.write(pkt)
			c.expectRejectedAndClosed()
		})
	}
}

// "The Client responds to an AUTH packet from the Server by sending a further
// AUTH packet. This packet MUST contain a Reason Code of 0x18 (Continue
// authentication)" [MQTT-4.12.0-3]. A reply with another Reason Code ends the
// exchange without a Success.
func TestEnhancedAuth_MQTT_4_12_0_3_AReplyOtherThanContinueEndsTheExchange(t *testing.T) {
	addr := p3Broker(t, withAuther)
	for _, code := range []packet.ReasonCode{packet.Success, packet.ReAuthenticate} {
		c := dialRaw(t, addr)
		c.send(authConnect("cv-badcode", "alice"))
		nonce := c.expectAuth(packet.ContinueAuthentication).Properties.AuthenticationData
		c.send(authPkt(code, hmacMethod, proof("s3cret", nonce)))
		c.expectRejectedAndClosed()
	}
}

// "If the Client does not include an Authentication Method in the CONNECT, the
// Client MUST NOT send an AUTH packet to the Server" [MQTT-4.12.0-7]: an AUTH
// from a connection that did not ask for enhanced authentication is a Protocol
// Error.
func TestEnhancedAuth_MQTT_4_12_0_7_AnAuthPacketWithoutAnAuthenticationMethodIsAProtocolError(t *testing.T) {
	addr := p3Broker(t, withAuther)
	c := connectedRaw(t, addr, "cv-unasked")
	c.send(authPkt(packet.ReAuthenticate, hmacMethod, nil))
	c.disconnectedWith(packet.ProtocolError, packet.MalformedPacket)

	c = connectedRaw(t, addr, "cv-unasked-continue")
	c.send(authPkt(packet.ContinueAuthentication, hmacMethod, nil))
	c.disconnectedWith(packet.ProtocolError, packet.MalformedPacket)
}

// "The Client MUST set the Authentication Method to the same value as the
// Authentication Method originally used to authenticate the Network Connection"
// [MQTT-4.12.1-1]. A re-authentication that names another method is refused and
// the connection closed (a Protocol Error, or Bad authentication method).
func TestEnhancedAuth_MQTT_4_12_1_1_AReauthenticationWithAnotherMethodIsRefused(t *testing.T) {
	addr := p3Broker(t, withAuther)
	c := connectedWithAuth(t, addr, "cv-reauth", "alice", "s3cret")
	c.send(authPkt(packet.ReAuthenticate, "OTHER-METHOD", []byte("alice")))
	c.disconnectedWith(packet.ProtocolError, packet.BadAuthenticationMethod)
}
