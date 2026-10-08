package natsmqtt5_test

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// Conformance audit, part 2: MQTT 5.0 statements that bind a Server, from
// [MQTT-3.2.2-22] to [MQTT-3.11.2-1] (rows 86-170 of
// conformance/mqtt5-statements.tsv). Every test runs a real embedded
// nats-server, a real Broker and a real TCP socket, and asserts what the
// specification says a peer can observe. Statement wording is quoted from the
// OASIS standard (mqtt-v5.0-os.html, Appendix C, read 2026-10-08).
//
// The raw tests use this module's codec to send well-formed packets (the same
// compromise rawClient documents) and hand-written bytes wherever the packet
// is malformed or the byte layout is the point; the receiving side is judged on
// what the broker puts on the wire.

// --- helpers ---------------------------------------------------------------

// p2Counter counts the bytes read from a connection, so a test can measure the
// wire size of each packet the broker sent without trusting the encoder.
type p2Counter struct {
	r io.Reader
	n int
}

func (c *p2Counter) Read(b []byte) (int, error) {
	n, err := c.r.Read(b)
	c.n += n
	return n, err
}

// p2Client is a rawClient that also measures packet sizes.
type p2Client struct {
	*rawClient
	cnt *p2Counter
}

func p2Dial(t *testing.T, addr string) *p2Client {
	t.Helper()
	nc, err := net.DialTimeout("tcp", addr, 5*time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = nc.Close() })
	cnt := &p2Counter{r: nc}
	return &p2Client{
		rawClient: &rawClient{t: t, nc: nc, r: bufio.NewReader(cnt)},
		cnt:       cnt,
	}
}

// readSized returns the next packet and its size on the wire.
func (c *p2Client) readSized() (packet.Packet, int) {
	c.t.Helper()
	before := c.cnt.n - c.r.Buffered()
	p := c.read()
	return p, c.cnt.n - c.r.Buffered() - before
}

// p2Connect opens a clean-start session with a 0 Session Expiry Interval.
func p2Connect(t *testing.T, addr, id string, mods ...func(*packet.Connect)) (*p2Client, *packet.Connack) {
	t.Helper()
	c := p2Dial(t, addr)
	cp := rawConnect(id, 0)
	cp.CleanStart = true
	for _, m := range mods {
		m(cp)
	}
	c.send(cp)
	ack, ok := c.read().(*packet.Connack)
	require.True(t, ok, "the first packet from the broker must be a CONNACK")
	require.False(t, ack.ReasonCode.IsError(), "CONNACK refused with 0x%02X", byte(ack.ReasonCode))
	return c, ack
}

func (c *rawClient) writeBytes(b []byte) {
	c.t.Helper()
	require.NoError(c.t, c.nc.SetWriteDeadline(time.Now().Add(5*time.Second)))
	_, err := c.nc.Write(b)
	require.NoError(c.t, err)
}

// closedWith asserts the broker ends the connection without sending anything
// but a DISCONNECT, and returns that DISCONNECT if there was one.
func (c *rawClient) closedWith() *packet.Disconnect {
	c.t.Helper()
	var d *packet.Disconnect
	deadline := time.Now().Add(5 * time.Second)
	for {
		require.NoError(c.t, c.nc.SetReadDeadline(deadline))
		p, err := packet.Read(c.r, 0)
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				c.t.Fatal("the broker did not close the connection")
			}
			return d
		}
		dd, ok := p.(*packet.Disconnect)
		if !ok {
			c.t.Fatalf("expected the connection to close, got a %s", p.Type())
		}
		d = dd
	}
}

func p2Next[T packet.Packet](t *testing.T, c *rawClient) T {
	t.Helper()
	p := c.read()
	got, ok := p.(T)
	var want T
	require.True(t, ok, "expected %T, got %s", want, p.Type())
	return got
}

// p2Sub subscribes and returns the granted code; the caller names the options.
func p2Sub(t *testing.T, c *rawClient, sub packet.Subscription) packet.ReasonCode {
	t.Helper()
	c.subID++
	c.send(&packet.Subscribe{PacketID: c.subID, Subscriptions: []packet.Subscription{sub}})
	ack := p2Next[*packet.Suback](t, c)
	require.Equal(t, c.subID, ack.PacketID)
	require.Len(t, ack.ReasonCodes, 1)
	return ack.ReasonCodes[0]
}

// p2Wire reads the first byte of the next packet without consuming it.
func (c *rawClient) peekFirstByte() byte {
	c.t.Helper()
	require.NoError(c.t, c.nc.SetReadDeadline(time.Now().Add(5*time.Second)))
	b, err := c.r.Peek(1)
	require.NoError(c.t, err)
	return b[0]
}

// p2PublishBytes hand-builds a PUBLISH. flags is the low nibble of the first
// byte (DUP, QoS, RETAIN); props is the already-encoded property bytes.
func p2PublishBytes(flags byte, topic string, id uint16, props []byte, payload string) []byte {
	var body []byte
	body = append(body, byte(len(topic)>>8), byte(len(topic)))
	body = append(body, topic...)
	if (flags>>1)&3 != 0 {
		body = append(body, byte(id>>8), byte(id))
	}
	body = append(body, byte(len(props)))
	body = append(body, props...)
	body = append(body, payload...)
	return append([]byte{0x30 | flags, byte(len(body))}, body...)
}

// p2Deny returns an option that refuses publishes to, and subscriptions on,
// anything under "deny/".
func p2Deny(o *natsmqtt5.Options) {
	o.Authorizer = natsmqtt5.AuthorizerFunc(func(_ context.Context, req *natsmqtt5.AuthzRequest) error {
		if strings.HasPrefix(req.Topic, "deny/") {
			return errors.New("denied")
		}
		return nil
	})
}

func p2Start(t *testing.T, opts ...func(*natsmqtt5.Options)) string {
	t.Helper()
	return startBroker(t, startNATS(t), opts...)
}

func containsCode(set []packet.ReasonCode, c packet.ReasonCode) bool {
	for _, s := range set {
		if s == c {
			return true
		}
	}
	return false
}

// --- 3.2.2 CONNACK -----------------------------------------------------------

// [MQTT-3.2.2-22] "If the Server does not send the Server Keep Alive, the
// Server MUST use the Keep Alive value set by the Client on CONNECT."
func TestConnack_MQTT_3_2_2_22(t *testing.T) {
	addr := p2Start(t)
	c := p2Dial(t, addr)
	cp := rawConnect("ka", 0)
	cp.CleanStart = true
	cp.KeepAlive = 1
	c.send(cp)
	ack := p2Next[*packet.Connack](t, c.rawClient)
	require.False(t, ack.Properties != nil && ack.Properties.ServerKeepAlive != nil,
		"this broker was configured without a Server Keep Alive, so the premise of the statement holds")

	// The client's value governs: pinging inside 1.5 x 1 s keeps it alive past
	// the point where any longer interval would never have mattered...
	for i := 0; i < 4; i++ {
		time.Sleep(500 * time.Millisecond)
		c.send(&packet.Pingreq{})
		p2Next[*packet.Pingresp](t, c.rawClient)
	}
	// ...and silence ends it within 1.5 x the client's 1 s [MQTT-3.1.2-22], far
	// inside what a default of 60 s would allow.
	start := time.Now()
	c.closedWith()
	assert.Less(t, time.Since(start), 4*time.Second)
}

// --- 3.3.1 PUBLISH fixed header and retain ------------------------------------

// [MQTT-3.3.1-2] "The DUP flag MUST be set to 0 for all QoS 0 messages." The
// bit is read from the first byte on the wire.
func TestPublish_MQTT_3_3_1_2(t *testing.T) {
	addr := p2Start(t)
	sub, _ := p2Connect(t, addr, "sub")
	p2Sub(t, sub.rawClient, packet.Subscription{Filter: "dup/q0", QoS: 0})
	pub, _ := p2Connect(t, addr, "pub")
	pub.send(&packet.Publish{Topic: "dup/q0", Payload: []byte("x")})

	first := sub.peekFirstByte()
	got := p2Next[*packet.Publish](t, sub.rawClient)
	assert.Equal(t, byte(0x30), first, "PUBLISH, DUP 0, QoS 0, RETAIN 0")
	assert.False(t, got.Dup)
}

// [MQTT-3.3.1-3] "The DUP flag in the outgoing PUBLISH packet is set
// independently to the incoming PUBLISH packet, its value MUST be determined
// solely by whether the outgoing PUBLISH packet is a retransmission." A
// publisher's own retransmission (DUP 1) is a first delivery to the subscriber.
func TestPublish_MQTT_3_3_1_3(t *testing.T) {
	addr := p2Start(t)
	sub, _ := p2Connect(t, addr, "sub")
	p2Sub(t, sub.rawClient, packet.Subscription{Filter: "dup/in", QoS: 1})
	pub, _ := p2Connect(t, addr, "pub")
	pub.send(&packet.Publish{Dup: true, QoS: 1, PacketID: 7, Topic: "dup/in", Payload: []byte("again")})
	p2Next[*packet.Puback](t, pub.rawClient)

	first := sub.peekFirstByte()
	got := p2Next[*packet.Publish](t, sub.rawClient)
	assert.Equal(t, "again", string(got.Payload))
	assert.Equal(t, byte(0x32), first, "PUBLISH, DUP 0, QoS 1, RETAIN 0 although the incoming one had DUP 1")
}

// [MQTT-3.3.1-4] "A PUBLISH Packet MUST NOT have both QoS bits set to 1." The
// broker is the receiver here (MQTT-5.0 §3.3.1.2: a Malformed Packet): it must
// close the connection and forward nothing. It also never sends QoS 3.
func TestPublish_MQTT_3_3_1_4(t *testing.T) {
	addr := p2Start(t)
	watch, _ := p2Connect(t, addr, "watch")
	p2Sub(t, watch.rawClient, packet.Subscription{Filter: "qos3/#", QoS: 2})

	bad, _ := p2Connect(t, addr, "bad")
	// 0x36: PUBLISH with QoS bits 11. Topic "qos3/x", Packet Id 1, no properties.
	body := append([]byte{0x00, 0x06}, "qos3/x"...)
	body = append(body, 0x00, 0x01, 0x00)
	bad.writeBytes(append([]byte{0x36, byte(len(body))}, body...))
	d := bad.closedWith()
	if d != nil {
		assert.Equal(t, packet.MalformedPacket, d.ReasonCode)
	}
	watch.expectNothing()

	// What the broker sends never has both QoS bits set.
	pub, _ := p2Connect(t, addr, "pub")
	pub.send(&packet.Publish{QoS: 2, PacketID: 9, Topic: "qos3/y", Payload: []byte("ok")})
	p2Next[*packet.Pubrec](t, pub.rawClient)
	assert.NotEqual(t, byte(3), (watch.peekFirstByte()>>1)&3)
}

// [MQTT-3.3.1-5] "If the RETAIN flag is set to 1 in a PUBLISH packet sent by a
// Client to a Server, the Server MUST replace any existing retained message for
// this topic and store the Application Message."
func TestPublish_MQTT_3_3_1_5(t *testing.T) {
	addr := p2Start(t)
	pub, _ := connectClient(t, addr, connectOpts("pub"))
	pub.publish(&paho.Publish{Topic: "ret/replace", QoS: 1, Retain: true, Payload: []byte("one")})
	pub.publish(&paho.Publish{Topic: "ret/replace", QoS: 1, Retain: true, Payload: []byte("two")})

	late, _ := connectClient(t, addr, connectOpts("late"))
	late.subscribe(paho.SubscribeOptions{Topic: "ret/replace", QoS: 1})
	got := late.expectMessage()
	assert.Equal(t, "two", got.Payload)
	assert.True(t, got.Retain)
	late.expectNoMessage()
}

// [MQTT-3.3.1-7] "A retained message with a Payload containing zero bytes MUST
// NOT be stored as a retained message on the Server." and [MQTT-3.3.1-6] "If the
// Payload contains zero bytes it is processed normally by the Server but any
// retained message with the same topic name MUST be removed and any future
// subscribers for the topic will not receive a retained message."
func TestPublish_MQTT_3_3_1_6_and_7(t *testing.T) {
	addr := p2Start(t)
	live, _ := connectClient(t, addr, connectOpts("live"))
	live.subscribe(paho.SubscribeOptions{Topic: "ret/empty", QoS: 1})
	pub, _ := connectClient(t, addr, connectOpts("pub"))
	pub.publish(&paho.Publish{Topic: "ret/empty", QoS: 1, Retain: true, Payload: []byte("full")})
	assert.Equal(t, "full", live.expectMessage().Payload)
	pub.publish(&paho.Publish{Topic: "ret/empty", QoS: 1, Retain: true})
	assert.Equal(t, "", live.expectMessage().Payload, "processed normally")

	late, _ := connectClient(t, addr, connectOpts("late"))
	late.subscribe(paho.SubscribeOptions{Topic: "ret/empty", QoS: 1})
	late.expectNoMessage()

	// Publishing the empty retained message to a topic with nothing retained
	// stores nothing either.
	pub.publish(&paho.Publish{Topic: "ret/never", QoS: 1, Retain: true})
	late.subscribe(paho.SubscribeOptions{Topic: "ret/never", QoS: 1})
	late.expectNoMessage()
}

// [MQTT-3.3.1-8] "If the RETAIN flag is 0 in a PUBLISH packet sent by a Client
// to a Server, the Server MUST NOT store the message as a retained message and
// MUST NOT remove or replace any existing retained message." (The reserved
// "$retained/" variant is in reserved_test.go.)
func TestPublish_MQTT_3_3_1_8(t *testing.T) {
	addr := p2Start(t)
	pub, _ := connectClient(t, addr, connectOpts("pub"))
	pub.publish(&paho.Publish{Topic: "ret/keep", QoS: 1, Retain: true, Payload: []byte("kept")})
	pub.publish(&paho.Publish{Topic: "ret/keep", QoS: 1, Payload: []byte("transient")})
	pub.publish(&paho.Publish{Topic: "ret/keep", QoS: 1}) // empty, not retained: must not clear it
	pub.publish(&paho.Publish{Topic: "ret/fresh", QoS: 1, Payload: []byte("transient")})

	late, _ := connectClient(t, addr, connectOpts("late"))
	late.subscribe(paho.SubscribeOptions{Topic: "ret/#", QoS: 1})
	got := late.expectMessage()
	assert.Equal(t, "ret/keep", got.Topic)
	assert.Equal(t, "kept", got.Payload)
	late.expectNoMessage() // nothing was stored for ret/fresh
}

// [MQTT-3.3.1-11] "If Retain Handling is set to 2, the Server MUST NOT send the
// retained messages."
func TestPublish_MQTT_3_3_1_11(t *testing.T) {
	addr := p2Start(t)
	pub, _ := connectClient(t, addr, connectOpts("pub"))
	pub.publish(&paho.Publish{Topic: "ret/rh2", QoS: 1, Retain: true, Payload: []byte("old")})

	sub, _ := connectClient(t, addr, connectOpts("sub"))
	sub.subscribe(paho.SubscribeOptions{Topic: "ret/rh2", QoS: 1, RetainHandling: 2})
	sub.expectNoMessage()

	// The subscription is live: a new message reaches it.
	pub.publish(&paho.Publish{Topic: "ret/rh2", QoS: 1, Retain: true, Payload: []byte("new")})
	assert.Equal(t, "new", sub.expectMessage().Payload)
}

// --- 3.3.2 Topic Name and properties ----------------------------------------

// [MQTT-3.3.2-1] "The Topic Name MUST be present as the first field in the
// PUBLISH packet Variable Header. It MUST be a UTF-8 Encoded String." A Topic
// Name that is not well-formed UTF-8 (or holds U+0000) is a Malformed Packet.
func TestPublish_MQTT_3_3_2_1(t *testing.T) {
	cases := map[string]string{
		"invalid byte":    "a/\xff",
		"lone surrogate":  "a/\xed\xa0\x80",
		"null character":  "a/\x00b",
		"overlong encode": "a/\xc0\xaf",
	}
	for name, topic := range cases {
		t.Run(name, func(t *testing.T) {
			addr := p2Start(t)
			watch, _ := p2Connect(t, addr, "watch")
			p2Sub(t, watch.rawClient, packet.Subscription{Filter: "#", QoS: 0})
			bad, _ := p2Connect(t, addr, "bad")
			bad.writeBytes(p2PublishBytes(0, topic, 0, nil, "x"))
			bad.closedWith()
			watch.expectNothing()
		})
	}
}

// [MQTT-3.3.2-2] "The Topic Name in the PUBLISH packet MUST NOT contain
// wildcard characters." The broker refuses the publish (0x90) and forwards
// nothing to the subscribers a wildcard would have matched.
func TestPublish_MQTT_3_3_2_2(t *testing.T) {
	addr := p2Start(t)
	watch, _ := p2Connect(t, addr, "watch")
	p2Sub(t, watch.rawClient, packet.Subscription{Filter: "#", QoS: 1})
	pub, _ := p2Connect(t, addr, "pub")
	for i, name := range []string{"wild/+", "wild/#", "#", "+", "wild/a+"} {
		id := uint16(i + 1)
		pub.send(&packet.Publish{QoS: 1, PacketID: id, Topic: name, Payload: []byte("x")})
		ack := p2Next[*packet.Puback](t, pub.rawClient)
		assert.Equal(t, id, ack.PacketID)
		assert.Equal(t, packet.TopicNameInvalid, ack.ReasonCode, name)
	}
	watch.expectNothing()
}

// [MQTT-3.3.2-3] "The Topic Name in a PUBLISH packet sent by a Server to a
// subscribing Client MUST match the Subscriptions Topic Filter." That includes
// '#' matching its parent level, and not matching a $ topic.
func TestPublish_MQTT_3_3_2_3(t *testing.T) {
	addr := p2Start(t)
	sub, _ := p2Connect(t, addr, "sub")
	p2Sub(t, sub.rawClient, packet.Subscription{Filter: "sport/#", QoS: 0})
	p2Sub(t, sub.rawClient, packet.Subscription{Filter: "+/b", QoS: 0})
	p2Sub(t, sub.rawClient, packet.Subscription{Filter: "#", QoS: 0})
	pub, _ := p2Connect(t, addr, "pub")

	publish := func(name string) {
		pub.send(&packet.Publish{Topic: name, Payload: []byte(name)})
	}
	publish("sport")        // matches sport/# (parent level) and #
	publish("a/b/b")        // +/b must not match; # does
	publish("$dollar/x")    // # must not match [MQTT-4.7.2-1]
	publish("sport/tennis") // matches sport/# and #

	filters := map[string][]string{
		"sport":        {"sport/#", "#"},
		"a/b/b":        {"#"},
		"sport/tennis": {"sport/#", "#"},
	}
	var seen []string
	for i := 0; i < 5; i++ {
		got := p2Next[*packet.Publish](t, sub.rawClient)
		seen = append(seen, got.Topic)
	}
	counts := map[string]int{}
	for _, topic := range seen {
		counts[topic]++
	}
	for topic, f := range filters {
		assert.Equal(t, len(f), counts[topic], "copies of %q, one per matching filter", topic)
	}
	assert.Zero(t, counts["$dollar/x"])
	sub.expectNothing()
}

// [MQTT-3.3.2-7] "A receiver MUST NOT carry forward any Topic Alias mappings
// from one Network Connection to another." The mapping made on the first
// connection of a persistent session is gone on the second.
func TestPublish_MQTT_3_3_2_7(t *testing.T) {
	addr := p2Start(t)
	watch, _ := p2Connect(t, addr, "watch")
	p2Sub(t, watch.rawClient, packet.Subscription{Filter: "alias/#", QoS: 1})

	first := p2Dial(t, addr)
	first.connect(rawConnect("aliaser", 60))
	first.send(&packet.Publish{QoS: 1, PacketID: 1, Topic: "alias/t",
		Properties: &packet.Properties{TopicAlias: packet.Uint16(5)}, Payload: []byte("mapped")})
	p2Next[*packet.Puback](t, first.rawClient)
	assert.Equal(t, "alias/t", p2Next[*packet.Publish](t, watch.rawClient).Topic)
	first.drop()

	second := p2Dial(t, addr)
	cp := rawConnect("aliaser", 60)
	cp.CleanStart = false
	second.send(cp)
	ack := p2Next[*packet.Connack](t, second.rawClient)
	require.True(t, ack.SessionPresent, "the session, and so any state a broker might wrongly keep, is resumed")
	second.send(&packet.Publish{QoS: 1, PacketID: 2, Topic: "",
		Properties: &packet.Properties{TopicAlias: packet.Uint16(5)}, Payload: []byte("stale")})
	d := second.closedWith()
	require.NotNil(t, d, "a Topic Alias with no mapping on this connection is refused")
	assert.Equal(t, packet.TopicAliasInvalid, d.ReasonCode)
	watch.expectNothing()
}

// [MQTT-3.3.2-8] "A sender MUST NOT send a PUBLISH packet containing a Topic
// Alias which has the value 0." The broker, as a sender, to a client that
// accepts aliases: it sends none, or a non-zero one within the client's maximum.
func TestPublish_MQTT_3_3_2_8(t *testing.T) {
	addr := p2Start(t)
	sub, _ := p2Connect(t, addr, "sub", func(c *packet.Connect) {
		c.Properties.TopicAliasMaximum = packet.Uint16(10)
	})
	p2Sub(t, sub.rawClient, packet.Subscription{Filter: "al/#", QoS: 0})
	pub, _ := p2Connect(t, addr, "pub")
	for i := 0; i < 5; i++ {
		pub.send(&packet.Publish{Topic: "al/same/topic", Payload: []byte("x")})
		got := p2Next[*packet.Publish](t, sub.rawClient)
		if got.Properties != nil && got.Properties.TopicAlias != nil {
			assert.NotZero(t, *got.Properties.TopicAlias)
			assert.LessOrEqual(t, *got.Properties.TopicAlias, uint16(10))
		}
	}
}

// [MQTT-3.3.2-12] "A Server MUST accept all Topic Alias values greater than 0
// and less than or equal to the Topic Alias Maximum value that it returned in
// the CONNACK packet."
func TestPublish_MQTT_3_3_2_12(t *testing.T) {
	addr := p2Start(t, func(o *natsmqtt5.Options) { o.TopicAliasMaximum = new(uint16); *o.TopicAliasMaximum = 3 })
	sub, _ := p2Connect(t, addr, "sub")
	p2Sub(t, sub.rawClient, packet.Subscription{Filter: "al/#", QoS: 1})
	pub, ack := p2Connect(t, addr, "pub")
	require.NotNil(t, ack.Properties)
	require.NotNil(t, ack.Properties.TopicAliasMaximum)
	require.Equal(t, uint16(3), *ack.Properties.TopicAliasMaximum)

	for alias := uint16(1); alias <= 3; alias++ {
		name := "al/" + strconv.Itoa(int(alias))
		pub.send(&packet.Publish{QoS: 1, PacketID: alias, Topic: name,
			Properties: &packet.Properties{TopicAlias: packet.Uint16(alias)}, Payload: []byte("set")})
		assert.Equal(t, packet.Success, p2Next[*packet.Puback](t, pub.rawClient).ReasonCode)
		assert.Equal(t, name, p2RecvAck(t, sub).Topic)
	}
	// Each alias, up to and including the maximum, now stands for its topic.
	for alias := uint16(1); alias <= 3; alias++ {
		pub.send(&packet.Publish{QoS: 1, PacketID: 10 + alias, Topic: "",
			Properties: &packet.Properties{TopicAlias: packet.Uint16(alias)}, Payload: []byte("use")})
		assert.Equal(t, packet.Success, p2Next[*packet.Puback](t, pub.rawClient).ReasonCode)
		got := p2RecvAck(t, sub)
		assert.Equal(t, "al/"+strconv.Itoa(int(alias)), got.Topic)
		assert.Equal(t, "use", string(got.Payload))
	}
	// One above the maximum is not within the promise, and is refused.
	pub.send(&packet.Publish{QoS: 1, PacketID: 20, Topic: "al/4",
		Properties: &packet.Properties{TopicAlias: packet.Uint16(4)}})
	d := pub.closedWith()
	require.NotNil(t, d)
	assert.Equal(t, packet.TopicAliasInvalid, d.ReasonCode)
}

// p2RecvAck reads a QoS 1 PUBLISH and acknowledges it.
func p2RecvAck(t *testing.T, c *p2Client) *packet.Publish {
	t.Helper()
	p := p2Next[*packet.Publish](t, c.rawClient)
	c.send(&packet.Puback{Ack: packet.Ack{PacketID: p.PacketID}})
	return p
}

// [MQTT-3.3.2-13] "The Response Topic MUST be a UTF-8 Encoded String." and
// [MQTT-3.3.2-19] "The Content Type MUST be a UTF-8 Encoded String." A PUBLISH
// whose property is not well-formed UTF-8 is a Malformed Packet; the broker
// closes the connection and forwards nothing.
func TestPublish_MQTT_3_3_2_13_and_19(t *testing.T) {
	cases := []struct {
		id    string
		name  string
		props []byte
	}{
		{"MQTT-3.3.2-13", "Response Topic", []byte{0x08, 0x00, 0x02, 'r', 0xff}},
		{"MQTT-3.3.2-19", "Content Type", []byte{0x03, 0x00, 0x02, 'c', 0xff}},
		{"MQTT-3.3.2-13", "Response Topic with U+0000", []byte{0x08, 0x00, 0x02, 'r', 0x00}},
		{"MQTT-3.3.2-19", "Content Type with U+0000", []byte{0x03, 0x00, 0x02, 'c', 0x00}},
	}
	for _, tc := range cases {
		t.Run(tc.id+" "+tc.name, func(t *testing.T) {
			addr := p2Start(t)
			watch, _ := p2Connect(t, addr, "watch")
			p2Sub(t, watch.rawClient, packet.Subscription{Filter: "utf/#", QoS: 0})
			bad, _ := p2Connect(t, addr, "bad")
			bad.writeBytes(p2PublishBytes(0, "utf/x", 0, tc.props, "x"))
			d := bad.closedWith()
			if d != nil {
				assert.Equal(t, packet.MalformedPacket, d.ReasonCode)
			}
			watch.expectNothing()
		})
	}
}

// --- 3.3.4 PUBLISH actions -----------------------------------------------------

// [MQTT-3.3.4-1] "The receiver of a PUBLISH Packet MUST respond with the packet
// as determined by the QoS in the PUBLISH Packet." (Table 3-3: QoS 0 none, QoS 1
// PUBACK, QoS 2 PUBREC, with the same Packet Identifier; PUBREL is answered by
// PUBCOMP.)
func TestPublish_MQTT_3_3_4_1(t *testing.T) {
	addr := p2Start(t)
	c, _ := p2Connect(t, addr, "pub")

	c.send(&packet.Publish{Topic: "resp/q0", Payload: []byte("x")})
	c.send(&packet.Publish{QoS: 1, PacketID: 11, Topic: "resp/q1", Payload: []byte("x")})
	puback := p2Next[*packet.Puback](t, c.rawClient)
	assert.Equal(t, uint16(11), puback.PacketID, "QoS 0 got no response, QoS 1 got PUBACK")

	c.send(&packet.Publish{QoS: 2, PacketID: 12, Topic: "resp/q2", Payload: []byte("x")})
	pubrec := p2Next[*packet.Pubrec](t, c.rawClient)
	assert.Equal(t, uint16(12), pubrec.PacketID)
	c.send(&packet.Pubrel{Ack: packet.Ack{PacketID: 12}})
	pubcomp := p2Next[*packet.Pubcomp](t, c.rawClient)
	assert.Equal(t, uint16(12), pubcomp.PacketID)
	c.expectNothing()
}

// [MQTT-3.3.4-2] "In this case the Server MUST deliver the message to the
// Client respecting the maximum QoS of all the matching subscriptions."
// (overlapping subscriptions, here "ov/#" at QoS 0 and "ov/x" at QoS 1, and a
// QoS 2 publish: the highest QoS any copy reaches is 1 and no copy exceeds its
// own subscription's grant.)
func TestPublish_MQTT_3_3_4_2(t *testing.T) {
	addr := p2Start(t)
	sub, _ := p2Connect(t, addr, "sub")
	p2Sub(t, sub.rawClient, packet.Subscription{Filter: "ov/#", QoS: 0})
	p2Sub(t, sub.rawClient, packet.Subscription{Filter: "ov/x", QoS: 1})
	pub, _ := p2Connect(t, addr, "pub")
	pub.send(&packet.Publish{QoS: 2, PacketID: 1, Topic: "ov/x", Payload: []byte("m")})
	p2Next[*packet.Pubrec](t, pub.rawClient)

	qos := map[packet.QoS]int{}
	for i := 0; i < 2; i++ {
		got := p2Next[*packet.Publish](t, sub.rawClient)
		qos[got.QoS]++
		if got.QoS == 1 {
			sub.send(&packet.Puback{Ack: packet.Ack{PacketID: got.PacketID}})
		}
	}
	assert.Equal(t, map[packet.QoS]int{0: 1, 1: 1}, qos,
		"the maximum QoS of the matching subscriptions (1) is delivered; none is raised to the publisher's 2")
	sub.expectNothing()
}

// [MQTT-3.3.4-3] "If the Client specified a Subscription Identifier for any of
// the overlapping subscriptions the Server MUST send those Subscription
// Identifiers in the message which is published as the result of the
// subscriptions." and
// [MQTT-3.3.4-5] "If the Server sends multiple PUBLISH packets it MUST send, in
// each of them, the Subscription Identifier of the matching subscription if it
// has a Subscription Identifier." [MQTT-3.3.4-4] ("If the Server sends a single
// copy ... MUST include ... all") never applies: the broker always sends one
// copy per subscription, which this test shows by counting exactly two packets
// each carrying exactly one identifier.
func TestPublish_MQTT_3_3_4_3_and_5(t *testing.T) {
	addr := p2Start(t)
	sub, _ := p2Connect(t, addr, "sub")
	for i, f := range []string{"sid/#", "sid/x"} {
		sub.subID++
		sub.send(&packet.Subscribe{PacketID: sub.subID,
			Properties:    &packet.Properties{SubscriptionIdentifiers: []int{i + 1}},
			Subscriptions: []packet.Subscription{{Filter: f, QoS: 0}}})
		p2Next[*packet.Suback](t, sub.rawClient)
	}
	// A third overlapping subscription without an identifier.
	p2Sub(t, sub.rawClient, packet.Subscription{Filter: "sid/+", QoS: 0})

	pub, _ := p2Connect(t, addr, "pub")
	pub.send(&packet.Publish{Topic: "sid/x", Payload: []byte("m")})

	var ids [][]int
	for i := 0; i < 3; i++ {
		got := p2Next[*packet.Publish](t, sub.rawClient)
		var have []int
		if got.Properties != nil {
			have = got.Properties.SubscriptionIdentifiers
		}
		ids = append(ids, have)
	}
	sub.expectNothing()
	assert.ElementsMatch(t, [][]int{{1}, {2}, nil}, ids,
		"one copy per subscription, each with its own identifier and none with another's")
}

// [MQTT-3.3.4-9] "The Server MUST NOT send more than Receive Maximum QoS 1 and
// QoS 2 PUBLISH packets for which it has not received PUBACK, PUBCOMP, or PUBREC
// with a Reason Code of 128 or greater from the Client."
func TestPublish_MQTT_3_3_4_9(t *testing.T) {
	t.Run("QoS 1 holds a slot until PUBACK", func(t *testing.T) {
		addr := p2Start(t)
		sub, _ := p2Connect(t, addr, "sub", func(c *packet.Connect) { c.Properties.ReceiveMaximum = packet.Uint16(2) })
		p2Sub(t, sub.rawClient, packet.Subscription{Filter: "rm/#", QoS: 1})
		pub, _ := p2Connect(t, addr, "pub")
		for i := 1; i <= 5; i++ {
			pub.send(&packet.Publish{QoS: 1, PacketID: uint16(i), Topic: "rm/q1", Payload: []byte{byte('0' + i)}})
			p2Next[*packet.Puback](t, pub.rawClient)
		}
		m1 := p2Next[*packet.Publish](t, sub.rawClient)
		m2 := p2Next[*packet.Publish](t, sub.rawClient)
		assert.Equal(t, "1", string(m1.Payload))
		assert.Equal(t, "2", string(m2.Payload))
		sub.expectNothing() // a third would be the 3rd unacknowledged

		sub.send(&packet.Puback{Ack: packet.Ack{PacketID: m1.PacketID}})
		m3 := p2Next[*packet.Publish](t, sub.rawClient)
		assert.Equal(t, "3", string(m3.Payload))
		sub.expectNothing()
	})

	t.Run("QoS 2 holds a slot until PUBCOMP", func(t *testing.T) {
		addr := p2Start(t)
		sub, _ := p2Connect(t, addr, "sub", func(c *packet.Connect) { c.Properties.ReceiveMaximum = packet.Uint16(1) })
		p2Sub(t, sub.rawClient, packet.Subscription{Filter: "rm/#", QoS: 2})
		pub, _ := p2Connect(t, addr, "pub")
		for i := 1; i <= 2; i++ {
			pub.send(&packet.Publish{QoS: 2, PacketID: uint16(i), Topic: "rm/q2", Payload: []byte{byte('0' + i)}})
			p2Next[*packet.Pubrec](t, pub.rawClient)
			pub.send(&packet.Pubrel{Ack: packet.Ack{PacketID: uint16(i)}})
			p2Next[*packet.Pubcomp](t, pub.rawClient)
		}
		m1 := p2Next[*packet.Publish](t, sub.rawClient)
		sub.send(&packet.Pubrec{Ack: packet.Ack{PacketID: m1.PacketID}})
		p2Next[*packet.Pubrel](t, sub.rawClient)
		sub.expectNothing() // PUBREC alone does not free the slot
		sub.send(&packet.Pubcomp{Ack: packet.Ack{PacketID: m1.PacketID}})
		assert.Equal(t, "2", string(p2Next[*packet.Publish](t, sub.rawClient).Payload))
	})

	t.Run("a PUBREC with a Reason Code of 128 or more frees the slot", func(t *testing.T) {
		addr := p2Start(t)
		sub, _ := p2Connect(t, addr, "sub", func(c *packet.Connect) { c.Properties.ReceiveMaximum = packet.Uint16(1) })
		p2Sub(t, sub.rawClient, packet.Subscription{Filter: "rm/#", QoS: 2})
		pub, _ := p2Connect(t, addr, "pub")
		for i := 1; i <= 2; i++ {
			pub.send(&packet.Publish{QoS: 2, PacketID: uint16(i), Topic: "rm/q2", Payload: []byte{byte('0' + i)}})
			p2Next[*packet.Pubrec](t, pub.rawClient)
			pub.send(&packet.Pubrel{Ack: packet.Ack{PacketID: uint16(i)}})
			p2Next[*packet.Pubcomp](t, pub.rawClient)
		}
		m1 := p2Next[*packet.Publish](t, sub.rawClient)
		sub.expectNothing()
		sub.send(&packet.Pubrec{Ack: packet.Ack{PacketID: m1.PacketID, ReasonCode: packet.UnspecifiedError}})
		assert.Equal(t, "2", string(p2Next[*packet.Publish](t, sub.rawClient).Payload))
	})
}

// [MQTT-3.3.4-10] "The Server MUST NOT delay the sending of any packets other
// than PUBLISH packets due to having sent Receive Maximum PUBLISH packets
// without receiving acknowledgements for them."
func TestPublish_MQTT_3_3_4_10(t *testing.T) {
	addr := p2Start(t)
	c, _ := p2Connect(t, addr, "c", func(c *packet.Connect) { c.Properties.ReceiveMaximum = packet.Uint16(1) })
	p2Sub(t, c.rawClient, packet.Subscription{Filter: "full/#", QoS: 1})
	pub, _ := p2Connect(t, addr, "pub")
	for i := 1; i <= 2; i++ {
		pub.send(&packet.Publish{QoS: 1, PacketID: uint16(i), Topic: "full/x", Payload: []byte{byte('0' + i)}})
		p2Next[*packet.Puback](t, pub.rawClient)
	}
	first := p2Next[*packet.Publish](t, c.rawClient)
	require.Equal(t, "1", string(first.Payload))
	// The quota is spent; the second message waits. Everything else must not.
	c.send(&packet.Pingreq{})
	p2Next[*packet.Pingresp](t, c.rawClient)
	assert.Equal(t, packet.GrantedQoS1, p2Sub(t, c.rawClient, packet.Subscription{Filter: "other/#", QoS: 1}))
	c.send(&packet.Unsubscribe{PacketID: 90, Filters: []string{"other/#"}})
	assert.Equal(t, uint16(90), p2Next[*packet.Unsuback](t, c.rawClient).PacketID)
	c.send(&packet.Publish{QoS: 1, PacketID: 91, Topic: "other/y", Payload: []byte("own")})
	assert.Equal(t, uint16(91), p2Next[*packet.Puback](t, c.rawClient).PacketID)
	c.send(&packet.Publish{QoS: 2, PacketID: 92, Topic: "other/y", Payload: []byte("own")})
	assert.Equal(t, uint16(92), p2Next[*packet.Pubrec](t, c.rawClient).PacketID)
	c.send(&packet.Pubrel{Ack: packet.Ack{PacketID: 92}})
	assert.Equal(t, uint16(92), p2Next[*packet.Pubcomp](t, c.rawClient).PacketID)
	c.expectNothing()
}

// --- 3.4 - 3.7 acknowledgement packets -----------------------------------------

var (
	pubackCodes = []packet.ReasonCode{0x00, 0x10, 0x80, 0x83, 0x87, 0x90, 0x91, 0x97, 0x99}
	pubrelCodes = []packet.ReasonCode{0x00, 0x92}
)

// [MQTT-3.4.2-1] "The Client or Server sending the PUBACK packet MUST use one of
// the PUBACK Reason Codes." and [MQTT-3.5.2-1] for PUBREC (the sets are the same
// in Tables 3-5 and 3-7). Success, a refusal by the Authorizer and an invalid
// topic name each produce a code from the table.
func TestPuback_MQTT_3_4_2_1_and_Pubrec_MQTT_3_5_2_1(t *testing.T) {
	addr := p2Start(t, p2Deny)
	c, _ := p2Connect(t, addr, "c")

	type want struct {
		topic string
		code  packet.ReasonCode
	}
	for i, w := range []want{{"ok/x", 0x00}, {"deny/x", 0x87}, {"bad/+", 0x90}} {
		id1, id2 := uint16(100+i), uint16(200+i)
		c.send(&packet.Publish{QoS: 1, PacketID: id1, Topic: w.topic, Payload: []byte("x")})
		ack := p2Next[*packet.Puback](t, c.rawClient)
		assert.Equal(t, id1, ack.PacketID)
		assert.Equal(t, w.code, ack.ReasonCode, "PUBACK for %q", w.topic)
		assert.True(t, containsCode(pubackCodes, ack.ReasonCode))

		c.send(&packet.Publish{QoS: 2, PacketID: id2, Topic: w.topic, Payload: []byte("x")})
		rec := p2Next[*packet.Pubrec](t, c.rawClient)
		assert.Equal(t, id2, rec.PacketID)
		assert.Equal(t, w.code, rec.ReasonCode, "PUBREC for %q", w.topic)
		assert.True(t, containsCode(pubackCodes, rec.ReasonCode))
		if w.code == 0 {
			c.send(&packet.Pubrel{Ack: packet.Ack{PacketID: id2}})
			p2Next[*packet.Pubcomp](t, c.rawClient)
		}
	}
}

// p2AckRun drives every acknowledgement a Server sends through a client that
// advertised a tiny Maximum Packet Size, measuring each packet on the wire.
type p2AckRun struct {
	max      uint32
	puback   []p2Sent // refusal and success
	pubrec   []p2Sent
	pubrel   []p2Sent
	pubcomp  []p2Sent
	suback   []p2Sent
	unsuback []p2Sent
}

type p2Sent struct {
	p    packet.Packet
	size int
}

// p2AckScenario runs every acknowledgement type past a client whose Maximum
// Packet Size is just the CONNACK's size. The client sends User Properties of its own to tempt an echo.
func p2AckScenario(t *testing.T) *p2AckRun {
	t.Helper()
	addr := p2Start(t, p2Deny)
	// The CONNACK is itself bound by the limit, so measure it first: the limit
	// is exactly the CONNACK's size, far below what a Reason String on a
	// refusal (or an echoed User Property) would add to an acknowledgement.
	probe := p2Dial(t, addr)
	pc := rawConnect("probe", 0)
	pc.CleanStart = true
	probe.send(pc)
	_, size := probe.readSized()
	run := &p2AckRun{max: uint32(size)}
	c, _ := p2Connect(t, addr, "tiny", func(cp *packet.Connect) {
		cp.Properties.MaximumPacketSize = packet.Uint32(run.max)
	})
	user := &packet.Properties{User: []packet.UserProperty{{Key: "k", Value: "v"}}}
	next := func() p2Sent {
		p, n := c.readSized()
		return p2Sent{p, n}
	}

	// SUBACK (success and failure), UNSUBACK.
	c.send(&packet.Subscribe{PacketID: 1, Properties: user, Subscriptions: []packet.Subscription{{Filter: "t", QoS: 2}}})
	run.suback = append(run.suback, next())
	c.send(&packet.Subscribe{PacketID: 2, Properties: user, Subscriptions: []packet.Subscription{{Filter: "deny/t", QoS: 2}, {Filter: "bad/#/x", QoS: 1}}})
	run.suback = append(run.suback, next())

	// PUBACK / PUBREC / PUBCOMP for the client's own publishes, accepted and refused.
	for i, topic := range []string{"u", "deny/t"} {
		id := uint16(10 + i)
		c.send(&packet.Publish{QoS: 1, PacketID: id, Topic: topic, Properties: user, Payload: []byte("x")})
		run.puback = append(run.puback, next())
	}
	c.send(&packet.Publish{QoS: 2, PacketID: 20, Topic: "u", Properties: user, Payload: []byte("x")})
	run.pubrec = append(run.pubrec, next())
	c.send(&packet.Pubrel{Ack: packet.Ack{PacketID: 20, Properties: user}})
	run.pubcomp = append(run.pubcomp, next())
	c.send(&packet.Pubrel{Ack: packet.Ack{PacketID: 21, Properties: user}}) // unknown: 0x92
	run.pubcomp = append(run.pubcomp, next())
	c.send(&packet.Publish{QoS: 2, PacketID: 22, Topic: "deny/t", Properties: user, Payload: []byte("x")})
	run.pubrec = append(run.pubrec, next())

	// PUBREL for a QoS 2 delivery to this client, and for an unknown id.
	pub, _ := p2Connect(t, addr, "pub")
	pub.send(&packet.Publish{QoS: 2, PacketID: 1, Topic: "t", Payload: []byte("x")})
	p2Next[*packet.Pubrec](t, pub.rawClient)
	del, _ := next().p.(*packet.Publish)
	require.NotNil(t, del, "the delivery must fit the client's maximum")
	c.send(&packet.Pubrec{Ack: packet.Ack{PacketID: del.PacketID, Properties: user}})
	run.pubrel = append(run.pubrel, next())
	c.send(&packet.Pubcomp{Ack: packet.Ack{PacketID: del.PacketID}})
	c.send(&packet.Pubrec{Ack: packet.Ack{PacketID: 777}}) // unknown: PUBREL 0x92
	run.pubrel = append(run.pubrel, next())

	c.send(&packet.Unsubscribe{PacketID: 30, Properties: user, Filters: []string{"t", "nothing"}})
	run.unsuback = append(run.unsuback, next())
	if d, ok := run.unsuback[0].p.(*packet.Disconnect); ok {
		t.Fatalf("DISCONNECT 0x%02X %v", byte(d.ReasonCode), d.Properties)
	}
	return run
}

// p2Bare asserts a Server acknowledgement fits the client's Maximum Packet Size
// and carries no Reason String and no User Property.
func p2Bare(t *testing.T, run *p2AckRun, sent []p2Sent, props func(packet.Packet) *packet.Properties) {
	t.Helper()
	require.NotEmpty(t, sent)
	for _, s := range sent {
		assert.LessOrEqual(t, uint32(s.size), run.max, "%s is %d bytes", s.p.Type(), s.size)
		if pr := props(s.p); pr != nil {
			assert.Empty(t, pr.ReasonString, "%s carries a Reason String", s.p.Type())
			assert.Empty(t, pr.User, "%s carries User Properties", s.p.Type())
		}
	}
}

// [MQTT-3.4.2-3] "The sender MUST NOT send this property [User Property] if it
// would increase the size of the PUBACK packet beyond the Maximum Packet Size
// specified by the receiver." (the Reason String twin, [MQTT-3.4.2-2], is in
// oversize_ack_test.go.) The broker sends no User Properties, so the packet
// always fits.
func TestPuback_MQTT_3_4_2_3(t *testing.T) {
	run := p2AckScenario(t)
	p2Bare(t, run, run.puback, func(p packet.Packet) *packet.Properties { return p.(*packet.Puback).Properties })
}

// [MQTT-3.5.2-3] The same for PUBREC.
func TestPubrec_MQTT_3_5_2_3(t *testing.T) {
	run := p2AckScenario(t)
	p2Bare(t, run, run.pubrec, func(p packet.Packet) *packet.Properties { return p.(*packet.Pubrec).Properties })
}

// [MQTT-3.6.2-2] and [MQTT-3.6.2-3] "The sender MUST NOT send this
// property/Property if it would increase the size of the PUBREL packet beyond
// the Maximum Packet Size specified by the receiver."
func TestPubrel_MQTT_3_6_2_2_and_3(t *testing.T) {
	run := p2AckScenario(t)
	p2Bare(t, run, run.pubrel, func(p packet.Packet) *packet.Properties { return p.(*packet.Pubrel).Properties })
}

// [MQTT-3.7.2-2] and [MQTT-3.7.2-3] The same for PUBCOMP.
func TestPubcomp_MQTT_3_7_2_2_and_3(t *testing.T) {
	run := p2AckScenario(t)
	p2Bare(t, run, run.pubcomp, func(p packet.Packet) *packet.Properties { return p.(*packet.Pubcomp).Properties })
}

// [MQTT-3.9.2-1] and [MQTT-3.9.2-2] "The Server MUST NOT send this
// Property/property if it would increase the size of the SUBACK packet beyond
// the Maximum Packet Size specified by the Client."
func TestSuback_MQTT_3_9_2_1_and_2(t *testing.T) {
	run := p2AckScenario(t)
	p2Bare(t, run, run.suback, func(p packet.Packet) *packet.Properties { return p.(*packet.Suback).Properties })
}

// [MQTT-3.11.2-1] The same for UNSUBACK.
func TestUnsuback_MQTT_3_11_2_1(t *testing.T) {
	run := p2AckScenario(t)
	p2Bare(t, run, run.unsuback, func(p packet.Packet) *packet.Properties { return p.(*packet.Unsuback).Properties })
}

// [MQTT-3.6.2-1] "The Client or Server sending the PUBREL packet MUST use one
// of the PUBREL Reason Code values": 0x00 Success, 0x92 Packet Identifier not
// found.
func TestPubrel_MQTT_3_6_2_1(t *testing.T) {
	run := p2AckScenario(t)
	require.Len(t, run.pubrel, 2)
	codes := []packet.ReasonCode{}
	for _, s := range run.pubrel {
		code := s.p.(*packet.Pubrel).ReasonCode
		assert.True(t, containsCode(pubrelCodes, code), "PUBREL used 0x%02X", byte(code))
		codes = append(codes, code)
	}
	assert.Equal(t, []packet.ReasonCode{0x00, 0x92}, codes)
}

// [MQTT-3.7.2-1] "The Client or Server sending the PUBCOMP packets MUST use one
// of the PUBCOMP Reason Code values": 0x00 or 0x92.
func TestPubcomp_MQTT_3_7_2_1(t *testing.T) {
	run := p2AckScenario(t)
	require.Len(t, run.pubcomp, 2)
	var codes []packet.ReasonCode
	for _, s := range run.pubcomp {
		code := s.p.(*packet.Pubcomp).ReasonCode
		assert.True(t, containsCode(pubrelCodes, code), "PUBCOMP used 0x%02X", byte(code))
		codes = append(codes, code)
	}
	assert.Equal(t, []packet.ReasonCode{0x00, 0x92}, codes)
}

// --- reserved fixed header bits ------------------------------------------------

// [MQTT-3.6.1-1] "Bits 3,2,1 and 0 of the Fixed Header in the PUBREL packet are
// reserved and MUST be set to 0,0,1 and 0 respectively. The Server MUST treat
// any other value as malformed and close the Network Connection."
// [MQTT-3.8.1-1] is the same for SUBSCRIBE and [MQTT-3.10.1-1] for UNSUBSCRIBE;
// the packet package tests the decoder, this drives the live broker.
func TestReservedFlags_MQTT_3_6_1_1_3_8_1_1_3_10_1_1(t *testing.T) {
	type tc struct {
		id    string
		bytes []byte
	}
	pubrel := func(flags byte) []byte { return []byte{0x60 | flags, 0x02, 0x00, 0x01} }
	subscribeBytes := func(flags byte) []byte {
		// Packet Id 1, no properties, filter "rsv/#", options QoS 0.
		body := []byte{0x00, 0x01, 0x00, 0x00, 0x05, 'r', 's', 'v', '/', '#', 0x00}
		return append([]byte{0x80 | flags, byte(len(body))}, body...)
	}
	unsubscribeBytes := func(flags byte) []byte {
		body := []byte{0x00, 0x01, 0x00, 0x00, 0x05, 'r', 's', 'v', '/', '#'}
		return append([]byte{0xA0 | flags, byte(len(body))}, body...)
	}
	var cases []tc
	for _, f := range []byte{0x0, 0x1, 0x3, 0x6, 0xA, 0xF} {
		cases = append(cases, tc{"MQTT-3.6.1-1 PUBREL flags " + strconv.Itoa(int(f)), pubrel(f)})
		cases = append(cases, tc{"MQTT-3.8.1-1 SUBSCRIBE flags " + strconv.Itoa(int(f)), subscribeBytes(f)})
		cases = append(cases, tc{"MQTT-3.10.1-1 UNSUBSCRIBE flags " + strconv.Itoa(int(f)), unsubscribeBytes(f)})
	}
	addr := p2Start(t)
	for _, c := range cases {
		t.Run(c.id, func(t *testing.T) {
			bad, _ := p2Connect(t, addr, "bad")
			bad.writeBytes(c.bytes)
			d := bad.closedWith() // no PUBCOMP, SUBACK or UNSUBACK is sent first
			if d != nil {
				assert.Equal(t, packet.MalformedPacket, d.ReasonCode)
			}
		})
	}
	// The one valid layout (0,0,1,0) is accepted, so the cases above fail for
	// the flags and nothing else.
	good, _ := p2Connect(t, addr, "good")
	good.writeBytes(subscribeBytes(0x2))
	assert.Equal(t, []packet.ReasonCode{0}, p2Next[*packet.Suback](t, good.rawClient).ReasonCodes)
	good.writeBytes(pubrel(0x2))
	assert.Equal(t, packet.PacketIdentifierNotFound, p2Next[*packet.Pubcomp](t, good.rawClient).ReasonCode)
	good.writeBytes(unsubscribeBytes(0x2))
	assert.Equal(t, []packet.ReasonCode{0}, p2Next[*packet.Unsuback](t, good.rawClient).ReasonCodes)
}

// --- 3.8 SUBSCRIBE ----------------------------------------------------------

// [MQTT-3.8.3-5] "The Server MUST treat a SUBSCRIBE packet as malformed if any
// of Reserved bits in the Payload are non-zero." (bits 6 and 7 of the options)
func TestSubscribe_MQTT_3_8_3_5(t *testing.T) {
	addr := p2Start(t)
	for _, opts := range []byte{0x40, 0x80, 0xC0, 0x41} {
		t.Run(strconv.Itoa(int(opts)), func(t *testing.T) {
			bad, _ := p2Connect(t, addr, "bad")
			body := []byte{0x00, 0x01, 0x00, 0x00, 0x03, 'r', 's', 'v', opts}
			bad.writeBytes(append([]byte{0x82, byte(len(body))}, body...))
			d := bad.closedWith()
			if d != nil {
				assert.Equal(t, packet.MalformedPacket, d.ReasonCode)
			}
		})
	}
}

// [MQTT-3.8.4-1] "When the Server receives a SUBSCRIBE packet from a Client, the
// Server MUST respond with a SUBACK packet." and [MQTT-3.8.4-2] "The SUBACK
// packet MUST have the same Packet Identifier as the SUBSCRIBE packet that it is
// acknowledging." Even when every filter fails.
func TestSubscribe_MQTT_3_8_4_1_and_2(t *testing.T) {
	addr := p2Start(t, p2Deny)
	c, _ := p2Connect(t, addr, "c")
	for _, tc := range []struct {
		id      uint16
		filters []string
		want    []packet.ReasonCode
	}{
		{0x0001, []string{"ok/a"}, []packet.ReasonCode{1}},
		{0xBEEF, []string{"ok/b"}, []packet.ReasonCode{1}},
		{0xFFFF, []string{"bad/#/x"}, []packet.ReasonCode{packet.TopicFilterInvalid}},
		{0x1234, []string{"deny/a", "bad/#/x"}, []packet.ReasonCode{packet.NotAuthorized, packet.TopicFilterInvalid}},
	} {
		var subs []packet.Subscription
		for _, f := range tc.filters {
			subs = append(subs, packet.Subscription{Filter: f, QoS: 1})
		}
		c.send(&packet.Subscribe{PacketID: tc.id, Subscriptions: subs})
		ack := p2Next[*packet.Suback](t, c.rawClient)
		assert.Equal(t, tc.id, ack.PacketID)
		assert.Equal(t, tc.want, ack.ReasonCodes)
	}
	c.expectNothing()
}

// [MQTT-3.8.4-3] "If a Server receives a SUBSCRIBE packet containing a Topic
// Filter that is identical to a Non-shared Subscriptions Topic Filter for the
// current Session then it MUST replace that existing Subscription with a new
// Subscription." After a second SUBSCRIBE of the same filter at a different QoS
// one message produces one copy, at the new QoS, not one per SUBSCRIBE.
func TestSubscribe_MQTT_3_8_4_3(t *testing.T) {
	addr := p2Start(t)
	sub, _ := p2Connect(t, addr, "sub")
	assert.Equal(t, packet.GrantedQoS0, p2Sub(t, sub.rawClient, packet.Subscription{Filter: "rep/x", QoS: 0}))
	assert.Equal(t, packet.GrantedQoS1, p2Sub(t, sub.rawClient, packet.Subscription{Filter: "rep/x", QoS: 1}))
	pub, _ := p2Connect(t, addr, "pub")
	pub.send(&packet.Publish{QoS: 1, PacketID: 1, Topic: "rep/x", Payload: []byte("once")})
	p2Next[*packet.Puback](t, pub.rawClient)
	got := p2Next[*packet.Publish](t, sub.rawClient)
	assert.Equal(t, packet.QoS1, got.QoS, "the new subscription's QoS")
	sub.send(&packet.Puback{Ack: packet.Ack{PacketID: got.PacketID}})
	sub.expectNothing()

	// The replacement also takes the new options: NoLocal now suppresses the
	// subscriber's own publishes.
	assert.Equal(t, packet.GrantedQoS1, p2Sub(t, sub.rawClient, packet.Subscription{Filter: "rep/x", QoS: 1, NoLocal: true}))
	sub.send(&packet.Publish{Topic: "rep/x", Payload: []byte("mine")})
	sub.expectNothing()
}

// [MQTT-3.8.4-5] "If a Server receives a SUBSCRIBE packet that contains multiple
// Topic Filters it MUST handle that packet as if it had received a sequence of
// multiple SUBSCRIBE packets, except that it combines their responses into a
// single SUBACK response." A failing filter does not stop the ones after it.
// [MQTT-3.8.4-6] "The SUBACK packet sent by the Server to the Client MUST
// contain a Reason Code for each Topic Filter/Subscription Option pair." and
// [MQTT-3.9.3-1] "The order of Reason Codes in the SUBACK packet MUST match the
// order of Topic Filters in the SUBSCRIBE packet." [MQTT-3.9.3-2] "The Server
// sending the SUBACK packet MUST send one of the Subscribe Reason Code values
// for each Topic Filter received." (Tables of 3.9.3: 0,1,2,0x80,0x83,0x87,0x8F,
// 0x91,0x97,0x9E,0xA1,0xA2.)
func TestSubscribe_MQTT_3_8_4_5_and_6_and_Suback_MQTT_3_9_3_1_and_2(t *testing.T) {
	addr := p2Start(t, p2Deny)
	sub, _ := p2Connect(t, addr, "sub")
	filters := []packet.Subscription{
		{Filter: "multi/q2", QoS: 2},
		{Filter: "bad/#/x", QoS: 1},
		{Filter: "deny/a", QoS: 1},
		{Filter: "multi/q0", QoS: 0},
		{Filter: "multi/q1", QoS: 1},
	}
	sub.send(&packet.Subscribe{PacketID: 5, Subscriptions: filters})
	ack := p2Next[*packet.Suback](t, sub.rawClient)
	assert.Equal(t, uint16(5), ack.PacketID)
	require.Len(t, ack.ReasonCodes, len(filters), "one Reason Code per filter")
	assert.Equal(t, []packet.ReasonCode{2, packet.TopicFilterInvalid, packet.NotAuthorized, 0, 1}, ack.ReasonCodes,
		"in the order of the filters")
	valid := []packet.ReasonCode{0, 1, 2, 0x80, 0x83, 0x87, 0x8F, 0x91, 0x97, 0x9E, 0xA1, 0xA2}
	for _, c := range ack.ReasonCodes {
		assert.True(t, containsCode(valid, c), "0x%02X is not a Subscribe Reason Code", byte(c))
	}
	sub.expectNothing() // one SUBACK, not one per filter

	// The filters after the failing ones are live.
	pub, _ := p2Connect(t, addr, "pub")
	for _, name := range []string{"multi/q2", "multi/q0", "multi/q1"} {
		pub.send(&packet.Publish{Topic: name, Payload: []byte(name)})
		assert.Equal(t, name, p2Next[*packet.Publish](t, sub.rawClient).Topic)
	}
}

// [MQTT-3.8.4-7] "This Reason Code MUST either show the maximum QoS that was
// granted for that Subscription or indicate that the subscription failed." With
// the broker capped at QoS 1 a request for QoS 2 is answered 1, never 2.
func TestSubscribe_MQTT_3_8_4_7(t *testing.T) {
	one := uint8(1)
	addr := p2Start(t, func(o *natsmqtt5.Options) { o.MaximumQoS = &one })
	sub, ack := p2Connect(t, addr, "sub")
	require.NotNil(t, ack.Properties.MaximumQoS)
	require.Equal(t, byte(1), *ack.Properties.MaximumQoS)

	assert.Equal(t, packet.GrantedQoS1, p2Sub(t, sub.rawClient, packet.Subscription{Filter: "cap/a", QoS: 2}))
	assert.Equal(t, packet.GrantedQoS0, p2Sub(t, sub.rawClient, packet.Subscription{Filter: "cap/b", QoS: 0}))
	assert.Equal(t, packet.TopicFilterInvalid, p2Sub(t, sub.rawClient, packet.Subscription{Filter: "cap/#/c", QoS: 1}))

	// And the grant is what is delivered.
	pub, _ := p2Connect(t, addr, "pub")
	pub.send(&packet.Publish{Topic: "cap/a", Payload: []byte("x")})
	assert.Equal(t, packet.QoS0, p2Next[*packet.Publish](t, sub.rawClient).QoS)
}

// --- 3.10 UNSUBSCRIBE -------------------------------------------------------------

// [MQTT-3.10.4-1] "The Topic Filters (whether they contain wildcards or not)
// supplied in an UNSUBSCRIBE packet MUST be compared character-by-character with
// the current set of Topic Filters held by the Server for the Client. If any
// filter matches exactly then its owning Subscription MUST be deleted."
func TestUnsubscribe_MQTT_3_10_4_1(t *testing.T) {
	addr := p2Start(t)
	sub, _ := p2Connect(t, addr, "sub")
	for _, f := range []string{"un/+", "un/b", "un/#"} {
		p2Sub(t, sub.rawClient, packet.Subscription{Filter: f, QoS: 0})
	}
	pub, _ := p2Connect(t, addr, "pub")
	count := func(name string) int {
		pub.send(&packet.Publish{Topic: name, Payload: []byte("m")})
		n := 0
		for {
			_ = sub.nc.SetReadDeadline(time.Now().Add(400 * time.Millisecond))
			if _, err := packet.Read(sub.r, 0); err != nil {
				return n
			}
			n++
		}
	}
	require.Equal(t, 3, count("un/b"))

	// "un/b" matches only the filter that is identical to it: not "un/+", which
	// it merely matches, and not "un/#".
	sub.send(&packet.Unsubscribe{PacketID: 1, Filters: []string{"un/b"}})
	assert.Equal(t, []packet.ReasonCode{packet.Success}, p2Next[*packet.Unsuback](t, sub.rawClient).ReasonCodes)
	assert.Equal(t, 2, count("un/b"))

	// A filter that only matches the same topics ("un/c" for "un/+") deletes nothing.
	sub.send(&packet.Unsubscribe{PacketID: 2, Filters: []string{"un/c", "un/#/", "UN/+", "un/+ "}})
	assert.Equal(t, []packet.ReasonCode{0x11, 0x11, 0x11, 0x11}, p2Next[*packet.Unsuback](t, sub.rawClient).ReasonCodes)
	assert.Equal(t, 2, count("un/b"))

	sub.send(&packet.Unsubscribe{PacketID: 3, Filters: []string{"un/+"}})
	p2Next[*packet.Unsuback](t, sub.rawClient)
	assert.Equal(t, 1, count("un/b"), "only un/# is left")
}

// [MQTT-3.10.4-2] "When a Server receives UNSUBSCRIBE It MUST stop adding any
// new messages which match the Topic Filters, for delivery to the Client." Once
// the UNSUBACK is out, nothing more arrives, at any QoS and via a '#' filter.
func TestUnsubscribe_MQTT_3_10_4_2(t *testing.T) {
	addr := p2Start(t)
	sub, _ := p2Connect(t, addr, "sub")
	p2Sub(t, sub.rawClient, packet.Subscription{Filter: "stop/#", QoS: 1})
	p2Sub(t, sub.rawClient, packet.Subscription{Filter: "stop/q0", QoS: 0})
	pub, _ := p2Connect(t, addr, "pub")

	sub.send(&packet.Unsubscribe{PacketID: 1, Filters: []string{"stop/#", "stop/q0"}})
	p2Next[*packet.Unsuback](t, sub.rawClient)
	for i, q := range []packet.QoS{0, 1} {
		pub.send(&packet.Publish{QoS: q, PacketID: uint16(i + 1), Topic: "stop/q0", Payload: []byte("late")})
		if q == 1 {
			p2Next[*packet.Puback](t, pub.rawClient)
		}
	}
	pub.send(&packet.Publish{Topic: "stop", Payload: []byte("parent")})
	sub.expectNothing()
}

// [MQTT-3.10.4-3] "When a Server receives UNSUBSCRIBE It MUST complete the
// delivery of any QoS 1 or QoS 2 messages which match the Topic Filters and it
// has started to send to the Client." A QoS 2 exchange begun before the
// UNSUBSCRIBE finishes after it (PUBREC answered by PUBREL, PUBCOMP accepted),
// and a QoS 1 delivery can still be acknowledged without the broker treating the
// PUBACK as unknown.
func TestUnsubscribe_MQTT_3_10_4_3(t *testing.T) {
	addr := p2Start(t)
	sub, _ := p2Connect(t, addr, "sub")
	p2Sub(t, sub.rawClient, packet.Subscription{Filter: "fin/q1", QoS: 1})
	p2Sub(t, sub.rawClient, packet.Subscription{Filter: "fin/q2", QoS: 2})
	pub, _ := p2Connect(t, addr, "pub")
	pub.send(&packet.Publish{QoS: 1, PacketID: 1, Topic: "fin/q1", Payload: []byte("one")})
	p2Next[*packet.Puback](t, pub.rawClient)
	pub.send(&packet.Publish{QoS: 2, PacketID: 2, Topic: "fin/q2", Payload: []byte("two")})
	p2Next[*packet.Pubrec](t, pub.rawClient)
	pub.send(&packet.Pubrel{Ack: packet.Ack{PacketID: 2}})
	p2Next[*packet.Pubcomp](t, pub.rawClient)

	m1 := p2Next[*packet.Publish](t, sub.rawClient)
	m2 := p2Next[*packet.Publish](t, sub.rawClient)
	byTopic := map[string]*packet.Publish{m1.Topic: m1, m2.Topic: m2}
	q1, q2 := byTopic["fin/q1"], byTopic["fin/q2"]
	require.NotNil(t, q1)
	require.NotNil(t, q2)

	sub.send(&packet.Unsubscribe{PacketID: 9, Filters: []string{"fin/q1", "fin/q2"}})
	p2Next[*packet.Unsuback](t, sub.rawClient)

	sub.send(&packet.Puback{Ack: packet.Ack{PacketID: q1.PacketID}})
	sub.send(&packet.Pubrec{Ack: packet.Ack{PacketID: q2.PacketID}})
	rel := p2Next[*packet.Pubrel](t, sub.rawClient)
	assert.Equal(t, q2.PacketID, rel.PacketID)
	assert.Equal(t, packet.Success, rel.ReasonCode, "the exchange is still known")
	sub.send(&packet.Pubcomp{Ack: packet.Ack{PacketID: q2.PacketID}})

	// No DISCONNECT for an "unknown" acknowledgement, and the connection works.
	sub.send(&packet.Pingreq{})
	p2Next[*packet.Pingresp](t, sub.rawClient)
}

// [MQTT-3.10.4-4] "The Server MUST respond to an UNSUBSCRIBE request by sending
// an UNSUBACK packet." and [MQTT-3.10.4-5] "The UNSUBACK packet MUST have the
// same Packet Identifier as the UNSUBSCRIBE packet. Even where no Topic
// Subscriptions are deleted, the Server MUST respond with an UNSUBACK."
func TestUnsubscribe_MQTT_3_10_4_4_and_5(t *testing.T) {
	addr := p2Start(t)
	c, _ := p2Connect(t, addr, "c")
	p2Sub(t, c.rawClient, packet.Subscription{Filter: "ua/x", QoS: 0})
	for _, tc := range []struct {
		id     uint16
		filter string
		want   packet.ReasonCode
	}{
		{0x0001, "ua/x", packet.Success},
		{0xCAFE, "ua/x", packet.NoSubscriptionExisted}, // already gone
		{0xFFFF, "never/subscribed", packet.NoSubscriptionExisted},
	} {
		c.send(&packet.Unsubscribe{PacketID: tc.id, Filters: []string{tc.filter}})
		ack := p2Next[*packet.Unsuback](t, c.rawClient)
		assert.Equal(t, tc.id, ack.PacketID)
		assert.Equal(t, []packet.ReasonCode{tc.want}, ack.ReasonCodes)
	}
	c.expectNothing()
}

// [MQTT-3.10.4-6] "If a Server receives an UNSUBSCRIBE packet that contains
// multiple Topic Filters, it MUST process that packet as if it had received a
// sequence of multiple UNSUBSCRIBE packets, except that it sends just one
// UNSUBACK response."
func TestUnsubscribe_MQTT_3_10_4_6(t *testing.T) {
	addr := p2Start(t)
	sub, _ := p2Connect(t, addr, "sub")
	p2Sub(t, sub.rawClient, packet.Subscription{Filter: "mu/a", QoS: 0})
	p2Sub(t, sub.rawClient, packet.Subscription{Filter: "mu/b", QoS: 0})
	p2Sub(t, sub.rawClient, packet.Subscription{Filter: "mu/keep", QoS: 0})

	sub.send(&packet.Unsubscribe{PacketID: 4, Filters: []string{"mu/a", "mu/unknown", "mu/b"}})
	ack := p2Next[*packet.Unsuback](t, sub.rawClient)
	assert.Equal(t, uint16(4), ack.PacketID)
	assert.Equal(t, []packet.ReasonCode{packet.Success, packet.NoSubscriptionExisted, packet.Success}, ack.ReasonCodes)
	sub.expectNothing() // just one UNSUBACK

	pub, _ := p2Connect(t, addr, "pub")
	for _, name := range []string{"mu/a", "mu/b", "mu/keep"} {
		pub.send(&packet.Publish{Topic: name, Payload: []byte(name)})
	}
	assert.Equal(t, "mu/keep", p2Next[*packet.Publish](t, sub.rawClient).Topic, "only the untouched subscription remains")
	sub.expectNothing()
}

// --- batch 2: retain handling, forwarding, QoS, DUP -----------------------------

// [MQTT-3.3.1-1] "The DUP flag MUST be set to 1 by the Client or Server when it
// attempts to re-deliver a PUBLISH packet." The first delivery has DUP 0; the
// copy resent when the session resumes has DUP 1 and the same Packet Identifier.
func TestPublish_MQTT_3_3_1_1(t *testing.T) {
	addr := p2Start(t)
	sub := dialRaw(t, addr)
	sub.connect(rawConnect("dupsub", 60))
	sub.subscribe("dup/re", 1)
	pub, _ := p2Connect(t, addr, "pub")
	pub.send(&packet.Publish{QoS: 1, PacketID: 1, Topic: "dup/re", Payload: []byte("again")})
	p2Next[*packet.Puback](t, pub.rawClient)

	first := sub.expectPublish()
	assert.False(t, first.Dup, "the first attempt is not a re-delivery")
	sub.drop() // never acknowledged

	again := dialRaw(t, addr)
	cp := rawConnect("dupsub", 60)
	cp.CleanStart = false
	ack := again.connect(cp)
	require.True(t, ack.SessionPresent)
	re := again.expectPublish()
	assert.True(t, re.Dup, "a re-delivery has DUP 1")
	assert.Equal(t, first.PacketID, re.PacketID)
	assert.Equal(t, "again", string(re.Payload))
}

// [MQTT-3.3.1-9] "If Retain Handling is set to 0 the Server MUST send the
// retained messages matching the Topic Filter of the subscription to the
// Client." Every SUBSCRIBE, including a repeated one, gets them with RETAIN 1.
func TestPublish_MQTT_3_3_1_9(t *testing.T) {
	addr := p2Start(t)
	pub, _ := p2Connect(t, addr, "pub")
	for _, n := range []string{"rh0/a", "rh0/b"} {
		pub.send(&packet.Publish{QoS: 1, PacketID: 1, Retain: true, Topic: n, Payload: []byte(n)})
		p2Next[*packet.Puback](t, pub.rawClient)
	}
	sub, _ := p2Connect(t, addr, "sub")
	for round := 0; round < 2; round++ {
		p2Sub(t, sub.rawClient, packet.Subscription{Filter: "rh0/#", QoS: 0, RetainHandling: packet.RetainSendAlways})
		got := map[string]bool{}
		for i := 0; i < 2; i++ {
			m := p2Next[*packet.Publish](t, sub.rawClient)
			assert.True(t, m.Retain, "retained messages sent on subscribe have RETAIN 1")
			got[m.Topic] = true
		}
		assert.Equal(t, map[string]bool{"rh0/a": true, "rh0/b": true}, got, "round %d", round)
		sub.expectNothing()
	}
}

// [MQTT-3.3.1-10] The OASIS standard words this two ways. Section 3.3.1.3 reads
// "If Retain Handling is set to 1 then if the subscription did not already
// exist, the Server MUST send all retained message matching the Topic Filter of
// the subscription to the Client, and if the subscription did exist the Server
// MUST NOT send the retained messages", which agrees with section 3.8.3.1
// ("1 = Send retained messages at subscribe only if the subscription does not
// currently exist"). Appendix C words the same statement with new and existing
// swapped, an inconsistency inside the specification; the body text is the one
// every other part of the standard agrees with and is what is asserted.
func TestPublish_MQTT_3_3_1_10(t *testing.T) {
	addr := p2Start(t)
	pub, _ := p2Connect(t, addr, "pub")
	pub.send(&packet.Publish{QoS: 1, PacketID: 1, Retain: true, Topic: "rh1/a", Payload: []byte("r")})
	p2Next[*packet.Puback](t, pub.rawClient)

	sub, _ := p2Connect(t, addr, "sub")
	p2Sub(t, sub.rawClient, packet.Subscription{Filter: "rh1/a", QoS: 0, RetainHandling: packet.RetainSendOnNew})
	m := p2Next[*packet.Publish](t, sub.rawClient) // subscription did not exist: sent
	assert.Equal(t, "r", string(m.Payload))
	assert.True(t, m.Retain)

	p2Sub(t, sub.rawClient, packet.Subscription{Filter: "rh1/a", QoS: 0, RetainHandling: packet.RetainSendOnNew})
	sub.expectNothing() // subscription did exist: not sent

	// A different filter that merely overlaps is a new subscription.
	p2Sub(t, sub.rawClient, packet.Subscription{Filter: "rh1/+", QoS: 0, RetainHandling: packet.RetainSendOnNew})
	p2Next[*packet.Publish](t, sub.rawClient)
}

// [MQTT-3.3.1-12] "If the value of Retain As Published subscription option is
// set to 0, the Server MUST set the RETAIN flag to 0 when forwarding an
// Application Message regardless of how the RETAIN flag was set in the received
// PUBLISH packet." and [MQTT-3.3.1-13] "... set to 1, the Server MUST set the
// RETAIN flag equal to the RETAIN flag in the received PUBLISH packet."
func TestPublish_MQTT_3_3_1_12_and_13(t *testing.T) {
	addr := p2Start(t)
	rap0, _ := p2Connect(t, addr, "rap0")
	rap1, _ := p2Connect(t, addr, "rap1")
	p2Sub(t, rap0.rawClient, packet.Subscription{Filter: "rap/#", QoS: 0, RetainAsPublished: false})
	p2Sub(t, rap1.rawClient, packet.Subscription{Filter: "rap/#", QoS: 0, RetainAsPublished: true})
	pub, _ := p2Connect(t, addr, "pub")
	for _, retain := range []bool{true, false} {
		pub.send(&packet.Publish{Retain: retain, Topic: "rap/x", Payload: []byte("m")})
		assert.False(t, p2Next[*packet.Publish](t, rap0.rawClient).Retain, "RAP 0, published with RETAIN=%v", retain)
		assert.Equal(t, retain, p2Next[*packet.Publish](t, rap1.rawClient).Retain, "RAP 1, published with RETAIN=%v", retain)
	}
}

// [MQTT-3.3.2-4] "A Server MUST send the Payload Format Indicator unaltered to
// all subscribers receiving the message.", [MQTT-3.3.2-15] (Response Topic),
// [MQTT-3.3.2-16] (Correlation Data), [MQTT-3.3.2-20] (Content Type) likewise,
// [MQTT-3.3.2-17] "The Server MUST send all User Properties unaltered in a
// PUBLISH packet when forwarding the Application Message to a Client." and
// [MQTT-3.3.2-18] "The Server MUST maintain the order of User Properties when
// forwarding the Application Message." Two subscribers, at QoS 0 and QoS 1.
func TestPublish_MQTT_3_3_2_4_15_16_17_18_20(t *testing.T) {
	addr := p2Start(t)
	s0, _ := p2Connect(t, addr, "s0")
	s1, _ := p2Connect(t, addr, "s1")
	p2Sub(t, s0.rawClient, packet.Subscription{Filter: "prop/#", QoS: 0})
	p2Sub(t, s1.rawClient, packet.Subscription{Filter: "prop/#", QoS: 1})

	user := []packet.UserProperty{{Key: "b", Value: "2"}, {Key: "a", Value: "1"}, {Key: "b", Value: "0"}, {Key: "a", Value: "1"}}
	corr := []byte{0x00, 0xff, 0x10, 0x00, 0x7f}
	pub, _ := p2Connect(t, addr, "pub")
	pub.send(&packet.Publish{QoS: 1, PacketID: 1, Topic: "prop/x", Payload: []byte("<xml/>"),
		Properties: &packet.Properties{
			PayloadFormat:   packet.Byte(1),
			ContentType:     "application/xml; charset=utf-8",
			ResponseTopic:   "reply/to/me",
			CorrelationData: corr,
			User:            user,
		}})
	p2Next[*packet.Puback](t, pub.rawClient)

	for name, c := range map[string]*p2Client{"QoS 0": s0, "QoS 1": s1} {
		got := p2Next[*packet.Publish](t, c.rawClient)
		if got.QoS == 1 {
			c.send(&packet.Puback{Ack: packet.Ack{PacketID: got.PacketID}})
		}
		require.NotNil(t, got.Properties, name)
		pr := got.Properties
		require.NotNil(t, pr.PayloadFormat, name)
		assert.Equal(t, byte(1), *pr.PayloadFormat, name)
		assert.Equal(t, "application/xml; charset=utf-8", pr.ContentType, name)
		assert.Equal(t, "reply/to/me", pr.ResponseTopic, name)
		assert.Equal(t, corr, pr.CorrelationData, name)
		assert.Equal(t, user, pr.User, "%s: all User Properties, duplicates kept, in order", name)
	}
}

// [MQTT-3.8.3-3] "Bit 2 of the Subscription Options represents the No Local
// option. If the value is 1, Application Messages MUST NOT be forwarded to a
// connection with a ClientID equal to the ClientID of the publishing
// connection." Others still get them; a subscription without the bit gets its
// own messages back.
func TestSubscribe_MQTT_3_8_3_3(t *testing.T) {
	addr := p2Start(t)
	nl, _ := p2Connect(t, addr, "nl")
	plain, _ := p2Connect(t, addr, "plain")
	p2Sub(t, nl.rawClient, packet.Subscription{Filter: "nl/#", QoS: 0, NoLocal: true})
	p2Sub(t, plain.rawClient, packet.Subscription{Filter: "nl/#", QoS: 0})
	other, _ := p2Connect(t, addr, "other")
	p2Sub(t, other.rawClient, packet.Subscription{Filter: "nl/#", QoS: 0, NoLocal: true})

	nl.send(&packet.Publish{Topic: "nl/own", Payload: []byte("own")})
	assert.Equal(t, "own", string(p2Next[*packet.Publish](t, plain.rawClient).Payload))
	assert.Equal(t, "own", string(p2Next[*packet.Publish](t, other.rawClient).Payload))
	nl.expectNothing()

	plain.send(&packet.Publish{Topic: "nl/theirs", Payload: []byte("theirs")})
	assert.Equal(t, "theirs", string(p2Next[*packet.Publish](t, nl.rawClient).Payload), "another client's message is forwarded")
	assert.Equal(t, "theirs", string(p2Next[*packet.Publish](t, plain.rawClient).Payload), "without No Local, one's own message comes back")
}

// [MQTT-3.8.4-8] "The QoS of Payload Messages sent in response to a Subscription
// MUST be the minimum of the QoS of the originally published message and the
// Maximum QoS granted by the Server."
func TestSubscribe_MQTT_3_8_4_8(t *testing.T) {
	addr := p2Start(t)
	pub, _ := p2Connect(t, addr, "pub")
	for _, tc := range []struct{ granted, published, want packet.QoS }{
		{0, 0, 0}, {0, 1, 0}, {0, 2, 0},
		{1, 0, 0}, {1, 1, 1}, {1, 2, 1},
		{2, 0, 0}, {2, 1, 1}, {2, 2, 2},
	} {
		name := "min/g" + strconv.Itoa(int(tc.granted)) + "p" + strconv.Itoa(int(tc.published))
		t.Run(name, func(t *testing.T) {
			sub, _ := p2Connect(t, addr, name)
			require.Equal(t, packet.ReasonCode(tc.granted), p2Sub(t, sub.rawClient, packet.Subscription{Filter: name, QoS: tc.granted}))
			p := &packet.Publish{QoS: tc.published, Topic: name, Payload: []byte("m")}
			switch tc.published {
			case 1:
				p.PacketID = 1
				pub.send(p)
				p2Next[*packet.Puback](t, pub.rawClient)
			case 2:
				p.PacketID = 2
				pub.send(p)
				p2Next[*packet.Pubrec](t, pub.rawClient)
				pub.send(&packet.Pubrel{Ack: packet.Ack{PacketID: 2}})
				p2Next[*packet.Pubcomp](t, pub.rawClient)
			default:
				pub.send(p)
			}
			got := p2Next[*packet.Publish](t, sub.rawClient)
			assert.Equal(t, tc.want, got.QoS)
		})
	}
}
