package natsmqtt5_test

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// These tests are about one sentence of the specification:
//
//	"When a Client reconnects with Clean Start set to 0 and a session is
//	present, both the Client and Server MUST resend any unacknowledged PUBLISH
//	packets (where QoS > 0) and PUBREL packets using their original Packet
//	Identifiers. This is the only circumstance where a Client or Server is
//	REQUIRED to resend messages. Clients and Servers MUST NOT resend messages at
//	any other time" [MQTT-4.4.0-1].
//
// Both halves are tested: what must be resent, and — with the same setup and
// only the Clean Start flag changed — what must not be.

// A QoS 1 delivery the client never acknowledged, driven by the Eclipse Paho
// client. This is the independent check: Paho has never seen this code, so the
// DUP flag and the Packet Identifier it reports are read off the wire rather
// than out of the broker's own encoder.
func TestUnacknowledgedQoS1PublishIsResentOnResume(t *testing.T) {
	addr := startBroker(t, startNATS(t))

	// Manual acknowledgement is what makes the message stay unacknowledged:
	// Paho hands it to the handler and sends no PUBACK until Client.Ack is
	// called, which this test never does.
	sub, connack := connectClient(t, addr, durableConnect("q1", 300), manualAck)
	assert.False(t, connack.SessionPresent)
	sub.subscribe(paho.SubscribeOptions{Topic: "resend/q1", QoS: 1})

	pub, _ := connectClient(t, addr, connectOpts("pub"))
	pub.publish(&paho.Publish{Topic: "resend/q1", QoS: 1, Payload: []byte("hello")})

	first := sub.expectMessage()
	require.Equal(t, "hello", first.Payload)
	assert.False(t, first.Dup, "a first attempt must have DUP 0 [MQTT-3.3.1-1]")
	require.NotZero(t, first.PacketID, "a QoS 1 PUBLISH carries a Packet Identifier")

	// The network goes away with the PUBACK still owed.
	sub.dropConnection()

	resumed, connack := connectClient(t, addr, durableConnect("q1", 300), manualAck)
	require.True(t, connack.SessionPresent, "the session must be resumed for §4.4 to apply")

	again := resumed.expectMessage()
	assert.Equal(t, "resend/q1", again.Topic)
	assert.Equal(t, "hello", again.Payload)
	assert.Equal(t, byte(1), again.QoS)
	assert.True(t, again.Dup, "a resent PUBLISH must have DUP 1 [MQTT-3.3.1-1]")
	assert.Equal(t, first.PacketID, again.PacketID,
		"a resend must use the original Packet Identifier [MQTT-4.4.0-1]")
}

// The other half of [MQTT-4.4.0-1]: "Clients and Servers MUST NOT resend
// messages at any other time". This is the test above with one flag changed, so
// the pair discriminates between a broker that resends on resume and one that
// resends on every reconnect.
func TestNothingIsResentAfterACleanStart(t *testing.T) {
	addr := startBroker(t, startNATS(t))

	sub, _ := connectClient(t, addr, durableConnect("q1-clean", 300), manualAck)
	sub.subscribe(paho.SubscribeOptions{Topic: "resend/clean", QoS: 1})

	pub, _ := connectClient(t, addr, connectOpts("pub"))
	pub.publish(&paho.Publish{Topic: "resend/clean", QoS: 1, Payload: []byte("hello")})
	require.Equal(t, "hello", sub.expectMessage().Payload)

	sub.dropConnection()

	cp := durableConnect("q1-clean", 300)
	cp.CleanStart = true
	resumed, connack := connectClient(t, addr, cp, manualAck)
	assert.False(t, connack.SessionPresent, "Clean Start 1 must start a fresh session")
	resumed.expectNoMessage()
}

// Unacknowledged messages must come back in the order they were first sent: a
// Server "MUST send PUBLISH packets to consumers (for the same Topic and QoS)
// in the order that they were received from any given Client" [MQTT-4.6.0-5],
// and by default "MUST treat every Topic as an Ordered Topic when it is
// forwarding messages on Non-shared Subscriptions" [MQTT-4.6.0-6].
//
// This checks the order over the wire; it is a weak guard against the bug of
// resending in map order, and deliberately not the only one. Go's map iteration
// over a set this small comes out in insertion order about half the time, so
// against an implementation with the ordering removed this test failed only
// twice in eight runs. TestUnacknowledgedReturnsTheOrderMessagesWereSentIn is
// the deterministic one.
func TestResendsKeepTheOriginalOrder(t *testing.T) {
	addr := startBroker(t, startNATS(t))
	order := []string{"one", "two", "three", "four", "five"}

	sub, _ := connectClient(t, addr, durableConnect("q1-order", 300), manualAck)
	sub.subscribe(paho.SubscribeOptions{Topic: "resend/order", QoS: 1})

	pub, _ := connectClient(t, addr, connectOpts("pub"))
	for _, payload := range order {
		pub.publish(&paho.Publish{Topic: "resend/order", QoS: 1, Payload: []byte(payload)})
	}

	var ids []uint16
	for _, want := range order {
		got := sub.expectMessage()
		require.Equal(t, want, got.Payload)
		ids = append(ids, got.PacketID)
	}

	sub.dropConnection()

	resumed, connack := connectClient(t, addr, durableConnect("q1-order", 300), manualAck)
	require.True(t, connack.SessionPresent)

	for i, want := range order {
		got := resumed.expectMessage()
		assert.Equal(t, want, got.Payload, "resend %d arrived out of order", i)
		assert.Equal(t, ids[i], got.PacketID, "resend %d changed its Packet Identifier", i)
		assert.True(t, got.Dup)
	}
}

// A resumed session can owe more acknowledgements than the new connection is
// willing to take at once: the send quota "and Receive Maximum value are not
// preserved across Network Connections, and are re-initialized with each new
// Network Connection" (MQTT-5.0 §4.9), so a client may come back with a
// smaller one than it left. The broker has to hold the rest of the resend back
// — "If the send quota reaches zero, the Client or Server MUST NOT send any
// more PUBLISH packets with QoS > 0" [MQTT-4.9.0-2].
//
// This is the only place the resend waits on the client, so it is the only one
// where getting it wrong leaves a client waiting for ever rather than merely
// misbehaving.
func TestResendsRespectASmallerReceiveMaximum(t *testing.T) {
	addr := startBroker(t, startNATS(t))
	order := []string{"one", "two", "three"}

	roomy := durableConnect("q1-quota", 300)
	roomy.Properties.ReceiveMaximum = natsmqtt5.Ptr(uint16(5))
	sub, _ := connectClient(t, addr, roomy, manualAck)
	sub.subscribe(paho.SubscribeOptions{Topic: "resend/quota", QoS: 1})

	pub, _ := connectClient(t, addr, connectOpts("pub"))
	for _, payload := range order {
		pub.publish(&paho.Publish{Topic: "resend/quota", QoS: 1, Payload: []byte(payload)})
	}
	for _, want := range order {
		require.Equal(t, want, sub.expectMessage().Payload)
	}
	sub.dropConnection()

	// Back with room for one message at a time.
	cramped := durableConnect("q1-quota", 300)
	cramped.Properties.ReceiveMaximum = natsmqtt5.Ptr(uint16(1))
	resumed, connack := connectClient(t, addr, cramped, manualAck)
	require.True(t, connack.SessionPresent)

	for i, want := range order {
		got := resumed.expectMessage()
		require.Equal(t, want, got.Payload, "resend %d", i)
		assert.True(t, got.Dup)
		// The next one must wait for this one's PUBACK.
		resumed.expectNoMessage()
		require.NoError(t, got.Ack())
	}
}

// A QoS 2 exchange that got as far as the PUBREL owes a PUBCOMP, and it is the
// PUBREL that must be resent — not the PUBLISH, whose ownership the client took
// when it sent the PUBREC [MQTT-4.3.3-8].
//
// This one is driven by a hand-rolled client rather than Paho, because the
// scenario is defined by an acknowledgement that never arrives: Paho's manual
// mode withholds the PUBREC, and once that is sent it completes the handshake
// on its own. See rawClient for what that costs in independence.
func TestUnacknowledgedPubrelIsResentOnResume(t *testing.T) {
	addr := startBroker(t, startNATS(t))

	raw := dialRaw(t, addr)
	connack := raw.connect(rawConnect("q2", 300))
	require.False(t, connack.SessionPresent)
	raw.subscribe("resend/q2", packet.QoS2)

	pub, _ := connectClient(t, addr, connectOpts("pub"))
	pub.publish(&paho.Publish{Topic: "resend/q2", QoS: 2, Payload: []byte("exactly once")})

	first := raw.expectPublish()
	require.Equal(t, packet.QoS2, first.QoS)
	require.Equal(t, "exactly once", string(first.Payload))
	id := first.PacketID

	// Take ownership of the message, then go quiet: the PUBCOMP is what the
	// broker is left waiting for.
	raw.send(&packet.Pubrec{Ack: packet.Ack{PacketID: id}})
	rel := raw.expectPubrel()
	require.Equal(t, id, rel.PacketID)
	raw.drop()

	resumed := dialRaw(t, addr)
	connack = resumed.connect(rawConnect("q2", 300))
	require.True(t, connack.SessionPresent)

	again := resumed.expectPubrel()
	assert.Equal(t, id, again.PacketID,
		"the PUBREL must be resent with the original Packet Identifier [MQTT-4.4.0-1]")

	// Completing it must end the exchange rather than start another round: the
	// resend happens once per resumption, not on a timer [MQTT-4.4.0-1].
	resumed.send(&packet.Pubcomp{Ack: packet.Ack{PacketID: id}})
	resumed.expectNothing()
}

// A QoS 2 delivery the client never took ownership of — no PUBREC — is still
// the broker's message, so it is the PUBLISH that comes back, with DUP set.
func TestUnacknowledgedQoS2PublishIsResentOnResume(t *testing.T) {
	addr := startBroker(t, startNATS(t))

	raw := dialRaw(t, addr)
	raw.connect(rawConnect("q2-pub", 300))
	raw.subscribe("resend/q2pub", packet.QoS2)

	pub, _ := connectClient(t, addr, connectOpts("pub"))
	pub.publish(&paho.Publish{Topic: "resend/q2pub", QoS: 2, Payload: []byte("unowned")})

	first := raw.expectPublish()
	require.False(t, first.Dup)
	raw.drop()

	resumed := dialRaw(t, addr)
	connack := resumed.connect(rawConnect("q2-pub", 300))
	require.True(t, connack.SessionPresent)

	again := resumed.expectPublish()
	assert.Equal(t, first.PacketID, again.PacketID)
	assert.Equal(t, packet.QoS2, again.QoS)
	assert.Equal(t, "unowned", string(again.Payload))
	assert.True(t, again.Dup, "a resent PUBLISH must have DUP 1 [MQTT-3.3.1-1]")
}

// Arrears go out before anything published since the reconnect: a Server "MUST
// send PUBLISH packets to consumers (for the same Topic and QoS) in the order
// that they were received from any given Client" [MQTT-4.6.0-5], and the
// messages left over from the last connection were received first.
//
// Receive Maximum 1 is what makes this discriminating rather than a race the
// resend usually wins: the resend is still waiting for the first message's
// PUBACK when the new message is published, so a broker that ran the resend
// beside the delivery loop instead of ahead of it would interleave here.
func TestArrearsComeBeforeMessagesPublishedSince(t *testing.T) {
	addr := startBroker(t, startNATS(t))

	roomy := durableConnect("q1-arrears", 300)
	roomy.Properties.ReceiveMaximum = natsmqtt5.Ptr(uint16(5))
	sub, _ := connectClient(t, addr, roomy, manualAck)
	sub.subscribe(paho.SubscribeOptions{Topic: "resend/arrears", QoS: 1})

	pub, _ := connectClient(t, addr, connectOpts("pub"))
	for _, payload := range []string{"one", "two"} {
		pub.publish(&paho.Publish{Topic: "resend/arrears", QoS: 1, Payload: []byte(payload)})
		require.Equal(t, payload, sub.expectMessage().Payload)
	}
	sub.dropConnection()

	cramped := durableConnect("q1-arrears", 300)
	cramped.Properties.ReceiveMaximum = natsmqtt5.Ptr(uint16(1))
	resumed, connack := connectClient(t, addr, cramped, manualAck)
	require.True(t, connack.SessionPresent)

	// Published while the resend is blocked on the first message's PUBACK.
	pub.publish(&paho.Publish{Topic: "resend/arrears", QoS: 1, Payload: []byte("after")})

	for _, want := range []string{"one", "two", "after"} {
		got := resumed.expectMessage()
		assert.Equal(t, want, got.Payload)
		require.NoError(t, got.Ack())
	}
}

// A takeover rather than a reconnect after a drop: the first connection is
// still open when the second CONNECT claims the Client Identifier. The session
// moves, arrears and all — and the packet the session is holding must come out
// of it unmodified, since the displaced connection's delivery loop can still be
// running when the new one starts resending.
func TestArrearsFollowASessionTakenOverByALiveConnection(t *testing.T) {
	addr := startBroker(t, startNATS(t))

	first, _ := connectClient(t, addr, durableConnect("q1-takeover", 300), manualAck)
	first.subscribe(paho.SubscribeOptions{Topic: "resend/takeover", QoS: 1})

	pub, _ := connectClient(t, addr, connectOpts("pub"))
	pub.publish(&paho.Publish{Topic: "resend/takeover", QoS: 1, Payload: []byte("moved")})
	original := first.expectMessage()
	require.Equal(t, "moved", original.Payload)
	require.False(t, original.Dup)

	// No drop: the second CONNECT displaces a connection that is still up.
	second, connack := connectClient(t, addr, durableConnect("q1-takeover", 300), manualAck)
	require.True(t, connack.SessionPresent)
	assert.Equal(t, byte(packet.SessionTakenOver), first.expectServerDisconnect(),
		"the displaced connection must be told 0x8E [MQTT-3.1.4-3]")

	got := second.expectMessage()
	assert.Equal(t, "moved", got.Payload)
	assert.Equal(t, original.PacketID, got.PacketID)
	assert.True(t, got.Dup)
}

// "If PUBACK or PUBREC is received containing a Reason Code of 0x80 or greater
// the corresponding PUBLISH packet is treated as acknowledged, and MUST NOT be
// retransmitted" [MQTT-4.4.0-2]. A client that refused a message must not be
// handed it again on every reconnect for the life of the session.
func TestARefusedMessageIsNotResent(t *testing.T) {
	for _, tc := range []struct {
		name  string
		qos   packet.QoS
		topic string
		// refuse sends the error acknowledgement the client answers with.
		refuse func(c *rawClient, id uint16)
	}{
		{
			name: "puback", qos: packet.QoS1, topic: "refused/q1",
			refuse: func(c *rawClient, id uint16) {
				c.send(&packet.Puback{Ack: packet.Ack{PacketID: id, ReasonCode: packet.UnspecifiedError}})
			},
		},
		{
			name: "pubrec", qos: packet.QoS2, topic: "refused/q2",
			refuse: func(c *rawClient, id uint16) {
				c.send(&packet.Pubrec{Ack: packet.Ack{PacketID: id, ReasonCode: packet.UnspecifiedError}})
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr := startBroker(t, startNATS(t))

			raw := dialRaw(t, addr)
			raw.connect(rawConnect("refuse-"+tc.name, 300))
			raw.subscribe(tc.topic, tc.qos)

			pub, _ := connectClient(t, addr, connectOpts("pub"))
			pub.publish(&paho.Publish{Topic: tc.topic, QoS: byte(tc.qos), Payload: []byte("unwanted")})

			first := raw.expectPublish()
			tc.refuse(raw, first.PacketID)
			raw.drop()

			resumed := dialRaw(t, addr)
			connack := resumed.connect(rawConnect("refuse-"+tc.name, 300))
			require.True(t, connack.SessionPresent)
			resumed.expectNothing()
		})
	}
}

// A resend the client's Maximum Packet Size forbids must end the exchange, not
// stall it. "Where a Packet is too large to send, the Server MUST discard it
// without sending it and then behave as if it had completed sending that
// Application Message" [MQTT-3.1.2-25] — so the send-quota slot and the Packet
// Identifier go back, and the connection can still deliver.
//
// Getting this wrong is silent and total: the client receives nothing more, for
// the life of the connection and every connection after it, with no error
// anywhere the client can see.
func TestAResendTooLargeForTheClientDoesNotStallTheConnection(t *testing.T) {
	addr := startBroker(t, startNATS(t))

	raw := dialRaw(t, addr)
	raw.connect(rawConnect("too-large", 300))
	raw.subscribe("resend/large", packet.QoS1)

	pub, _ := connectClient(t, addr, connectOpts("pub"))
	pub.publish(&paho.Publish{
		Topic: "resend/large", QoS: 1, Payload: make([]byte, 1000),
	})
	require.Len(t, raw.expectPublish().Payload, 1000)
	raw.drop()

	// Back with room for one message at a time and a packet limit the arrear
	// cannot fit in.
	cp := rawConnect("too-large", 300)
	cp.Properties.MaximumPacketSize = packet.Uint32(200)
	cp.Properties.ReceiveMaximum = packet.Uint16(1)
	resumed := dialRaw(t, addr)
	connack := resumed.connect(cp)
	require.True(t, connack.SessionPresent)
	resumed.expectNothing()

	// The quota slot the discarded resend would have stranded has to be back,
	// or nothing at this QoS ever reaches this client again.
	pub.publish(&paho.Publish{Topic: "resend/large", QoS: 1, Payload: []byte("small")})
	assert.Equal(t, "small", string(resumed.expectPublish().Payload))
}

// The send quota belongs to the network connection, the in-flight set belongs
// to the session, and a resumed connection holds both. An acknowledgement for a
// message the *previous* connection sent must not buy room on this one:
// Receive Maximum is what a constrained client uses to say how many QoS 1 and 2
// publications it can hold at once (MQTT-5.0 §3.1.2.11.3), and a client that
// says 1 must never be handed 2.
//
// The set is mixed on purpose. The QoS 2 entry is past its PUBREC, so its
// resend is a PUBREL and spends no quota — and the PUBCOMP answering it is
// therefore an acknowledgement with no slot behind it, which is the one most
// likely to be miscounted.
func TestAnOldConnectionsAcknowledgementsDoNotBuyRoomOnTheNewOne(t *testing.T) {
	addr := startBroker(t, startNATS(t))

	roomy := rawConnect("q-mixed", 300)
	roomy.Properties.ReceiveMaximum = packet.Uint16(5)
	raw := dialRaw(t, addr)
	raw.connect(roomy)
	raw.subscribe("resend/mixed/two", packet.QoS2)
	raw.subscribe("resend/mixed/one", packet.QoS1)

	pub, _ := connectClient(t, addr, connectOpts("pub"))
	pub.publish(&paho.Publish{Topic: "resend/mixed/two", QoS: 2, Payload: []byte("owned")})
	owned := raw.expectPublish()
	require.Equal(t, packet.QoS2, owned.QoS)
	// Take ownership but never complete it: the resend will be a PUBREL.
	raw.send(&packet.Pubrec{Ack: packet.Ack{PacketID: owned.PacketID}})
	require.Equal(t, owned.PacketID, raw.expectPubrel().PacketID)

	pub.publish(&paho.Publish{Topic: "resend/mixed/one", QoS: 1, Payload: []byte("unacked")})
	unacked := raw.expectPublish()
	require.Equal(t, packet.QoS1, unacked.QoS)
	raw.drop()

	// Back with room for exactly one QoS > 0 publication.
	cramped := rawConnect("q-mixed", 300)
	cramped.Properties.ReceiveMaximum = packet.Uint16(1)
	resumed := dialRaw(t, addr)
	require.True(t, resumed.connect(cramped).SessionPresent)

	// PUBREL first — it was sent first, and it spends no quota.
	require.Equal(t, owned.PacketID, resumed.expectPubrel().PacketID)
	again := resumed.expectPublish()
	require.Equal(t, unacked.PacketID, again.PacketID)
	require.True(t, again.Dup)

	// Completing the QoS 2 exchange releases nothing: the PUBLISH still
	// outstanding is the one holding this connection's single slot.
	resumed.send(&packet.Pubcomp{Ack: packet.Ack{PacketID: owned.PacketID}})
	pub.publish(&paho.Publish{Topic: "resend/mixed/one", QoS: 1, Payload: []byte("third")})
	resumed.expectNothing()

	// Acknowledging the one that does hold the slot lets it through.
	resumed.send(&packet.Puback{Ack: packet.Ack{PacketID: again.PacketID}})
	assert.Equal(t, "third", string(resumed.expectPublish().Payload))
}

// rawClient is a minimal MQTT client built on this module's own codec.
//
// Everywhere else the suite drives the broker with Eclipse Paho, on purpose: a
// test that encodes with the same code the broker decodes with proves
// self-consistency, not conformance. This one cannot. The behaviour under test
// is what happens when a specific acknowledgement is *withheld* mid-handshake,
// and no conforming client will do that on request — Paho's manual mode holds
// back the PUBREC, and having sent one it goes on to send the PUBCOMP by
// itself. So the QoS 2 tests above are worth less as evidence than the Paho
// ones: they prove the broker resends what it should, over a real socket, but
// with a peer that shares its encoder.
type rawClient struct {
	t  *testing.T
	nc net.Conn
	r  *bufio.Reader
	// subID numbers this client's SUBSCRIBE packets. Reusing one while it is
	// outstanding would be a protocol error, and a client that subscribes twice
	// is the point of some of the tests above.
	subID uint16
}

func dialRaw(t *testing.T, addr string) *rawClient {
	t.Helper()
	nc, err := net.DialTimeout("tcp", addr, 5*time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = nc.Close() })
	return &rawClient{t: t, nc: nc, r: bufio.NewReader(nc)}
}

func (c *rawClient) send(p packet.Packet) {
	c.t.Helper()
	require.NoError(c.t, c.nc.SetWriteDeadline(time.Now().Add(5*time.Second)))
	require.NoError(c.t, packet.Write(c.nc, p))
}

// read returns the next packet, failing the test if none arrives.
func (c *rawClient) read() packet.Packet {
	c.t.Helper()
	require.NoError(c.t, c.nc.SetReadDeadline(time.Now().Add(5*time.Second)))
	p, err := packet.Read(c.r, 0)
	require.NoError(c.t, err)
	return p
}

// rawConnect is a CONNECT that keeps its session for expiry seconds. Callers
// add to Properties for the connection limits they want to exercise.
func rawConnect(clientID string, expiry uint32) *packet.Connect {
	return &packet.Connect{
		ClientID:   clientID,
		KeepAlive:  30,
		Properties: &packet.Properties{SessionExpiryInterval: packet.Uint32(expiry)},
	}
}

func (c *rawClient) connect(cp *packet.Connect) *packet.Connack {
	c.t.Helper()
	c.send(cp)
	ack, ok := c.read().(*packet.Connack)
	require.True(c.t, ok, "the first packet from the broker must be a CONNACK")
	require.False(c.t, ack.ReasonCode.IsError(),
		"CONNACK refused the connection with reason 0x%02X", byte(ack.ReasonCode))
	return ack
}

func (c *rawClient) subscribe(filter string, qos packet.QoS) {
	c.t.Helper()
	c.subID++
	c.send(&packet.Subscribe{
		PacketID:      c.subID,
		Subscriptions: []packet.Subscription{{Filter: filter, QoS: qos}},
	})
	ack, ok := c.read().(*packet.Suback)
	require.True(c.t, ok, "a SUBSCRIBE must be answered with a SUBACK")
	require.Equal(c.t, []packet.ReasonCode{packet.ReasonCode(qos)}, ack.ReasonCodes)
}

func (c *rawClient) expectPublish() *packet.Publish {
	c.t.Helper()
	p, ok := c.read().(*packet.Publish)
	require.True(c.t, ok, "expected a PUBLISH")
	return p
}

func (c *rawClient) expectPubrel() *packet.Pubrel {
	c.t.Helper()
	p, ok := c.read().(*packet.Pubrel)
	require.True(c.t, ok, "expected a PUBREL")
	return p
}

// expectNothing asserts the broker sends nothing more within a settling window.
// The window is a compromise, the same one expectNoMessage makes: long enough
// that a resend would have landed, short enough not to dominate the suite.
func (c *rawClient) expectNothing() {
	c.t.Helper()
	require.NoError(c.t, c.nc.SetReadDeadline(time.Now().Add(300*time.Millisecond)))
	p, err := packet.Read(c.r, 0)
	if err == nil {
		c.t.Fatalf("expected no further packet, got %s", p.Type())
	}
	var ne net.Error
	require.ErrorAs(c.t, err, &ne)
	require.True(c.t, ne.Timeout(), "expected a read timeout, got %v", err)
}

// drop closes the socket without a DISCONNECT, which is what a lost network
// looks like to the broker.
func (c *rawClient) drop() {
	c.t.Helper()
	require.NoError(c.t, c.nc.Close())
}
