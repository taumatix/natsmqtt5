package natsmqtt5_test

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// A session restored from the JetStream record resends what the previous broker
// left unacknowledged, with the original Packet Identifiers. The payloads are
// not in the record: they come back from the offline queue stream by sequence.
//
// "When a Client reconnects with Clean Start set to 0 and a session is present,
// both the Client and Server MUST resend any unacknowledged PUBLISH packets
// (where QoS > 0) and PUBREL packets using their original Packet Identifiers"
// [MQTT-4.4.0-1]; the resent PUBLISH has DUP 1 [MQTT-3.3.1-1]. Broker A is
// stopped before the client reconnects, so everything B knows is from the record.

func TestMQTT4_4_0_1_QoS1UnacknowledgedAtReleaseIsResentByAnotherBroker(t *testing.T) {
	natsURL := startNATS(t)
	addrA, stopA := startStoppableBroker(t, natsURL, persistent)
	addrB := startBroker(t, natsURL, persistent)

	c := dialRaw(t, addrA)
	c.connect(rawConnect("restore-q1", 300))
	c.subscribe("rs/q1/#", packet.QoS1)

	pub, _ := connectClient(t, addrA, connectOpts("pub-restore-q1"))
	var first []*packet.Publish
	for _, n := range []string{"a", "b", "c"} {
		pub.publish(&paho.Publish{Topic: "rs/q1/" + n, QoS: 1, Payload: []byte(n)})
		got := c.expectPublish()
		require.Equal(t, n, string(got.Payload))
		require.False(t, got.Dup)
		first = append(first, got)
	}
	// "b" is acknowledged and so is not owed; "a" and "c" are.
	c.send(&packet.Puback{Ack: packet.Ack{PacketID: first[1].PacketID}})
	time.Sleep(100 * time.Millisecond)
	c.drop()
	time.Sleep(100 * time.Millisecond)
	stopA()

	back := dialRaw(t, addrB)
	require.True(t, back.connect(rawConnect("restore-q1", 300)).SessionPresent)
	for _, i := range []int{0, 2} {
		got := back.expectPublish()
		assert.Equal(t, first[i].PacketID, got.PacketID,
			"the original Packet Identifier must be used [MQTT-4.4.0-1]")
		assert.Equal(t, string(first[i].Payload), string(got.Payload))
		assert.Equal(t, first[i].Topic, got.Topic)
		assert.Equal(t, packet.QoS1, got.QoS)
		assert.True(t, got.Dup, "a resent PUBLISH must have DUP 1 [MQTT-3.3.1-1]")
		back.send(&packet.Puback{Ack: packet.Ack{PacketID: got.PacketID}})
	}
	// Once per resumption, not on a timer, and the acknowledged one is not resent.
	back.expectNothing()
}

// A restart of the same broker is the same case on one process: the record is
// written when the connection ends, which Close waits for.
func TestMQTT4_4_0_1_QoS1UnacknowledgedIsResentAfterTheBrokerRestarts(t *testing.T) {
	natsURL := startNATS(t)
	addrA, stopA := startStoppableBroker(t, natsURL, persistent)

	c := dialRaw(t, addrA)
	c.connect(rawConnect("restore-restart", 300))
	c.subscribe("rs/restart", packet.QoS1)
	pub, _ := connectClient(t, addrA, connectOpts("pub-restore-restart"))
	pub.publish(&paho.Publish{Topic: "rs/restart", QoS: 1, Payload: []byte("owed")})
	first := c.expectPublish()
	// The broker goes down with the client still connected and the message
	// unacknowledged.
	stopA()

	addrB := startBroker(t, natsURL, persistent)
	back := dialRaw(t, addrB)
	require.True(t, back.connect(rawConnect("restore-restart", 300)).SessionPresent)
	got := back.expectPublish()
	assert.Equal(t, first.PacketID, got.PacketID)
	assert.Equal(t, "owed", string(got.Payload))
	assert.True(t, got.Dup)
}

// A QoS 2 exchange the client took ownership of (it sent PUBREC) has nothing
// left to resend but the PUBREL [MQTT-4.3.3-8]; that is what a restored session
// resends, with no payload to fetch.
func TestMQTT4_4_0_1_QoS2PubrelIsResentByAnotherBroker(t *testing.T) {
	natsURL := startNATS(t)
	addrA, stopA := startStoppableBroker(t, natsURL, persistent)
	addrB := startBroker(t, natsURL, persistent)

	c := dialRaw(t, addrA)
	c.connect(rawConnect("restore-q2rel", 300))
	c.subscribe("rs/q2rel", packet.QoS2)
	pub, _ := connectClient(t, addrA, connectOpts("pub-restore-q2rel"))
	pub.publish(&paho.Publish{Topic: "rs/q2rel", QoS: 2, Payload: []byte("exactly once")})
	first := c.expectPublish()
	require.Equal(t, packet.QoS2, first.QoS)
	c.send(&packet.Pubrec{Ack: packet.Ack{PacketID: first.PacketID}})
	require.Equal(t, first.PacketID, c.expectPubrel().PacketID)
	c.drop()
	time.Sleep(100 * time.Millisecond)
	stopA()

	back := dialRaw(t, addrB)
	require.True(t, back.connect(rawConnect("restore-q2rel", 300)).SessionPresent)
	rel := back.expectPubrel()
	assert.Equal(t, first.PacketID, rel.PacketID,
		"the PUBREL must be resent with the original Packet Identifier [MQTT-4.4.0-1]")
	back.send(&packet.Pubcomp{Ack: packet.Ack{PacketID: first.PacketID}})
	back.expectNothing()
}

// A QoS 2 PUBLISH the client never took ownership of is still the broker's
// message: it is the PUBLISH that comes back, DUP 1, from the queue.
func TestMQTT4_4_0_1_QoS2PublishIsResentByAnotherBroker(t *testing.T) {
	natsURL := startNATS(t)
	addrA, stopA := startStoppableBroker(t, natsURL, persistent)
	addrB := startBroker(t, natsURL, persistent)

	c := dialRaw(t, addrA)
	c.connect(rawConnect("restore-q2pub", 300))
	c.subscribe("rs/q2pub", packet.QoS2)
	pub, _ := connectClient(t, addrA, connectOpts("pub-restore-q2pub"))
	pub.publish(&paho.Publish{Topic: "rs/q2pub", QoS: 2, Payload: []byte("unowned")})
	first := c.expectPublish()
	c.drop()
	time.Sleep(100 * time.Millisecond)
	stopA()

	back := dialRaw(t, addrB)
	require.True(t, back.connect(rawConnect("restore-q2pub", 300)).SessionPresent)
	got := back.expectPublish()
	assert.Equal(t, first.PacketID, got.PacketID)
	assert.Equal(t, packet.QoS2, got.QoS)
	assert.Equal(t, "unowned", string(got.Payload))
	assert.True(t, got.Dup)
	back.send(&packet.Pubrec{Ack: packet.Ack{PacketID: got.PacketID}})
	assert.Equal(t, got.PacketID, back.expectPubrel().PacketID)
}

// "The Server MUST NOT [deliver] a QoS 2 message twice": a QoS 2 PUBLISH whose
// Packet Identifier has been received and not released is acknowledged and not
// forwarded again when the client resends it, even to a broker that never saw
// the first [MQTT-4.3.3-10].
func TestMQTT4_3_3_10_ResentInboundQoS2IsForwardedOnceAcrossBrokers(t *testing.T) {
	natsURL := startNATS(t)
	addrA, stopA := startStoppableBroker(t, natsURL, persistent)
	addrB := startBroker(t, natsURL, persistent)

	sub, _ := connectClient(t, addrB, connectOpts("sub-inbound-q2"))
	sub.subscribe(paho.SubscribeOptions{Topic: "rs/in", QoS: 2})

	c := dialRaw(t, addrA)
	c.connect(rawConnect("restore-inbound", 300))
	c.send(&packet.Publish{Topic: "rs/in", QoS: packet.QoS2, PacketID: 41, Payload: []byte("once")})
	rec, ok := c.read().(*packet.Pubrec)
	require.True(t, ok, "a QoS 2 PUBLISH is answered with a PUBREC")
	require.Equal(t, uint16(41), rec.PacketID)
	require.Equal(t, "once", string(sub.expectMessage().Payload))
	// The PUBREC never reaches the client in this story: it resends.
	c.drop()
	time.Sleep(100 * time.Millisecond)
	stopA()

	back := dialRaw(t, addrB)
	require.True(t, back.connect(rawConnect("restore-inbound", 300)).SessionPresent)
	back.send(&packet.Publish{Topic: "rs/in", QoS: packet.QoS2, PacketID: 41, Dup: true, Payload: []byte("once")})
	again, ok := back.read().(*packet.Pubrec)
	require.True(t, ok, "the resent PUBLISH is acknowledged with a PUBREC")
	assert.Equal(t, uint16(41), again.PacketID)
	sub.expectNoMessage()

	// Releasing it completes the exchange, and the identifier is free again.
	back.send(&packet.Pubrel{Ack: packet.Ack{PacketID: 41}})
	comp, ok := back.read().(*packet.Pubcomp)
	require.True(t, ok, "a PUBREL for a restored identifier is answered with a PUBCOMP")
	assert.Equal(t, packet.Success, comp.ReasonCode)
}

// The same on one process: stop the broker holding the received identifier,
// start another, resend.
func TestMQTT4_3_3_10_ResentInboundQoS2IsForwardedOnceAfterARestart(t *testing.T) {
	natsURL := startNATS(t)
	sub, _ := connectClient(t, startBroker(t, natsURL), connectOpts("sub-inbound-restart"))
	sub.subscribe(paho.SubscribeOptions{Topic: "rs/inr", QoS: 2})

	addrA, stopA := startStoppableBroker(t, natsURL, persistent)
	c := dialRaw(t, addrA)
	c.connect(rawConnect("restore-inbound-restart", 300))
	c.send(&packet.Publish{Topic: "rs/inr", QoS: packet.QoS2, PacketID: 9, Payload: []byte("once")})
	_, ok := c.read().(*packet.Pubrec)
	require.True(t, ok)
	require.Equal(t, "once", string(sub.expectMessage().Payload))
	// Stopped with the client connected and the identifier unreleased.
	stopA()

	addrB := startBroker(t, natsURL, persistent)
	back := dialRaw(t, addrB)
	require.True(t, back.connect(rawConnect("restore-inbound-restart", 300)).SessionPresent)
	back.send(&packet.Publish{Topic: "rs/inr", QoS: packet.QoS2, PacketID: 9, Dup: true, Payload: []byte("once")})
	_, ok = back.read().(*packet.Pubrec)
	require.True(t, ok)
	sub.expectNoMessage()
}

// A resend whose message has waited out its interval is dropped, not sent
// [MQTT-3.3.2-5]; the restored entry is completed like any expired resend.
func TestMQTT3_3_2_5_ExpiredRestoredQoS1ResendIsDropped(t *testing.T) {
	natsURL := startNATS(t)
	addrA, stopA := startStoppableBroker(t, natsURL, persistent)
	addrB := startBroker(t, natsURL, persistent)

	c := dialRaw(t, addrA)
	c.connect(rawConnect("restore-expiry", 300))
	c.subscribe("rs/exp/#", packet.QoS1)
	pub, _ := connectClient(t, addrA, connectOpts("pub-restore-expiry"))
	for _, n := range []string{"short", "long"} {
		interval := uint32(1)
		if n == "long" {
			interval = 3600
		}
		_, err := pub.Client.Publish(t.Context(), &paho.Publish{
			Topic: "rs/exp/" + n, QoS: 1, Payload: []byte(n),
			Properties: &paho.PublishProperties{MessageExpiry: natsmqtt5.Ptr(interval)},
		})
		require.NoError(t, err)
		require.Equal(t, n, string(c.expectPublish().Payload))
	}
	c.drop()
	time.Sleep(100 * time.Millisecond)
	stopA()
	time.Sleep(2200 * time.Millisecond)

	back := dialRaw(t, addrB)
	require.True(t, back.connect(rawConnect("restore-expiry", 300)).SessionPresent)
	got := back.expectPublish()
	assert.Equal(t, "long", string(got.Payload), "the expired message must not be resent")
	require.NotNil(t, got.Properties)
	require.NotNil(t, got.Properties.MessageExpiryInterval)
	assert.Less(t, *got.Properties.MessageExpiryInterval, uint32(3600),
		"the interval is reduced by the time the message waited [MQTT-3.3.2-6]")
	assert.Greater(t, *got.Properties.MessageExpiryInterval, uint32(3590))
	back.expectNothing()
}

// A filter denied on resume stops its in-flight messages from being resent, as
// it does for a session resumed from memory: a message this connection may not
// receive is not sent, whatever [MQTT-4.4.0-1] says of resending.
func TestARestoredResendIsWithdrawnWhenTheFilterIsDeniedOnResume(t *testing.T) {
	natsURL := startNATS(t)
	addrA, stopA := startStoppableBroker(t, natsURL, persistent)
	var deny atomic.Bool
	addrB := startBroker(t, natsURL, persistent, func(o *natsmqtt5.Options) {
		o.Authorizer = natsmqtt5.AuthorizerFunc(func(_ context.Context, r *natsmqtt5.AuthzRequest) error {
			if deny.Load() && r.Action == natsmqtt5.ActionSubscribe && r.Topic == "rs/deny/#" {
				return fmt.Errorf("revoked")
			}
			return nil
		})
	})

	c := dialRaw(t, addrA)
	c.connect(rawConnect("restore-deny", 300))
	c.subscribe("rs/deny/#", packet.QoS1)
	c.subscribe("rs/keep/#", packet.QoS1)
	pub, _ := connectClient(t, addrA, connectOpts("pub-restore-deny"))
	pub.publish(&paho.Publish{Topic: "rs/deny/x", QoS: 1, Payload: []byte("denied")})
	pub.publish(&paho.Publish{Topic: "rs/keep/x", QoS: 1, Payload: []byte("kept")})
	c.expectPublish()
	c.expectPublish()
	c.drop()
	time.Sleep(100 * time.Millisecond)
	stopA()

	deny.Store(true)
	back := dialRaw(t, addrB)
	require.True(t, back.connect(rawConnect("restore-deny", 300)).SessionPresent)
	got := back.expectPublish()
	assert.Equal(t, "kept", string(got.Payload))
	back.expectNothing()
}

// With the offline queue off there is no copy of a payload to resend from, so a
// restored session has nothing in flight: the boundary the record states
// honestly rather than recording identifiers it cannot honour.
func TestWithoutTheOfflineQueueARestoredSessionCannotResend(t *testing.T) {
	natsURL := startNATS(t)
	noQueue := func(o *natsmqtt5.Options) { o.PersistentSessions, o.DisableOfflineQueue = true, true }
	addrA, stopA := startStoppableBroker(t, natsURL, noQueue)
	addrB := startBroker(t, natsURL, noQueue)

	c := dialRaw(t, addrA)
	c.connect(rawConnect("restore-noqueue", 300))
	c.subscribe("rs/nq", packet.QoS1)
	pub, _ := connectClient(t, addrA, connectOpts("pub-restore-noqueue"))
	pub.publish(&paho.Publish{Topic: "rs/nq", QoS: 1, Payload: []byte("lost")})
	c.expectPublish()
	c.drop()
	time.Sleep(100 * time.Millisecond)
	stopA()

	back := dialRaw(t, addrB)
	require.True(t, back.connect(rawConnect("restore-noqueue", 300)).SessionPresent)
	back.expectNothing()
}

// A record that would not fit a key-value value is cut to fit, oldest first,
// rather than failing the release and losing the whole session: the oldest
// in-flight messages are the ones that must go out first [MQTT-4.6.0-5].
func TestARecordTooLargeForAValueKeepsTheOldestInflightMessages(t *testing.T) {
	// 4 KiB holds the subscription and some tens of entries, not a hundred.
	natsURL := startNATSWithMaxPayload(t, 4096)
	addrA, stopA := startStoppableBroker(t, natsURL, persistent)
	addrB := startBroker(t, natsURL, persistent)

	const sent = 200
	c := dialRaw(t, addrA)
	cp := rawConnect("restore-overflow", 300)
	cp.Properties.ReceiveMaximum = natsmqtt5.Ptr(uint16(sent))
	c.connect(cp)
	c.subscribe("rs/big", packet.QoS1)
	pub, _ := connectClient(t, addrA, connectOpts("pub-restore-overflow"))
	ids := make([]uint16, 0, sent)
	for i := 0; i < sent; i++ {
		pub.publish(&paho.Publish{Topic: "rs/big", QoS: 1, Payload: []byte(fmt.Sprintf("m%03d", i))})
		ids = append(ids, c.expectPublish().PacketID)
	}
	c.drop()
	time.Sleep(200 * time.Millisecond)
	stopA()

	back := dialRaw(t, addrB)
	cp = rawConnect("restore-overflow", 300)
	cp.Properties.ReceiveMaximum = natsmqtt5.Ptr(uint16(sent))
	require.True(t, back.connect(cp).SessionPresent,
		"an oversized record must not cost the client its session")
	n := 0
	for {
		pk, ok := tryReadPublish(back)
		if !ok {
			break
		}
		assert.Equal(t, fmt.Sprintf("m%03d", n), string(pk.Payload), "oldest first, none skipped")
		assert.Equal(t, ids[n], pk.PacketID)
		assert.True(t, pk.Dup)
		n++
	}
	assert.Greater(t, n, 5, "some in-flight messages must survive")
	assert.Less(t, n, sent, "not all of them can fit this value limit")
}

// tryReadPublish returns the next PUBLISH, or false once nothing more arrives
// within a short window.
func tryReadPublish(c *rawClient) (*packet.Publish, bool) {
	c.t.Helper()
	require.NoError(c.t, c.nc.SetReadDeadline(time.Now().Add(500*time.Millisecond)))
	p, err := packet.Read(c.r, 0)
	if err != nil {
		var ne net.Error
		require.ErrorAs(c.t, err, &ne)
		require.True(c.t, ne.Timeout(), "expected a read timeout, got %v", err)
		return nil, false
	}
	pk, ok := p.(*packet.Publish)
	require.True(c.t, ok, "expected a PUBLISH, got %s", p.Type())
	c.send(&packet.Puback{Ack: packet.Ack{PacketID: pk.PacketID}})
	return pk, true
}
