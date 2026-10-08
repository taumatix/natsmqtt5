package natsmqtt5_test

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// A shared subscription's QoS 1 message that its member never acknowledges.
//
// MQTT-5.0 §4.8.2 (quoted from the specification): "If the Server is in the
// process of sending a QoS 1 message to its chosen subscribing Client and the
// connection to that Client breaks before the Server has received an
// acknowledgement from the Client, the Server MAY wait for the Client to
// reconnect and retransmit the message to that Client. If the Client's Session
// terminates before the Client reconnects, the Server SHOULD send the
// Application Message to another Client that is subscribed to the same Shared
// Subscription." It carries no statement id of its own. The next paragraph is
// [MQTT-4.8.2-6]: "If a Client responds with a PUBACK or PUBREC containing a
// Reason Code of 0x80 or greater to a PUBLISH packet from the Server, the
// Server MUST discard the Application Message and not attempt to send it to
// any other Subscriber."
//
// Every test runs a real nats-server with JetStream, brokers on TCP, and raw
// MQTT clients. JetStream's AckWait is shortened (SetSharedAckWait) so that a
// message wrongly left to time out shows within the test.

const handBackAckWait = 900 * time.Millisecond

// handBackSessionLife is, in seconds, how long the session of a dropped member
// lasts in the test that waits for it to expire.
const handBackSessionLife = 3

const group = "$share/g/jobs/#"

type sharedFleet struct {
	a, b string
}

func newSharedFleet(t *testing.T, customise ...func(*natsmqtt5.Options)) sharedFleet {
	t.Helper()
	natsmqtt5.SetSharedAckWait(t, handBackAckWait)
	natsURL := startNATS(t)
	opts := append([]func(*natsmqtt5.Options){sweeping}, customise...)
	return sharedFleet{a: startBroker(t, natsURL, opts...), b: startBroker(t, natsURL, opts...)}
}

func (f sharedFleet) publish(t *testing.T, payload string) {
	t.Helper()
	pub, _ := connectClient(t, f.a, connectOpts("producer"))
	pub.publish(&paho.Publish{Topic: "jobs/x", QoS: 1, Payload: []byte(payload)})
}

// member connects a raw client that joins the group.
func member(t *testing.T, addr, id string, expiry uint32) *rawClient {
	t.Helper()
	c := dialRaw(t, addr)
	c.connect(rawConnect(id, expiry))
	c.subscribe(group, packet.QoS1)
	return c
}

// expectNoPublishFor asserts nothing is sent for d, which is how a message that
// must not move shows.
func (c *rawClient) expectNoPublishFor(d time.Duration) {
	c.t.Helper()
	require.NoError(c.t, c.nc.SetReadDeadline(time.Now().Add(d)))
	p, err := packet.Read(c.r, 0)
	if err == nil {
		c.t.Fatalf("expected nothing for %v, got %s: %+v", d, p.Type(), p)
	}
	var ne net.Error
	require.ErrorAs(c.t, err, &ne)
	require.True(c.t, ne.Timeout(), "expected a read timeout, got %v", err)
}

// The member's session expires with the message unacknowledged, so another
// member gets it (§4.8.2, the SHOULD), and gets it once.
func TestAnUnacknowledgedSharedMessageGoesToAnotherMemberWhenTheSessionExpires(t *testing.T) {
	f := newSharedFleet(t)
	// The session lasts handBackSessionLife after the drop. The "not early"
	// check below runs once m2 has connected and joined the group, which on a
	// loaded runner took long enough to outlast a one-second session (seen on a
	// macOS CI runner, and reproduced by delaying m2 by 800 ms), so the check
	// saw the hand-back it was meant to rule out. The margin is now seconds.
	m1 := member(t, f.a, "m1", handBackSessionLife)
	f.publish(t, "job-1")
	first := m1.expectPublish()
	require.Equal(t, "job-1", string(first.Payload))
	require.Equal(t, packet.QoS1, first.QoS)
	m1.drop()

	m2 := member(t, f.b, "m2", 300)
	// The session of m1 is still there: it is not handed back early, which
	// would duplicate the message for a client that returns.
	m2.expectNothing()

	got := m2.expectPublish() // within the read deadline, the sweep has run
	assert.Equal(t, "job-1", string(got.Payload))
	assert.Equal(t, packet.QoS1, got.QoS)
	m2.send(&packet.Puback{Ack: packet.Ack{PacketID: got.PacketID}})
	m2.expectNoPublishFor(2 * handBackAckWait)
}

// While the session lives, JetStream does not give the message to another
// member however long the client takes ("the Server MAY wait for the Client to
// reconnect"), and the client that returns gets it again, as a DUP, and is the
// only one that does.
func TestAnUnacknowledgedSharedMessageIsHeldForTheReturningClient(t *testing.T) {
	f := newSharedFleet(t)
	m1 := member(t, f.a, "m1", 300)
	f.publish(t, "job-2")
	first := m1.expectPublish()
	m1.drop()

	m2 := member(t, f.b, "m2", 300)
	m2.expectNoPublishFor(3 * handBackAckWait)

	back := dialRaw(t, f.a)
	require.True(t, back.connect(rawConnect("m1", 300)).SessionPresent)
	again := back.expectPublish()
	assert.True(t, again.Dup)
	assert.Equal(t, first.PacketID, again.PacketID)
	assert.Equal(t, "job-2", string(again.Payload))
	back.send(&packet.Puback{Ack: packet.Ack{PacketID: again.PacketID}})
	m2.expectNoPublishFor(2 * handBackAckWait)
}

// Once the PUBACK is in, the message is the client's: the end of its session
// gives nobody a second copy. A Reason Code of 0x80 or greater ends it too, and
// the message goes to no other subscriber [MQTT-4.8.2-6].
func TestAnAcknowledgedSharedMessageIsNotHandedBack(t *testing.T) {
	for name, code := range map[string]packet.ReasonCode{
		"success":                            packet.Success,
		"refused with 0x80 [MQTT-4.8.2-6]":   packet.UnspecifiedError,
		"refused with 0x83 [MQTT-4.8.2-6]":   packet.ImplementationSpecificError,
		"quota exceeded 0x97 [MQTT-4.8.2-6]": packet.QuotaExceeded,
	} {
		t.Run(name, func(t *testing.T) {
			f := newSharedFleet(t)
			m1 := member(t, f.a, "m1", 1)
			f.publish(t, "job-3")
			p := m1.expectPublish()
			m1.send(&packet.Puback{Ack: packet.Ack{PacketID: p.PacketID, ReasonCode: code}})
			m1.expectNothing()
			m1.send(&packet.Disconnect{})

			m2 := member(t, f.b, "m2", 300)
			// Longer than the session of m1 lasts and than JetStream waits.
			m2.expectNoPublishFor(2*time.Second + handBackAckWait)
		})
	}
}

// A session that ends with the connection (Session Expiry Interval 0) has
// nothing to wait for: the message is handed back at once.
func TestAnUnacknowledgedSharedMessageIsHandedBackWhenASessionWithNoExpiryEnds(t *testing.T) {
	f := newSharedFleet(t)
	m1 := member(t, f.a, "m1", 0)
	f.publish(t, "job-4")
	m1.expectPublish()

	m2 := member(t, f.b, "m2", 300)
	m1.send(&packet.Disconnect{})
	assert.Equal(t, "job-4", string(m2.expectPublish().Payload))
}

// A client that takes the Client Identifier over with Clean Start ends the old
// session, so its message goes to another member.
func TestAnUnacknowledgedSharedMessageIsHandedBackWhenACleanStartEndsTheSession(t *testing.T) {
	f := newSharedFleet(t)
	m1 := member(t, f.a, "m1", 300)
	f.publish(t, "job-5")
	m1.expectPublish()

	m2 := member(t, f.b, "m2", 300)
	m2.expectNoPublishFor(handBackAckWait / 3)

	fresh := dialRaw(t, f.a)
	cp := rawConnect("m1", 300)
	cp.CleanStart = true
	require.False(t, fresh.connect(cp).SessionPresent)
	assert.Equal(t, "job-5", string(m2.expectPublish().Payload))
}

// Persistent sessions: the broker that held the message is gone, and the
// session comes back on another. The record names the message, so the client's
// PUBACK there settles it with the group; had it not, JetStream would give the
// message to m2 when its AckWait ran out.
func TestARestoredSessionSettlesTheSharedMessageItWasHolding(t *testing.T) {
	natsmqtt5.SetSharedAckWait(t, 1500*time.Millisecond)
	natsURL := startNATS(t)
	addrA, stopA := startStoppableBroker(t, natsURL, persistent)
	m1 := member(t, addrA, "m1", 300)
	pub, _ := connectClient(t, addrA, connectOpts("producer"))
	pub.publish(&paho.Publish{Topic: "jobs/x", QoS: 1, Payload: []byte("job-6")})
	first := m1.expectPublish()
	detach(m1)
	stopA()

	addrB := startBroker(t, natsURL, persistent)
	back := dialRaw(t, addrB)
	require.True(t, back.connect(rawConnect("m1", 300)).SessionPresent)
	again := back.expectPublish()
	require.True(t, again.Dup)
	require.Equal(t, first.PacketID, again.PacketID)
	back.send(&packet.Puback{Ack: packet.Ack{PacketID: again.PacketID}})

	m2 := member(t, addrB, "m2", 300)
	m2.expectNoPublishFor(3500 * time.Millisecond)
}

// The restored session is denied the subscription, so the message it was
// holding is not its to send: another member has it.
func TestARestoredSessionThatMayNotResumeTheSubscriptionHandsItsSharedMessageBack(t *testing.T) {
	natsmqtt5.SetSharedAckWait(t, 1500*time.Millisecond)
	natsURL := startNATS(t)
	addrA, stopA := startStoppableBroker(t, natsURL, persistent)
	m1 := member(t, addrA, "m1", 300)
	pub, _ := connectClient(t, addrA, connectOpts("producer"))
	pub.publish(&paho.Publish{Topic: "jobs/x", QoS: 1, Payload: []byte("job-7")})
	m1.expectPublish()
	detach(m1)
	stopA()

	addrB := startBroker(t, natsURL, persistent, func(o *natsmqtt5.Options) {
		o.Authorizer = natsmqtt5.AuthorizerFunc(func(_ context.Context, req *natsmqtt5.AuthzRequest) error {
			if req.Resume && req.ClientID == "m1" && strings.HasPrefix(req.Topic, "$share/") {
				return errors.New("not authorised")
			}
			return nil
		})
	})
	m2 := member(t, addrB, "m2", 300)
	back := dialRaw(t, addrB)
	back.connect(rawConnect("m1", 300))
	back.expectNothing()
	assert.Equal(t, "job-7", string(m2.expectPublish().Payload))
}
