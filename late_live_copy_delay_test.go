package natsmqtt5_test

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// A message's queue copy is stored before its live copy is published, and the
// live copy of an earlier-stored message can reach a subscriber's broker after
// the live copy of a later-stored one, and after the subscriber's connection
// ended. A fixed rewind (two seconds) finds it only if it is stored within
// that window of the connection's end. These tests delay it past the window.
//
//	[MQTT-4.4.0-1] "When a Client reconnects with Clean Start set to 0 and a
//	  session is present, both the Client and Server MUST resend any
//	  unacknowledged PUBLISH packets" — the session was owed the earlier
//	  message, however long its live copy was delayed.
//	[MQTT-3.1.2-23] "The Client and Server MUST store the Session State ...
//	  until the Session Expiry Interval has passed" — what a session was owed
//	  is session state.
//	[MQTT-4.6.0-5]'s ordering for one publisher does not apply here (two
//	  publishers), so the test asserts the order the queue stored them in,
//	  which is what the replay gives.

// lateCopyFixture is a raw subscriber that holds the only slot of its Receive
// Maximum with an unacknowledged seed, and two queue copies stored behind it
// whose live copies the test publishes itself, in the order it chooses.
type lateCopyFixture struct {
	nc             *nats.Conn
	c              *rawClient
	earlier, later *nats.Msg
}

func newLateCopyFixture(t *testing.T, natsURL, addr, clientID string) *lateCopyFixture {
	t.Helper()
	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	spy, err := nc.SubscribeSync(natsmqtt5.DefaultSubjectPrefix + ".ld.>")
	require.NoError(t, err)
	require.NoError(t, nc.Flush())

	c := dialRaw(t, addr)
	cp := rawConnect(clientID, 300)
	cp.Properties.ReceiveMaximum = packet.Uint16(1)
	c.connect(cp)
	c.subscribe("ld/#", packet.QoS1)
	pub, _ := connectClient(t, addr, connectOpts(clientID+"-pub"))
	pub.publish(&paho.Publish{Topic: "ld/x", QoS: 1, Payload: []byte("seed")})
	c.expectPublish() // takes the only slot, and is never acknowledged before the drop
	seedLive, err := spy.NextMsg(5 * time.Second)
	require.NoError(t, err)

	return &lateCopyFixture{
		nc:      nc,
		c:       c,
		earlier: queuedAndLive(t, js, seedLive, "earlier"),
		later:   queuedAndLive(t, js, seedLive, "later"),
	}
}

func (f *lateCopyFixture) publishLive(t *testing.T, m *nats.Msg) {
	t.Helper()
	require.NoError(t, f.nc.PublishMsg(m))
	require.NoError(t, f.nc.Flush())
}

// resumeAndRead connects back as clientID on addr and returns the payloads it is
// sent, acknowledging each.
func resumeAndRead(t *testing.T, addr, clientID string) []string {
	t.Helper()
	back := dialRaw(t, addr)
	require.True(t, back.connect(rawConnect(clientID, 300)).SessionPresent)
	var got []string
	for _, p := range back.p3Drain(1500 * time.Millisecond) {
		if pub, ok := p.(*packet.Publish); ok {
			got = append(got, string(pub.Payload))
			back.send(&packet.Puback{Ack: packet.Ack{PacketID: pub.PacketID}})
		}
	}
	return got
}

// A live copy that reaches the broker long after the connection ended (past the
// replay's two-second rewind from the connection's end) is still sent when the
// session resumes on the same broker.
func TestALiveCopyDelayedPastTheRewindIsStillSentOnResume(t *testing.T) {
	natsURL := startNATS(t)
	b, addr, _ := startBrokerHandle(t, natsURL, persistentWithQueue)
	f := newLateCopyFixture(t, natsURL, addr, "ld-mem")

	before := natsmqtt5.LiveCopies(b)
	f.publishLive(t, f.later)
	awaitLiveCopies(t, b, before, 1)
	// Both copies were stored at the start; the connection ends more than the
	// rewind later, so the stored time of "earlier" is before the rewind point.
	time.Sleep(2500 * time.Millisecond)
	f.c.drop()
	time.Sleep(500 * time.Millisecond) // the connection has ended and let go of the session
	before = natsmqtt5.LiveCopies(b)
	f.publishLive(t, f.earlier)
	awaitLiveCopies(t, b, before, 1)

	assert.Equal(t, []string{"seed", "earlier", "later"}, resumeAndRead(t, addr, "ld-mem"),
		"[MQTT-4.4.0-1] every message the session was owed, in the order the queue stored them")
}

// The same, when the session is restored from its stored record on another
// broker: the first broker, which still holds the subscription, notes the late
// copy and rewrites the record before the second broker claims it.
func TestALiveCopyDelayedPastTheRewindIsStillSentToASessionRestoredElsewhere(t *testing.T) {
	natsURL := startNATS(t)
	bA, addrA, _ := startBrokerHandle(t, natsURL, persistentWithQueue)
	addrB := startBroker(t, natsURL, persistentWithQueue)
	f := newLateCopyFixture(t, natsURL, addrA, "ld-restored")

	before := natsmqtt5.LiveCopies(bA)
	f.publishLive(t, f.later)
	awaitLiveCopies(t, bA, before, 1)
	time.Sleep(2500 * time.Millisecond)
	f.c.drop()
	time.Sleep(500 * time.Millisecond)
	before = natsmqtt5.LiveCopies(bA)
	f.publishLive(t, f.earlier)
	awaitLiveCopies(t, bA, before, 1)
	time.Sleep(300 * time.Millisecond) // the record rewrite that follows the note

	assert.Equal(t, []string{"seed", "earlier", "later"}, resumeAndRead(t, addrB, "ld-restored"),
		"[MQTT-4.4.0-1] the session record names the late copy, so the other broker replays it")
}

// A live copy that reaches the broker as the connection is closing, around the
// moment the replay position is worked out, is the window the timed tests above
// do not reach. The copy is published the instant the client's socket is
// dropped, round after round, and none may be lost.
func TestALiveCopyThatRacesTheConnectionsEndIsNeverLost(t *testing.T) {
	natsURL := startNATS(t)
	var logs syncBuffer
	b, addr, _ := startBrokerHandle(t, natsURL, persistentWithQueue, loggingBroker(t, &logs))
	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	spy, err := nc.SubscribeSync(natsmqtt5.DefaultSubjectPrefix + ".>")
	require.NoError(t, err)
	require.NoError(t, nc.Flush())

	for i := 0; i < 20; i++ {
		n := strconv.Itoa(i)
		id := "ld-race-" + n
		c := dialRaw(t, addr)
		cp := rawConnect(id, 300)
		cp.Properties.ReceiveMaximum = packet.Uint16(1)
		c.connect(cp)
		c.subscribe("rc"+n+"/#", packet.QoS1)
		pub, _ := connectClient(t, addr, connectOpts(id+"-pub"))
		pub.publish(&paho.Publish{Topic: "rc" + n + "/x", QoS: 1, Payload: []byte("seed" + n)})
		c.expectPublish()
		seed := nextWithPayload(t, spy, "seed"+n)
		earlier := queuedAndLive(t, js, seed, "earlier"+n)
		later := queuedAndLive(t, js, seed, "later"+n)
		before := natsmqtt5.LiveCopies(b)
		require.NoError(t, nc.PublishMsg(later))
		require.NoError(t, nc.Flush())
		awaitLiveCopies(t, b, before, 1)

		c.drop()
		require.NoError(t, nc.PublishMsg(earlier))
		require.NoError(t, nc.Flush())
		// The earlier copy races the connection's end on purpose, so it may find
		// no subscription at all and cannot be counted; the end of the
		// connection is the event to wait for.
		waitDetached(t, &logs, id)

		// The order is the queue's, which the two live copies racing a closing
		// connection can change; what matters here is that nothing is lost.
		assert.ElementsMatch(t, []string{"seed" + n, "earlier" + n, "later" + n}, resumeAndRead(t, addr, id),
			"[MQTT-4.4.0-1] round %d", i)
	}
}

// nextWithPayload returns the next message the spy sees with this payload.
func nextWithPayload(t *testing.T, spy *nats.Subscription, payload string) *nats.Msg {
	t.Helper()
	for {
		m, err := spy.NextMsg(5 * time.Second)
		require.NoError(t, err)
		// The queue's stored copy travels on a $queue subject nobody subscribes
		// to; a live copy made from it would reach no subscription.
		if string(m.Data) == payload && !strings.Contains(m.Subject, ".$queue.") {
			return m
		}
	}
}
