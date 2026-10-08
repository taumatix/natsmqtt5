package natsmqtt5_test

import (
	"os"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5/packet"
)

// A connection that is behind when it drops used to lose what was waiting for
// it: the resume replay started 2 seconds before the disconnect, and everything
// the client was owed from before that was gone [MQTT-3.1.2-23], [MQTT-4.5.0-1].
// Now the replay starts at the lowest queue sequence the client had not been
// sent, however long ago it was published.

// publishBurst publishes `n` QoS 1 messages numbered from `from` on topic
// "b/x" and then waits longer than the old 2-second rewind, so that a replay
// that started by time would miss all of them.
func publishBurst(t *testing.T, addr, publisher string, from, n int) {
	t.Helper()
	pub, _ := connectClient(t, addr, connectOpts(publisher))
	for _, m := range sequence(from, n) {
		pub.publish(&paho.Publish{Topic: "b/x", QoS: 1, Payload: []byte(m)})
	}
	time.Sleep(2500 * time.Millisecond)
}

// resume reconnects as `id` and requires the broker to have kept the session.
// A receive maximum of 0 leaves the default; a small one keeps the broker from
// sending the whole replay at once.
func resume(t *testing.T, addr, id string, receiveMax uint16) *rawClient {
	t.Helper()
	back := dialRaw(t, addr)
	cp := rawConnect(id, 300)
	if receiveMax != 0 {
		cp.Properties.ReceiveMaximum = packet.Uint16(receiveMax)
	}
	require.True(t, back.connect(cp).SessionPresent)
	return back
}

// The client reads a hundred messages and then stops reading while thousands
// more arrive: far more than the 2048 the broker holds for one connection, so
// the connection is behind, with messages waiting and others turned away.
// Dropped like that, the session gets every one of them on resume, in order,
// the unacknowledged ones resent with DUP set and none sent twice as new.
func TestAConnectionDroppedWhileBehindLosesNothingOnResume(t *testing.T) {
	addr := startBroker(t, startNATS(t))
	slow := slowSubscriber(t, addr, "behind-1", "b/#")

	pub, _ := connectClient(t, addr, connectOpts("behind-pub-0"))
	const total, read = 5000, 100
	for _, m := range sequence(0, total) {
		pub.publish(&paho.Publish{Topic: "b/x", QoS: 1, Payload: []byte(m)})
	}
	slow.readInOrder(sequence(0, read))
	time.Sleep(2500 * time.Millisecond)
	slow.drop()

	back := resume(t, addr, "behind-1", 0)
	back.readInOrder(sequence(read, total-read))
	back.expectNothing()
}

// The same, dropped during the catch-up from the stream rather than before it:
// the client has been reading what the stream replays when the network fails.
func TestAConnectionDroppedMidCatchUpResumesWhereItStood(t *testing.T) {
	addr := startBroker(t, startNATS(t))
	slow := slowSubscriber(t, addr, "behind-2", "b/#")

	pub, _ := connectClient(t, addr, connectOpts("behind-pub-1"))
	const total, read = 6000, 4000
	for _, m := range sequence(0, total) {
		pub.publish(&paho.Publish{Topic: "b/x", QoS: 1, Payload: []byte(m)})
	}
	slow.readInOrder(sequence(0, read))
	time.Sleep(2500 * time.Millisecond)
	slow.drop()

	back := resume(t, addr, "behind-2", 0)
	back.readInOrder(sequence(read, total-read))
	back.expectNothing()
}

// The client keeps its subscription across two drops, and the second comes
// before the first resume has been given everything.
//
// What the client read before the first drop was 0 to 9, each acknowledged. It
// cannot know the broker saw the last PUBACKs: if the old connection was closed
// before its reader reached them, those messages are still unacknowledged there
// and the resume resends them, with DUP set and their original Packet
// Identifiers [MQTT-4.4.0-1], [MQTT-3.3.1-1]. That is allowed, so the test
// accepts such copies, DUP required, and nothing else out of order. (This was
// the one failure seen on CI, undiagnosed: message 9 came where 10 was
// expected, and the test could not tell a DUP resend from a broker repeating it
// as new.)
func TestAConnectionDroppedDuringTheResumeReplayStillGetsTheRest(t *testing.T) {
	addr := startBroker(t, startNATS(t))
	slow := slowSubscriber(t, addr, "behind-3", "b/#")
	publishBurst(t, addr, "behind-pub-2", 0, 1500)
	slow.readInOrder(sequence(0, 10))
	slow.drop()

	second := resume(t, addr, "behind-3", 5)
	second.readInOrderAfterResume(sequence(0, 10), sequence(10, 200))
	time.Sleep(2500 * time.Millisecond)
	second.drop()

	third := resume(t, addr, "behind-3", 0)
	third.readInOrder(sequence(210, 1290))
	third.expectNothing()
}

// A client that takes longer than the old 30-second limit to be sent a backlog
// still gets all of it: the replay goes at the client's pace. Gated because it
// takes about 40 seconds; set NATSMQTT5_SLOW_TESTS=1 to run it.
func TestAReplayToASlowReaderIsNotCutOffAfterThirtySeconds(t *testing.T) {
	if os.Getenv("NATSMQTT5_SLOW_TESTS") == "" {
		t.Skip("takes about 40 seconds; set NATSMQTT5_SLOW_TESTS=1")
	}
	addr := startBroker(t, startNATS(t))
	c := dialRaw(t, addr)
	cp := rawConnect("behind-slow", 300)
	cp.Properties.ReceiveMaximum = packet.Uint16(1)
	c.connect(cp)
	c.subscribe("b/#", packet.QoS1)
	c.drop()

	// More than one fetch batch (256), so the cut-off would lose a tail.
	const total = 400
	pub, _ := connectClient(t, addr, connectOpts("behind-pub-3"))
	for _, m := range sequence(0, total) {
		pub.publish(&paho.Publish{Topic: "b/x", QoS: 1, Payload: []byte(m)})
	}

	back := dialRaw(t, addr)
	cp = rawConnect("behind-slow", 300)
	cp.Properties.ReceiveMaximum = packet.Uint16(1)
	require.True(t, back.connect(cp).SessionPresent)
	start := time.Now()
	for i, want := range sequence(0, total) {
		p := back.expectPublish()
		require.Equal(t, want, string(p.Payload), "message %d", i)
		time.Sleep(90 * time.Millisecond)
		back.send(&packet.Puback{Ack: packet.Ack{PacketID: p.PacketID}})
	}
	assert.Greater(t, time.Since(start), 31*time.Second, "the test must outlast the old limit")
	back.expectNothing()
}
