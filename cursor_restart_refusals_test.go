package natsmqtt5_test

import (
	"math"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// The three cases in which the scan that restarts a dropped replay cursor
// (restartCursor) throws its result away, driven over real sockets against a
// real NATS server. Each is a restart that would otherwise succeed (the
// control is TestACursorDroppedWhileConnectedRestartsAndReplaysTheHole...), so
// the cursor staying away is the refusal's doing. A cursor started from a scan
// that read too little would skip a message the session is owed
// [MQTT-4.4.0-1]; refusing leaves the session on the time rewind, as before.
//
// The scan is held between its read and its apply by natsmqtt5.SetScanGate,
// which the restart scan calls too.

// withoutACursor leaves r's connected session without a replay cursor, its
// restart scans quiet, and every message it was sent older than the rewind.
func withoutACursor(t *testing.T, r *cursorRig) {
	t.Helper()
	natsmqtt5.SetCursorScanAfter(t, 1)
	natsmqtt5.SetMaxReplayHoles(t, 0)
	r.send("before")
	r.sendWithheld("gone")
	r.send("drops-it")
	require.Eventually(t, func() bool {
		next, _ := natsmqtt5.ReplayCursor(r.b, r.id)
		return next == 0
	}, 5*time.Second, 20*time.Millisecond, "a hole past the bound drops the cursor")
	natsmqtt5.SetCursorScanAfter(t, math.MaxInt32)
	time.Sleep(3 * cursorRewind) // the restart scan the last delivery started is over
	natsmqtt5.SetMaxReplayHoles(t, 1024)
}

// neverACursor fails if the session gets a replay cursor within a while.
func neverACursor(t *testing.T, r *cursorRig, msg string) {
	t.Helper()
	assert.Never(t, func() bool {
		next, _ := natsmqtt5.ReplayCursor(r.b, r.id)
		return next != 0
	}, 600*time.Millisecond, 20*time.Millisecond, msg)
}

// A scan that took longer than the rewind: the delivered ids it needs are kept
// only for twice the rewind, and they are what tells a hole from a delivered
// message.
func TestARestartScanThatTookLongerThanTheRewindDoesNotStartTheCursor_MQTT_4_4_0_1(t *testing.T) {
	natsURL := startNATS(t)
	r := newCursorRig(t, natsURL, "cur-restart-slow", packet.QoS1)
	withoutACursor(t, r)

	held := make(chan struct{})
	var once sync.Once
	natsmqtt5.SetScanGate(r.b, func() {
		once.Do(func() {
			time.Sleep(3 * cursorRewind)
			close(held)
		})
	})
	t.Cleanup(func() { natsmqtt5.SetScanGate(r.b, nil) })
	natsmqtt5.SetCursorScanAfter(t, 1)
	r.send("restarts-it")
	natsmqtt5.SetCursorScanAfter(t, math.MaxInt32)

	select {
	case <-held:
	case <-time.After(5 * time.Second):
		t.Fatal("no restart scan reached the gate")
	}
	neverACursor(t, r, "the scan outlived the rewind, so its result is not applied")
}

// A subscription made while the scan read: it may be owed what the scan passed
// without looking, so the result is thrown away.
func TestARestartScanOverlappedBySubscribeDoesNotStartTheCursor_MQTT_4_4_0_1(t *testing.T) {
	natsURL := startNATS(t)
	r := newCursorRig(t, natsURL, "cur-restart-sub", packet.QoS1)
	withoutACursor(t, r)

	reached, release := make(chan struct{}), make(chan struct{})
	var first, open sync.Once
	natsmqtt5.SetScanGate(r.b, func() {
		first.Do(func() {
			close(reached)
			<-release
		})
	})
	t.Cleanup(func() { natsmqtt5.SetScanGate(r.b, nil) })
	releaseGate := func() { open.Do(func() { close(release) }) }
	t.Cleanup(releaseGate)
	natsmqtt5.SetCursorScanAfter(t, 1)
	r.send("restarts-it")
	natsmqtt5.SetCursorScanAfter(t, math.MaxInt32)
	select {
	case <-reached:
	case <-time.After(5 * time.Second):
		t.Fatal("no restart scan reached the gate")
	}

	r.sub.subscribe(r.id+"-other/#", r.qos)
	releaseGate()

	neverACursor(t, r, "a subscription was made while the scan read, so its result is not applied")

	// The refusal costs one restart, not the session's cursor for good.
	natsmqtt5.SetScanGate(r.b, nil)
	natsmqtt5.SetCursorScanAfter(t, 1)
	r.send("again")
	require.Eventually(t, func() bool {
		next, _ := natsmqtt5.ReplayCursor(r.b, r.id)
		return next != 0
	}, 5*time.Second, 20*time.Millisecond, "the next restart scan starts the cursor")
}

// A message the connection holds below the scan's base: a rewind would start at
// it and send what lies between, which the scan did not read. The client has a
// Receive Maximum of 1, so messages stored before it acknowledges the first
// wait in the broker, older than the rewind by the time it does.
func TestARestartScanWhileTheConnectionHoldsAMessageBelowItsBaseDoesNotStartTheCursor_MQTT_4_4_0_1(t *testing.T) {
	natsURL := startNATS(t)
	r := &cursorRig{t: t, natsURL: natsURL, id: "cur-restart-floor", topic: "cur-restart-floor/x", qos: packet.QoS1, logs: &syncBuffer{}}
	r.b, r.addr, r.stop = startBrokerHandle(t, natsURL, persistentWithQueue, rewindOf(cursorRewind), loggingBroker(t, r.logs))
	r.sub = dialRaw(t, r.addr)
	cp := rawConnect(r.id, 300)
	cp.Properties.ReceiveMaximum = packet.Uint16(1)
	r.sub.connect(cp)
	r.sub.subscribe(r.id+"/#", r.qos)
	r.pub, _ = connectClient(t, r.addr, connectOpts(r.id+"-pub"))
	awaitCursor(t, r)
	withoutACursor(t, r)

	for _, p := range []string{"m1", "m2", "m3"} {
		r.pub.publish(&paho.Publish{Topic: r.topic, QoS: byte(r.qos), Payload: []byte(p)})
	}
	first := r.sub.expectPublish()
	require.Equal(t, "m1", string(first.Payload))
	time.Sleep(3 * cursorRewind) // all three are now older than the rewind
	// A message inside it, so that the scan's base is this one and not the end of
	// the stream: a scan that reads nothing waits out a fetch longer than the
	// rewind, which would be refused for its age and prove nothing here.
	r.pub.publish(&paho.Publish{Topic: r.topic, QoS: byte(r.qos), Payload: []byte("m4")})

	natsmqtt5.SetCursorScanAfter(t, 1)
	r.sub.send(&packet.Puback{Ack: packet.Ack{PacketID: first.PacketID}})
	second := r.sub.expectPublish() // m2 is delivered; m3 waits
	require.Equal(t, "m2", string(second.Payload))
	natsmqtt5.SetCursorScanAfter(t, math.MaxInt32)

	neverACursor(t, r, "m3 is held below what the scan read (from m4), so a cursor is not started")

	natsmqtt5.SetCursorScanAfter(t, 1)
	r.sub.send(&packet.Puback{Ack: packet.Ack{PacketID: second.PacketID}})
	third := r.sub.expectPublish()
	require.Equal(t, "m3", string(third.Payload))
	r.sub.send(&packet.Puback{Ack: packet.Ack{PacketID: third.PacketID}})
	completeOne(t, r.sub, "m4")
	r.send("m5") // the scan m4's delivery started may have run while m3 was still in hand
	require.Eventually(t, func() bool {
		next, _ := natsmqtt5.ReplayCursor(r.b, r.id)
		return next != 0
	}, 5*time.Second, 20*time.Millisecond, "once nothing is held, the restart scan starts the cursor")
}
