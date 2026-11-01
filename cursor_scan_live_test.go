package natsmqtt5_test

import (
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5/v2"
	"github.com/taumatix/natsmqtt5/v2/packet"
)

// A live copy that reaches the session while a replay-cursor scan has read the
// queue and not yet applied it. The scan is held between its read and its apply
// (natsmqtt5.SetScanGate); a withheld message's late live copy and a fresh
// publish are delivered meanwhile. Once the scan applies, the cursor passes
// both without leaving a hole for either, and a resume sends neither again
// [MQTT-4.4.0-1].
func TestALiveCopyArrivingWhileAScanAppliesLosesNothingAndSendsNothingTwice_MQTT_4_4_0_1(t *testing.T) {
	natsmqtt5.SetCursorScanAfter(t, math.MaxInt32)
	natsURL := startNATS(t)
	r := newCursorRig(t, natsURL, "cur-scan-live", packet.QoS1)
	awaitCursor(t, r)

	late := r.sendWithheld("late")
	r.send("before")

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
	r.send("trigger")
	select {
	case <-reached:
	case <-time.After(5 * time.Second):
		t.Fatal("no scan reached the gate")
	}

	// The scan has read "late" as owed and is about to apply.
	publishLate(t, natsURL, late)
	completeOne(t, r.sub, "late")
	r.send("during")
	natsmqtt5.SetCursorScanAfter(t, math.MaxInt32)
	releaseGate()

	require.Eventually(t, func() bool {
		next, holes := natsmqtt5.ReplayCursor(r.b, r.id)
		return next > seqOf(t, late) && len(holes) == 0
	}, 5*time.Second, 20*time.Millisecond, "the scan applies and leaves no hole")
	r.dropPastTheRewind()

	assert.Empty(t, r.resume(r.addr), "nothing delivered during the apply is lost, and none is sent again")
}
