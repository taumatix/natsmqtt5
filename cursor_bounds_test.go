package natsmqtt5_test

import (
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5/v2"
	"github.com/taumatix/natsmqtt5/v2/packet"
)

// The replay cursor's two bounds that keep its record small (cursor.go,
// session.go), driven over a real socket against a real NATS server with the
// bounds lowered by test-only hooks (16384 ids above the cursor, and 4096+65536
// ids in a record, take minutes to deliver under -race). What each bound costs
// is the same: the session is replayed from the time rewind, which no longer
// reaches a message whose live copy never came [MQTT-4.4.0-1].
//
// Scans are held off (SetCursorScanAfter) so the cursor stays where the
// subscription made it and the ids delivered above it pile up.

// awaitCursor waits for the session to have a replay cursor.
func awaitCursor(t *testing.T, r *cursorRig) {
	t.Helper()
	require.Eventually(t, func() bool {
		next, _ := natsmqtt5.ReplayCursor(r.b, r.id)
		return next != 0
	}, 5*time.Second, 20*time.Millisecond, "the session has a replay cursor")
}

// deliverMany has the subscriber receive and complete n messages.
func (r *cursorRig) deliverMany(n int) {
	r.t.Helper()
	for i := range n {
		r.send("m" + strconv.Itoa(i))
	}
}

// Control: with the bound at its default the cursor outlives the same
// deliveries, and the message the rewind does not reach is replayed.
func TestACursorWithFewIDsAboveItKeepsReplayingAMessageOlderThanTheRewind_MQTT_4_4_0_1(t *testing.T) {
	natsmqtt5.SetCursorScanAfter(t, math.MaxInt32)
	natsURL := startNATS(t)
	r := newCursorRig(t, natsURL, "cur-bound-ctl", packet.QoS1)
	awaitCursor(t, r)

	r.sendWithheld("lost")
	r.deliverMany(130)
	next, _ := natsmqtt5.ReplayCursor(r.b, r.id)
	require.NotZero(t, next, "130 ids are far below the bound")
	r.dropPastTheRewind()

	assert.Equal(t, []string{"lost"}, r.resume(r.addr))
}

// Past the bound the cursor is dropped while the client stays connected, and
// the session falls back to the rewind, which does not reach the message.
func TestMoreIDsAboveTheCursorThanItsBoundDropItAndTheRewindDoesNotReachTheMessage_MQTT_4_4_0_1(t *testing.T) {
	natsmqtt5.SetCursorScanAfter(t, math.MaxInt32)
	natsmqtt5.SetCursorIDBound(t, 5)
	natsURL := startNATS(t)
	r := newCursorRig(t, natsURL, "cur-bound", packet.QoS1)
	awaitCursor(t, r)

	r.sendWithheld("lost")
	r.deliverMany(130)
	require.Eventually(t, func() bool {
		next, _ := natsmqtt5.ReplayCursor(r.b, r.id)
		return next == 0
	}, 5*time.Second, 20*time.Millisecond, "more ids above the cursor than the bound drop it")
	r.dropPastTheRewind()

	assert.Empty(t, r.resume(r.addr), "the time rewind is all that is left, and it starts after the message")
}

// A record that cannot carry every id above the cursor carries no cursor, and
// the broker warns that ids were dropped.
func TestARecordThatLeftIDsOutCarriesNoCursorAndTheRewindIsAllThatIsLeft_MQTT_4_4_0_1(t *testing.T) {
	natsmqtt5.SetCursorScanAfter(t, math.MaxInt32)
	natsmqtt5.SetStoredDeliveredBound(t, 5)
	natsURL := startNATS(t)
	r := newCursorRig(t, natsURL, "cur-record", packet.QoS2)
	addrB := startBroker(t, natsURL, persistentWithQueue, rewindOf(cursorRewind))
	awaitCursor(t, r)

	r.sendWithheld("lost")
	r.deliverMany(12)
	next, _ := natsmqtt5.ReplayCursor(r.b, r.id)
	require.NotZero(t, next, "the connected session still has its cursor: only its record cannot carry it")
	r.dropPastTheRewind()

	rec := storedSession(t, natsURL, r.id)
	assert.Zero(t, rec["ReplayCursor"], "the record left ids out, so it carries no cursor")
	assert.True(t, strings.Contains(r.logs.String(), "the session record keeps only some of the delivered message ids"), "the release warned")
	assert.Empty(t, r.resume(addrB), "[MQTT-4.4.0-1] replayed from the time rewind, which starts after the message")
}
