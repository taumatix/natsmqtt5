package natsmqtt5_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// A restarted cursor (a restart scan gave the session back its cursor) is kept in the session record
// under keys of its own, so a broker that starts afterwards sends the hole the time rewind no longer
// reaches [MQTT-4.4.0-1]. ReplayCursor stays unset: a broker that predates the keys would read it as a
// cursor that accounts for everything below it, which a restarted one does not.
func TestARestartedCursorSurvivesTheBrokerRestartingAndReplaysTheHole_MQTT_4_4_0_1(t *testing.T) {
	natsmqtt5.SetCursorScanAfter(t, 1)
	natsURL := startNATS(t)
	r := newCursorRig(t, natsURL, "cur-restarted-rec", packet.QoS1)
	restartAfterADrop(t, r)

	lost := seqOf(t, r.sendWithheld("lost"))
	r.send("after")
	awaitHole(t, r.b, r.id, lost)
	r.dropPastTheRewind()
	r.stop()

	rec := storedSession(t, natsURL, r.id)
	assert.NotZero(t, rec["RestartCursor"], "the record carries the restarted cursor")
	assert.NotZero(t, rec["RestartBase"], "and the base it accounts from")
	assert.Len(t, rec["RestartHoles"], 1)
	assert.NotContains(t, rec, "ReplayCursor", "never under the key an older broker trusts without a base")

	addrB := startBroker(t, natsURL, persistentWithQueue, rewindOf(cursorRewind))
	assert.Equal(t, []string{"lost"}, r.resume(addrB),
		"the hole older than the rewind is sent by the broker that restored the record, and nothing else")
}

// The same session without the restarted cursor in its record is the time rewind, which does not reach
// the hole: this is what the record keys change.
func TestAnOrdinaryCursorIsStillWrittenUnderReplayCursor(t *testing.T) {
	natsURL := startNATS(t)
	r := newCursorRig(t, natsURL, "cur-plain-rec", packet.QoS1)
	r.send("before")
	r.sendWithheld("lost")
	r.send("after")
	r.dropPastTheRewind()
	r.stop()

	rec := storedSession(t, natsURL, r.id)
	require.NotZero(t, rec["ReplayCursor"])
	assert.NotContains(t, rec, "RestartCursor")
	assert.NotContains(t, rec, "RestartBase")
}
