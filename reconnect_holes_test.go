package natsmqtt5_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// Options.ReconnectCatchUp and the replay cursor: a message the session is owed whose live copy
// never reached it is a hole once a scan has moved the cursor past it. The catch-up after a
// reconnect starts at the cursor, so it never read the hole, which waited for the session's next
// resume though the client stayed connected. The catch-up now sends the holes first, each read by
// its sequence, and the message is not sent again afterwards (MQTT-5.0 §4.3.2, [MQTT-4.4.0-1],
// [MQTT-4.3.3-2]).
func TestReconnectCatchUpSendsAHoleTheCursorLeftBelowItsStart_MQTT_4_4_0_1(t *testing.T) {
	natsmqtt5.SetCursorScanAfter(t, 1)
	natsURL := startNATS(t)
	proxy := newDropProxy(t, natsURL)
	r := newCursorRig(t, proxy.url(), "cur-rc-hole", packet.QoS1, quickReconnect, withReconnectCatchUp)

	r.send("before")
	lost := seqOf(t, r.sendWithheld("lost"))
	for i := range 20 {
		r.send("after" + strconv.Itoa(i))
	}
	awaitHole(t, r.b, r.id, lost)

	pubAddr := startBroker(t, natsURL, persistentWithQueue)
	other, _ := connectClient(t, pubAddr, connectOpts("cur-rc-hole-other"))
	cutOffAndPublish(t, proxy, r.b, other, r.topic, 1)

	completeOne(t, r.sub, "lost")
	assert.Empty(t, r.sub.completeFor(time.Second), "the hole is sent once, and nothing else with it")
	_, holes := natsmqtt5.ReplayCursor(r.b, r.id)
	assert.Empty(t, holes, "sending it settled the hole")
}
