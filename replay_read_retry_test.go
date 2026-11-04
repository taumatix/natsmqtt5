package natsmqtt5_test

import (
	"testing"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// A resume whose queue read fails used to log "what it held is lost for this session" and send
// nothing until the next resume, though the cursor had not moved: the client stayed connected and
// could not tell the message from a lost one. The read is now made again while the client stays
// connected [MQTT-4.4.0-1]. SetFailedQueueFetches stands in for a JetStream that errors for a
// moment; the sockets, the broker and NATS are real.
func TestAResumeWhoseQueueReadFailsReadsAgainWhileTheClientStaysConnected_MQTT_4_4_0_1(t *testing.T) {
	natsURL := startNATS(t)
	r := newCursorRig(t, natsURL, "cur-read-fails", packet.QoS1)
	r.send("before")
	r.sub.drop()
	waitDetached(t, r.logs, r.id)
	r.pub.publish(&paho.Publish{Topic: r.topic, QoS: 1, Payload: []byte("owed")})

	natsmqtt5.SetFailedQueueFetches(r.b, 2)
	assert.Equal(t, []string{"owed"}, r.resume(r.addr),
		"[MQTT-4.4.0-1] the message stored while the session was away is sent although the first reads failed")
}
