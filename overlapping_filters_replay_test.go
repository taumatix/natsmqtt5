package natsmqtt5_test

import (
	"testing"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5/packet"
)

// A resume reads the offline queue through the subjects the session's subscriptions match, and JetStream
// refuses a consumer whose filter subjects overlap. Filters that cross (x/+/c and x/b/+) or that a later,
// broader one swallows must still replay exactly what the session is owed, and nothing it is not.
//
//	[MQTT-4.4.0-1] "both the Client and Server MUST resend any unacknowledged PUBLISH packets" for a resumed
//	  session; what the session was owed while it was away is Session State [MQTT-3.1.2-23].
//	[MQTT-3.8.4-3] a subscription's Topic Filter decides which Application Messages it receives.
func replayAfterAbsence(t *testing.T, id string, filters []string, published []string) []string {
	t.Helper()
	natsURL := startNATS(t)
	addr := startBroker(t, natsURL, persistentWithQueue)
	first := dialRaw(t, addr)
	first.connect(rawConnect(id, 300))
	for _, f := range filters {
		first.subscribe(f, packet.QoS1)
	}
	first.drop()

	pub, _ := connectClient(t, addr, connectOpts(id+"-pub"))
	for _, topic := range published {
		pub.publish(&paho.Publish{Topic: topic, QoS: 1, Payload: []byte(topic)})
	}
	back := dialRaw(t, addr)
	require.True(t, back.connect(rawConnect(id, 300)).SessionPresent)
	var got []string
	for range published {
		p := back.expectPublish()
		got = append(got, p.Topic)
		back.send(&packet.Puback{Ack: packet.Ack{PacketID: p.PacketID}})
	}
	back.expectNothing()
	return got
}

func TestAReplayForCrossingFiltersDeliversEachMatchOnceAndNothingElse(t *testing.T) {
	got := replayAfterAbsence(t, "overlap-cross", []string{"x/+/c", "x/b/+"}, []string{"x/q/c", "x/b/z"})
	assert.Equal(t, []string{"x/q/c", "x/b/z"}, got,
		"[MQTT-4.4.0-1] the messages the two crossing filters match are replayed in order")
}

func TestAReplayWhoseBroadFilterSwallowsNarrowerOnesStillWorks(t *testing.T) {
	got := replayAfterAbsence(t, "overlap-swallow", []string{"y/b", "y/+/c", "y/#"}, []string{"y/q/z", "y/q/q/q"})
	assert.Equal(t, []string{"y/q/z", "y/q/q/q"}, got,
		"[MQTT-4.4.0-1] the messages only the broad filter matches are replayed")
}
