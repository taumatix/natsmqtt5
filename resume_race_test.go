package natsmqtt5_test

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/eclipse/paho.golang/paho"
)

// A client acknowledges what it has read and its network fails at once; the
// broker has been sending more, up to the client's Receive Maximum. On the
// resume "both the Client and Server MUST resend any unacknowledged PUBLISH
// packets" [MQTT-4.4.0-1], so what comes first is the oldest message the broker
// holds unacknowledged, resent with DUP [MQTT-3.3.1-1]. That can be a message
// the client did acknowledge (its PUBACK may not have been read before the old
// connection was closed; on Linux CI messages 6 to 9 came again that way), but
// never a message skipped.
//
// The skip was a broker bug: it took its snapshot of the unacknowledged
// messages while the dropped connection's delivery goroutine could still send
// and track one more, so that message was in neither the resend nor the replay
// until the next resume. Pinned to one scheduler thread, about one round in
// twenty failed with the first message missing before the fix.
func TestAResumeAfterADropNeitherRepeatsNorSkipsWhatTheLastConnectionHandled(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))
	addr := startBroker(t, startNATS(t))

	const rounds, read, window = 150, 10, 8
	for i := 0; i < rounds; i++ {
		id := fmt.Sprintf("race-sub-%d", i)
		topic := fmt.Sprintf("race%d/x", i)
		slow := slowSubscriber(t, addr, id, fmt.Sprintf("race%d/#", i))
		pub, _ := connectClient(t, addr, connectOpts(fmt.Sprintf("race-pub-%d", i)))
		for _, m := range sequence(0, 40) {
			pub.publish(&paho.Publish{Topic: topic, QoS: 1, Payload: []byte(m)})
		}
		slow.readInOrder(sequence(0, read))
		slow.drop()

		back := resume(t, addr, id, 5)
		back.readInOrderAfterResume(sequence(0, read), sequence(read, window))
		back.drop()
	}
}
