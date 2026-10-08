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
// packets" [MQTT-4.4.0-1], so the first thing it sends is the oldest message
// the client has not acknowledged: not one it did acknowledge, and not a later
// one with that message skipped.
//
// The broker used to take its snapshot of the unacknowledged messages while the
// dropped connection was still ending. With one scheduler thread that is common:
// the new CONNECT is handled before the old connection's reader has decoded the
// last PUBACK (so an acknowledged message came again, which is the failure the
// resume-replay test showed once on CI), or before its delivery goroutine has
// tracked the message it was sending (so that one was skipped until the next
// resume). Pinned to one thread here, each of these fails about one run in
// twenty before the fix.
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
		back.readInOrder(sequence(read, window))
		back.drop()
	}
}
