package natsmqtt5_test

import (
	"context"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// A restored rewind starts below a message the previous connection delivered when
// another publisher's message, queued earlier, had not reached the connection yet:
// publisher X wrote its queue copy (sequence 1) and stopped before its live
// publish, publisher Y then queued (sequence 2) and was delivered. The rewind
// reaches back to X's copy and passes over Y's, which must not be sent as new
// a second time [MQTT-4.3.3-2], [MQTT-4.3.3-6], [MQTT-4.5.0-1]. Unlike
// TestARestoredRewindDoesNotRedeliverAQoS2MessageItAlreadyDelivered, nothing is
// written into the session record: the broker produces the out-of-order state,
// through natsmqtt5.SetPublishQueuedGate, which holds X between its two copies.
func TestARestoredRewindAfterAPublisherStoppedBetweenItsCopiesDoesNotRepeatTheLaterMessage(t *testing.T) {
	var logs syncBuffer
	natsURL := startNATS(t)
	broker, addrA, stopA := startBrokerHandle(t, natsURL, persistentWithQueue, loggingBroker(t, &logs))

	sub, _ := sessionPresentOnConnect(t, addrA, "ooo-sub", false, 300)
	sub.subscribe("o/#", packet.QoS2)
	pubX, _ := connectClient(t, addrA, connectOpts("ooo-pub-x"))
	pubY, _ := connectClient(t, addrA, connectOpts("ooo-pub-y"))

	held, release := make(chan struct{}), make(chan struct{})
	natsmqtt5.SetPublishQueuedGate(broker, func(payload []byte) {
		if string(payload) == "x" {
			close(held)
			<-release
		}
	})
	t.Cleanup(func() { natsmqtt5.SetPublishQueuedGate(broker, nil) })

	xDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, err := pubX.Client.Publish(ctx, &paho.Publish{Topic: "o/x", QoS: 2, Payload: []byte("x")})
		xDone <- err
	}()
	select {
	case <-held:
	case <-time.After(10 * time.Second):
		t.Fatal("x never reached its queue copy")
	}

	pubY.publish(&paho.Publish{Topic: "o/y", QoS: 2, Payload: []byte("y")})
	sub.receiveQoS2UntilPubrel("o/y", "y")
	sub.drop()
	waitDetached(t, &logs, "ooo-sub")

	close(release) // x's live copy goes out now, to nobody
	require.NoError(t, <-xDone)
	stopA()

	addrB := startBroker(t, natsURL, persistentWithQueue, loggingBroker(t, &logs))
	back, present := sessionPresentOnConnect(t, addrB, "ooo-sub", false, 300)
	require.True(t, present)
	// y's exchange was cut before its PUBCOMP, so the broker owes its PUBREL.
	rel := back.expectPubrel()
	back.send(&packet.Pubcomp{Ack: packet.Ack{PacketID: rel.PacketID}})
	got := back.expectPublish()
	require.Equal(t, "o/x", got.Topic, "the rewind reaches x, queued before y and never delivered")
	back.send(&packet.Pubrec{Ack: packet.Ack{PacketID: got.PacketID}})
	rel = back.expectPubrel()
	back.send(&packet.Pubcomp{Ack: packet.Ack{PacketID: rel.PacketID}})
	back.expectNothing() // y was delivered before the drop and must not come again
}
