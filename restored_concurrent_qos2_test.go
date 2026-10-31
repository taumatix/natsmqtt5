package natsmqtt5_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5/packet"
)

// Many publishers send QoS 2 to one persistent subscriber while its connection is dropped
// and its session moves to another broker. Every message must reach it, and none may be
// sent to it as new twice [MQTT-4.3.3-2], [MQTT-4.3.3-6], [MQTT-4.5.0-1]. The copies of one
// message reach the queue and the live subscription in an order the publishers' concurrency
// decides, which the sequential tests around the restored rewind cannot produce.
func TestConcurrentQoS2PublishersAcrossADropAndAMoveToAnotherBrokerLoseAndRepeatNothing(t *testing.T) {
	t.Parallel()
	const publishers = 6
	const perPublisher = 40
	const total = publishers * perPublisher
	const dropAfter = 50

	var logs syncBuffer
	natsURL := startNATS(t)
	addrA, stopA := startStoppableBroker(t, natsURL, persistentWithQueue, loggingBroker(t, &logs))

	sub, _ := sessionPresentOnConnect(t, addrA, "q2-stress", false, 300)
	sub.subscribe("s/#", packet.QoS2)

	pubs := make([]*testClient, publishers)
	for i := range pubs {
		pubs[i], _ = connectClient(t, addrA, connectOpts(fmt.Sprintf("q2-stress-pub-%d", i)))
	}

	var wg sync.WaitGroup
	errs := make(chan error, total)
	for i, p := range pubs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perPublisher; j++ {
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				_, err := p.Client.Publish(ctx, &paho.Publish{
					Topic: fmt.Sprintf("s/%d", i), QoS: 2, Payload: []byte(fmt.Sprintf("%d-%d", i, j)),
				})
				cancel()
				if err != nil {
					errs <- fmt.Errorf("publisher %d message %d: %w", i, j, err)
					return
				}
			}
		}()
	}

	// A PUBLISH the broker sent before the drop and the client never read comes again with DUP 1,
	// so a message may be seen with no DUP 0 copy at all, but never with two.
	newCount := map[string]int{}
	seen := map[string]bool{}
	// consume handles one packet of a QoS 2 receive. It reports whether a message completed.
	consume := func(c *rawClient, p packet.Packet) bool {
		switch p := p.(type) {
		case *packet.Publish:
			if !p.Dup {
				newCount[string(p.Payload)]++
			}
			seen[string(p.Payload)] = true
			c.send(&packet.Pubrec{Ack: packet.Ack{PacketID: p.PacketID}})
		case *packet.Pubrel:
			c.send(&packet.Pubcomp{Ack: packet.Ack{PacketID: p.PacketID}})
			return true
		default:
			t.Fatalf("unexpected %T", p)
		}
		return false
	}

	for completed := 0; completed < dropAfter; {
		if consume(sub, sub.readWithin(10*time.Second)) {
			completed++
		}
	}
	sub.drop()
	waitDetached(t, &logs, "q2-stress")

	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	stopA()

	addrB := startBroker(t, natsURL, persistentWithQueue, loggingBroker(t, &logs))
	back, present := sessionPresentOnConnect(t, addrB, "q2-stress", false, 300)
	require.True(t, present)
	for {
		p, ok := tryRead(back, 3*time.Second)
		if !ok {
			break
		}
		consume(back, p)
	}

	assert.Len(t, seen, total, "every message reached the subscriber")
	for i := 0; i < publishers; i++ {
		for j := 0; j < perPublisher; j++ {
			payload := fmt.Sprintf("%d-%d", i, j)
			assert.LessOrEqual(t, newCount[payload], 1, "message %s was sent as new %d times", payload, newCount[payload])
		}
	}
}
