package natsmqtt5

import (
	"context"
	"errors"

	"github.com/nats-io/nats.go"
)

// Catching up from the offline queue.
//
// A connection holds at most deliveryQueueDepth messages waiting for its
// client. A client slower than its publishers reaches that, and the broker
// used to drop what came next, QoS 1 and 2 included [MQTT-4.1.0-1],
// [MQTT-4.5.0-1]. With the offline queue on, every QoS 1 and 2 message that
// passed through a broker has a copy in the queue stream, and its live copy
// carries the copy's sequence. So a connection that cannot take a message
// stops taking live ones at all, notes the lowest sequence it turned away, and
// its delivery goroutine reads from the stream from there: the client gets
// everything, in stream order, at its own pace, and nothing is held in memory
// for it but the stream position.
//
// While behind, every live copy with a sequence is turned away, so nothing
// newer can overtake what is being read from the stream [MQTT-4.6.0-6]. A
// round reads up to the stream's end as it was when the round began; the copies
// turned away meanwhile set up the next round. When a round ends with nothing
// turned away during it, the connection goes back to live delivery, and a live
// copy of something a round already delivered is recognised by its id and
// skipped.
//
// Messages without a queued copy (QoS 0, or published straight onto NATS, or
// with the queue off) are dropped when the client is this far behind, as
// before; QoS 0 is at most once.

// enqueueQueued is enqueue for a message whose copy is in the queue stream. It
// runs on a NATS dispatcher goroutine and does not block.
func (c *conn) enqueueQueued(d *delivery) {
	c.catchMu.Lock()
	defer c.catchMu.Unlock()
	if !c.behind {
		select {
		case c.deliveries <- d:
			return
		case <-c.done:
			return
		default:
		}
		c.behind = true
		c.logger.Info("client is not keeping up; catching up from the offline queue",
			"topic", d.topic, "queue_depth", deliveryQueueDepth)
	}
	if c.fromSeq == 0 || d.seq < c.fromSeq {
		c.fromSeq = d.seq
	}
	select {
	case c.catchup <- struct{}{}:
	default:
	}
}

// catchUp runs on the delivery goroutine. It first sends what was already
// waiting, which is all older than anything turned away, then reads the stream
// from the lowest sequence turned away, round after round, until a round turns
// nothing away.
func (c *conn) catchUp() error {
	q := c.broker.queue
	if q == nil {
		return nil
	}
	sent := map[uint64]bool{}
	if err := c.drainDeliveries(sent); err != nil {
		return err
	}
	c.replayedBefore, c.replayed = c.replayed, make(map[string]bool)

	for {
		c.catchMu.Lock()
		from := c.fromSeq
		c.fromSeq = 0
		if from == 0 {
			c.behind = false
			c.catchMu.Unlock()
			return nil
		}
		c.catchMu.Unlock()

		delivered := c.sess.deliveredIDs()
		// No deadline: a round goes at the client's pace, which is the point.
		// It ends with the connection.
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			select {
			case <-c.done:
				cancel()
			case <-ctx.Done():
			}
		}()
		var deliverErr error
		_, err := q.replaySeq(ctx, from, func(msg *nats.Msg) {
			if deliverErr != nil {
				return
			}
			id := messageID(msg)
			if id == "" || delivered[id] || c.wasReplayed(id) || sent[queueSeq(msg)] {
				return
			}
			deliverErr = c.deliverQueued(id, msg)
		})
		cancel()
		if deliverErr != nil {
			return deliverErr
		}
		if err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
	}
}

// drainDeliveries sends everything already waiting for the client, recording
// the stream sequence of each so the stream read does not send it again. A
// message that was waiting can have a higher sequence than one turned away,
// because two publishers' live copies need not arrive in sequence order.
func (c *conn) drainDeliveries(sent map[uint64]bool) error {
	for {
		select {
		case d := <-c.deliveries:
			if d.id != "" && c.wasReplayed(d.id) {
				continue
			}
			if d.sub != nil && !d.sub.live.Load() {
				continue
			}
			if d.seq != 0 {
				sent[d.seq] = true
			}
			if err := c.deliver(d); err != nil {
				return err
			}
		default:
			return nil
		}
	}
}

// wasReplayed reports whether the message with this id was delivered from the
// queue stream by this connection, in this catch-up or the one before.
func (c *conn) wasReplayed(id string) bool {
	return c.replayed[id] || c.replayedBefore[id]
}
