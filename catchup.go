package natsmqtt5

import (
	"context"
	"errors"
	"time"

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
// If the connection ends while behind, the same positions say where the next
// one starts: awayFloor hands the session the lowest sequence the client was
// owed and did not get, and the resume replay starts there.
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
		c.streamNext = from
		c.catchMu.Unlock()

		delivered := c.sess.deliveredIDs()
		ctx, cancel := c.streamContext()
		var deliverErr error
		_, err := q.replaySeq(ctx, from, func(msg *nats.Msg) {
			if deliverErr != nil || c.isClosed() {
				return
			}
			seq := queueSeq(msg)
			id := messageID(msg)
			if id != "" && !delivered[id] && !c.wasReplayed(id) && !sent[seq] {
				deliverErr = c.deliverQueued(id, msg)
			}
			// Not once the connection has ended: deliver then returns without
			// sending, and the message has not been handled.
			if deliverErr == nil && !c.isClosed() {
				c.streamNext = seq + 1
			}
		})
		cancel()
		if deliverErr != nil {
			return deliverErr
		}
		if err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
		if err == nil {
			c.streamNext = 0
		}
	}
}

// streamContext is the context for reading the queue stream. It has no
// deadline: a read goes at the client's pace, which is the point, and ends with
// the connection.
func (c *conn) streamContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-c.done:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}

// isClosed reports whether the connection has been closed.
func (c *conn) isClosed() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

// deliverTracked is deliver for a message taken off the connection's queue,
// noting its sequence while it is being sent: if the connection ends meanwhile,
// the message may not have gone out.
func (c *conn) deliverTracked(d *delivery) error {
	c.inHand = d.seq
	err := c.deliver(d)
	select {
	case <-c.done:
		// Left set: deliver returns nil when the connection ended while it
		// waited for room. A replay sending it again is stopped by its id if it
		// did go out.
	default:
		c.inHand = 0
	}
	return err
}

// awayFloor says where a replay for this connection's session should start,
// once the connection has ended. Instead of a time it names the lowest queue
// sequence the client was owed and did not get: a message waiting in the
// connection's queue, the one being sent, the position of a stream read in
// progress, or the lowest message a catch-up turned away. A client minutes
// behind when its network failed gets those minutes back.
//
// Only a sequence that was never delivered, or whose delivery is recorded by
// id (session.noteDelivered keeps those near the newest sequence), can be
// replayed from: starting earlier would send a delivered QoS 2 message twice.
// A session with a stored record has the sequence and those ids written into it
// on release (sessionRecord.AwayFromSeq, Delivered), so a restored session
// rewinds the same way.
func (c *conn) awayFloor() away {
	a := away{at: time.Now()}
	if !c.loopStarted {
		return a
	}
	c.close()
	<-c.loopDone

	if c.resume != nil && c.streamNext == 0 {
		// The resume replay never handled a message: what it was owed is still
		// owed, from the time it was to start at.
		return *c.resume
	}
	lowest := func(seq uint64) {
		if seq != 0 && (a.fromSeq == 0 || seq < a.fromSeq) {
			a.fromSeq = seq
		}
	}
	lowest(c.inHand)
	lowest(c.streamNext)
	c.catchMu.Lock()
	lowest(c.fromSeq)
	c.catchMu.Unlock()
	for {
		select {
		case d := <-c.deliveries:
			lowest(d.seq)
			continue
		default:
		}
		break
	}
	return a
}

// drainDeliveries sends everything already waiting for the client, recording
// the stream sequence of each so the stream read does not send it again. A
// message that was waiting can have a higher sequence than one turned away,
// because two publishers' live copies need not arrive in sequence order.
func (c *conn) drainDeliveries(sent map[uint64]bool) error {
	for {
		select {
		case <-c.done:
			return nil
		default:
		}
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
			if err := c.deliverTracked(d); err != nil {
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
