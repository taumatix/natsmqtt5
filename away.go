package natsmqtt5

import (
	"log/slog"

	"github.com/taumatix/natsmqtt5/packet"
)

// A broker with no offline queue (DisableOfflineQueue, or no JetStream) keeps no copy of a QoS 1 or 2
// message, so the only place one can wait for a session that is away is this process's memory. The
// session's NATS subscriptions stay up while its client is away, so the messages do arrive; they used
// to be dropped there while the CONNACK of the resumed session still said Session Present 1.
//
// They are held now, per session and bounded by Options.AwayBufferMessages, in arrival order, and
// attach queues them on the next connection ahead of anything newer. A session that overflows the bound
// is marked lost (session.awayLost) and refused for resumption, so that its client is told Session
// Present 0 [MQTT-3.2.2-3] rather than 1 with a gap [MQTT-3.1.2-23], [MQTT-4.5.0-1]. The broker does not
// promise what it cannot keep.

const (
	// defaultAwayBuffer is AwayBufferMessages when it is 0.
	defaultAwayBuffer = 1000
	// maxAwayBuffer is the most attach can queue on a new connection without waiting.
	maxAwayBuffer = deliveryQueueDepth
)

// awayLimit is how many messages a session that is away may hold; 0 holds none.
func (b *Broker) awayLimit() int {
	switch n := b.opts.AwayBufferMessages; {
	case n < 0:
		return 0
	case n == 0:
		return defaultAwayBuffer
	default:
		return min(n, maxAwayBuffer)
	}
}

// holdsAway reports whether QoS 1 and 2 messages for a session that is away are held in memory: when
// there is no offline queue to hold them.
func (b *Broker) holdsAway() bool { return b.queue.Load() == nil }

// enqueueHeld queues a QoS 1 or 2 message on c when the broker has no offline queue. A connection that
// has finished is sealed, and the message goes to the session instead, to be held for the next one.
func (c *conn) enqueueHeld(d *delivery) {
	c.sendMu.Lock()
	if c.sealed.Load() {
		c.sendMu.Unlock()
		if next := c.sess.routeAway(d, c.broker.awayLimit(), c.logger); next != nil {
			next.enqueue(d)
		}
		return
	}
	select {
	case c.deliveries <- d:
		c.sendMu.Unlock()
		return
	default:
	}
	c.sendMu.Unlock()
	c.dropBehind(d)
}

// keepUnsent notes deliveries the delivery goroutine took off the queue and did not get to prepare
// before the connection ended; sealAndHold passes them to the session. Only that goroutine calls it.
func (c *conn) keepUnsent(ds []*delivery) {
	if !c.broker.holdsAway() {
		return
	}
	c.unsent = append(c.unsent, ds...)
}

// sealAndHold ends the connection's part in delivery when there is no offline queue: it takes what was
// waiting for the client, and what the delivery goroutine had taken and not prepared, to the session, so
// that the next connection is sent it. It runs once the delivery goroutine has stopped.
func (c *conn) sealAndHold() {
	c.sendMu.Lock()
	c.sealed.Store(true)
	held := c.unsent
	c.unsent = nil
drain:
	for {
		select {
		case d := <-c.deliveries:
			held = append(held, d)
		default:
			break drain
		}
	}
	c.sendMu.Unlock()
	c.sess.holdAway(held, c.broker.awayLimit(), c.logger)
}

// holdAway puts messages older than anything routeAway has held at the front of the buffer.
func (s *session) holdAway(older []*delivery, limit int, logger *slog.Logger) {
	keep := older[:0:0]
	for _, d := range older {
		if d.qos != packet.QoS0 && (d.sub == nil || d.sub.wanted()) {
			keep = append(keep, d)
		}
	}
	if len(keep) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.awayLost || s.discarded || s.expirySeconds == 0 {
		return
	}
	if len(keep)+len(s.away) > limit {
		s.away, s.awayLost = nil, true
		logger.Warn("a session that is away lost a message: its buffer is full, so the session is ended "+
			"and its client will be told Session Present 0 (Options.AwayBufferMessages)",
			"client_id", s.clientID, "limit", limit)
		return
	}
	s.away = append(keep, s.away...)
}

// routeAway is where a QoS 1 or 2 message goes when the broker has no offline queue and no copy of
// it is stored: the connection that is to deliver it, or nil when it was held for the next
// connection or could not be. QoS 0 is dropped between connections, as it may be. limit is how
// many messages the session may hold; a message past it ends the session's promise, see awayLost.
func (s *session) routeAway(d *delivery, limit int, logger *slog.Logger) *conn {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c := s.conn; c != nil && !c.sealed.Load() {
		return c
	}
	if d.qos == packet.QoS0 || s.awayLost || s.discarded || s.expirySeconds == 0 {
		return nil
	}
	if len(s.away) >= limit {
		s.away, s.awayLost = nil, true
		logger.Warn("a session that is away lost a message: its buffer is full, so the session is ended "+
			"and its client will be told Session Present 0 (Options.AwayBufferMessages)",
			"client_id", s.clientID, "limit", limit, "topic", d.topic)
		return nil
	}
	s.away = append(s.away, d)
	return nil
}
