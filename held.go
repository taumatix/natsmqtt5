package natsmqtt5

import (
	"context"
	"time"
)

// Keeping a shared subscription's messages for the sessions that were given
// them.
//
// A member pulls a message from its group's backlog (shared.go) and sends it to
// its client. The message stays unacknowledged in JetStream until the client's
// PUBACK, so it is not lost if the session ends first: the session's end hands
// it back (session.handBackHeld). While the session lives, JetStream must not
// give the message to another member, which is what its AckWait would do, so
// this broker says "still working" for every message its sessions hold,
// connected or not: "the Server MAY wait for the Client to reconnect and
// retransmit the message to that Client" (MQTT-5.0 §4.8.2). A broker that dies
// stops saying it, and the group gets the message after AckWait.
//
// Settling is by publishing the protocol's reply to the message's
// acknowledgement subject, which any connection to the server may do. That is
// what lets a session restored on another broker settle what the first one held.
//
// A held message counts towards the consumer's MaxAckPending, JetStream's
// default of which (1000) bounds how many the group's sessions can hold at once.

var (
	ackAck  = []byte("+ACK")
	ackNak  = []byte("-NAK")
	ackProg = []byte("+WPI")
)

// ack settles a backlog message for good.
func (b *Broker) ack(subject string) { b.settle(subject, ackAck) }

// handBack makes backlog messages available to the group's other members.
func (b *Broker) handBack(subjects []string) {
	for _, s := range subjects {
		b.settle(s, ackNak)
	}
}

func (b *Broker) settle(subject string, reply []byte) {
	if err := b.nc.Publish(subject, reply); err != nil {
		b.logger.Debug("settling a shared subscription's message failed", "error", err)
	}
}

// holdOrphans is holdLoop for the sessions of dead brokers (Options.SharedQoS2, on by default).
// A broker that was killed holds its sessions' messages no longer, and its
// client may stay away for as long as its session lasts: the group is not to be
// given a message that is that client's [MQTT-4.8.2-4], [MQTT-4.8.2-5], so the
// surviving brokers say "still working" for the messages named by the records
// that dead brokers left Attached, without restoring the session. A message
// held past SharedHoldLimit, counted from the first time this broker saw it
// orphaned, is handed back once, as it is for a live session. The client coming
// back restores the session on a broker that holds the message again, and the
// record's owner then answers.
func (b *Broker) holdOrphans(every time.Duration) {
	defer b.wg.Done()
	t := time.NewTicker(every)
	defer t.Stop()
	type orphan struct {
		since     time.Time
		handedOff bool
	}
	seenBefore := map[string]orphan{}
	for {
		select {
		case <-b.shutdown:
			return
		case <-t.C:
		}
		if b.store == nil {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), every)
		subjects := b.store.orphanedHolds(ctx)
		cancel()
		now := time.Now()
		seen := make(map[string]orphan, len(subjects))
		var expired []string
		for _, subject := range subjects {
			o, ok := seenBefore[subject]
			if !ok {
				o.since = now
			}
			if !o.handedOff {
				if limit := b.opts.SharedHoldLimit; limit > 0 && now.Sub(o.since) >= limit {
					expired = append(expired, subject)
					o.handedOff = true
				} else {
					b.settle(subject, ackProg)
				}
			}
			seen[subject] = o
		}
		if len(expired) > 0 {
			b.logger.Debug("handing back shared messages held for a dead broker's session past SharedHoldLimit",
				"messages", len(expired))
			b.handBack(expired)
		}
		seenBefore = seen
	}
}

// holdLoop tells JetStream, often enough to keep its AckWait from running out,
// that the sessions on this broker are still holding their backlog messages.
func (b *Broker) holdLoop(every time.Duration) {
	defer b.wg.Done()
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-b.shutdown:
			return
		case <-t.C:
			b.mu.Lock()
			sessions := make([]*session, 0, len(b.sessions))
			for _, s := range b.sessions {
				sessions = append(sessions, s)
			}
			b.mu.Unlock()
			for _, s := range sessions {
				held, expired := s.heldSubjects(time.Now(), b.opts.SharedHoldLimit)
				for _, subject := range held {
					b.settle(subject, ackProg)
				}
				if len(expired) > 0 {
					b.logger.Debug("handing back shared messages held past SharedHoldLimit",
						"client_id", s.clientID, "messages", len(expired))
					b.handBack(expired)
				}
			}
		}
	}
}
