package natsmqtt5

import "time"

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

// holdLoop tells JetStream, often enough to keep its AckWait from running out,
// that the sessions on this broker are still holding their backlog messages.
func (b *Broker) holdLoop() {
	defer b.wg.Done()
	t := time.NewTicker(sharedAckWait / 3)
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
				for _, subject := range s.heldSubjects() {
					b.settle(subject, ackProg)
				}
			}
		}
	}
}
