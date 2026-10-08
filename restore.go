package natsmqtt5

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/taumatix/natsmqtt5/packet"
	"github.com/taumatix/natsmqtt5/topic"
)

// Restoring a session's in-flight messages (Options.PersistentSessions).
//
// "When a Client reconnects with Clean Start set to 0 and a session is present,
// both the Client and Server MUST resend any unacknowledged PUBLISH packets
// (where QoS > 0) and PUBREL packets using their original Packet Identifiers"
// [MQTT-4.4.0-1]. A broker that has never held the connection can only do that
// if the session record says what was in flight, so the record carries each
// message's Packet Identifier, QoS state and offline-queue sequence
// (sessionRecord.Inflight) and the identifiers of the client's QoS 2 PUBLISH
// packets not yet released (sessionRecord.ReceivedQoS2), which is what lets the
// broker answer a resent one without forwarding it again [MQTT-4.3.3-10].
//
// The payload is not copied into the record. It is read back from the queue
// stream by sequence, here, during the handshake, so the retransmission that
// follows the CONNACK finds the in-flight set as a session that never left
// memory would hold it, and resends it through the same code (retransmit.go).
//
// What this cannot restore, each logged when it happens:
//   - a message with no copy in the queue (the queue is off, or the message was
//     retained) whose PUBLISH was over maxStoredPublish, so it was left out of the
//     record when it was written;
//   - a message whose copy has since left the queue (OfflineQueueMaxAge, or the
//     stream's limits);
//   - anything of a broker that was killed rather than stopped, which wrote no
//     record at all;
//   - entries beyond what one record value holds (fitRecord).

// restoreSessionState rebuilds the in-flight set and the received QoS 2
// identifiers a claim found in the record into a session that has none. It never
// fails the CONNECT: what cannot be restored is logged, and the session carries
// on with the rest.
func (b *Broker) restoreSessionState(ctx context.Context, s *session, rec *sessionRecord) {
	if len(rec.receivedQoS2Was) > 0 {
		s.restoreReceivedQoS2(rec.receivedQoS2Was)
	}
	lost := 0
	for _, st := range rec.inflightWas {
		o := b.restoreEntry(ctx, s.clientID, st)
		if o == nil {
			lost++
			if st.Ack != "" {
				// Nothing will be sent, so no PUBACK will settle it.
				b.handBack([]string{st.Ack})
			}
			continue
		}
		s.restoreInflight(o)
	}
	if lost > 0 {
		b.logger.Warn("some unacknowledged messages of a restored session could not be restored and will not be resent",
			"client_id", s.clientID, "lost", lost, "restored", len(rec.inflightWas)-lost)
	}
}

// restoreEntry turns one stored entry back into an in-flight message, or returns
// nil when it cannot.
func (b *Broker) restoreEntry(ctx context.Context, clientID string, st storedInflight) *outbound {
	if st.ID == 0 {
		return nil
	}
	if st.Rel {
		// Past the PUBREC: the client owns the message and the PUBREL is all
		// that is owed [MQTT-4.3.3-8]. There is no payload to fetch.
		return &outbound{packetID: st.ID, qos: packet.QoS2, awaitingPubcomp: true, publish: &packet.Publish{}}
	}
	if st.Blob != "" && b.store != nil {
		raw, err := b.store.getBlob(ctx, st.Blob)
		if err != nil {
			b.logger.Warn("the stored payload of an unacknowledged message could not be read",
				"client_id", clientID, "packet_id", st.ID, "error", err)
			return nil
		}
		st.Pub = raw
	}
	if len(st.Pub) > 0 {
		return restoreStoredPublish(st)
	}
	if b.queue == nil || st.Seq == 0 || st.QoS == packet.QoS0 {
		return nil
	}
	msg, err := b.queue.message(ctx, st.Seq)
	if err != nil {
		level := slog.LevelWarn
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			// Aged out of the queue: expected after OfflineQueueMaxAge.
			level = slog.LevelInfo
		}
		b.logger.Log(ctx, level, "the queued copy of an unacknowledged message could not be read",
			"client_id", clientID, "packet_id", st.ID, "queue_seq", st.Seq, "error", err)
		return nil
	}
	subject, ok := topic.TrimPrefix(b.opts.SubjectPrefix, msg.Subject)
	if !ok {
		return nil
	}
	_, _, _, props := fromNATS(msg)
	if st.SubID != 0 {
		props.SubscriptionIdentifiers = []int{st.SubID}
	}
	d := &delivery{
		topic:   topic.SubjectToName(subject),
		payload: msg.Data,
		qos:     st.QoS,
		retain:  st.Retain,
		props:   props,
		arrived: arrivedAt(msg, time.Now()),
	}
	// Sets the interval the resend counts down from, and the properties it
	// starts from. Whether the message has expired is the resend's to decide,
	// so that an expired QoS 1 message is completed the way any other is.
	d.applyExpiry(time.Now())
	return &outbound{
		packetID: st.ID,
		qos:      st.QoS,
		publish: &packet.Publish{
			PacketID:   st.ID,
			Topic:      d.topic,
			QoS:        st.QoS,
			Retain:     st.Retain,
			Payload:    d.payload,
			Properties: d.props,
		},
		arrived:    d.arrived,
		expiry:     d.expiry,
		queueSeq:   st.Seq,
		ackSubject: st.Ack,
	}
}

// restoreStoredPublish turns an entry that carries its own PUBLISH back into an
// in-flight message.
func restoreStoredPublish(st storedInflight) *outbound {
	p, err := packet.Read(bufio.NewReader(bytes.NewReader(st.Pub)), 0)
	pub, ok := p.(*packet.Publish)
	if err != nil || !ok || pub.QoS == packet.QoS0 {
		return nil
	}
	pub.PacketID, pub.QoS, pub.Dup = st.ID, st.QoS, false
	arrived := st.At
	if arrived.IsZero() {
		arrived = time.Now()
	}
	o := &outbound{packetID: st.ID, qos: st.QoS, publish: pub, arrived: arrived, expiry: st.Exp}
	if o.expiry == nil && pub.Properties != nil && pub.Properties.MessageExpiryInterval != nil {
		o.expiry = packet.Uint32(*pub.Properties.MessageExpiryInterval)
	}
	return o
}

// message reads the queue copy at seq as a live message would look: the subject
// is the one it was published on, and the headers carry its own sequence and
// the time the broker took it in.
func (q *offlineQueue) message(ctx context.Context, seq uint64) (*nats.Msg, error) {
	raw, err := q.stream.GetMsg(ctx, seq)
	if err != nil {
		return nil, err
	}
	header := raw.Header
	if header == nil {
		header = nats.Header{}
	}
	header.Set(hdrQueueSeq, strconv.FormatUint(raw.Sequence, 10))
	return &nats.Msg{Subject: q.liveSubject(raw.Subject), Header: setArrived(header, raw.Time), Data: raw.Data}, nil
}
