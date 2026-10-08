package natsmqtt5

import (
	"strconv"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/taumatix/natsmqtt5/packet"
)

// Message Expiry Interval (MQTT-5.0 §3.3.2.3.3).
//
//	"If the Message Expiry Interval has passed and the Server has not managed
//	to start onward delivery to a matching subscriber, then it MUST delete the
//	copy of the message for that subscriber" [MQTT-3.3.2-5].
//
//	"The PUBLISH packet sent to a Client by the Server MUST contain a Message
//	Expiry Interval set to the received value minus the time that the message
//	has been waiting in the Server" [MQTT-3.3.2-6].
//
// The wait starts when the broker took the message in: the moment its NATS
// copy arrived for a live delivery, the JetStream timestamp of its stored copy
// for a queue replay, a catch-up or a shared backlog, and the moment it was
// stored for a retained message (the stream's timestamp once read back). Every
// path goes through remainingExpiry, so they agree on what a second is.
//
// The interval is whole seconds and so is the time waited, rounded down. A
// message therefore goes out with its interval less the whole seconds it has
// waited: 2 s with 1.3 s waited leaves 1. That can overstate what is left by
// less than a second, and never the other way, which is the side to err on:
// the alternative, rounding the remaining time down, would send a still-live
// message with an interval of 0 for its last second, which a receiver
// reads as already expired. The cut-off itself is exact in whole seconds: a
// message has expired once it has waited its whole interval.
//
// An interval of 0 is not "already expired" for a message that has waited
// under a second, and an absent one never expires.

// hdrArrived is set on the copy of a message read back from JetStream, with the
// stream's timestamp in nanoseconds. It never reaches NATS: the copies it is
// set on are built inside the broker for delivery.
const hdrArrived = "Mqtt5-Arrived"

// arrivedAt is when the broker took the message in: the stream timestamp of a
// copy read back from JetStream, or now for a message arriving live.
func arrivedAt(msg *nats.Msg, now time.Time) time.Time {
	if msg.Header != nil {
		if v := msg.Header.Get(hdrArrived); v != "" {
			if ns, err := strconv.ParseInt(v, 10, 64); err == nil {
				return time.Unix(0, ns)
			}
		}
	}
	return now
}

// setArrived records the stream timestamp on a copy read back from JetStream.
func setArrived(h nats.Header, at time.Time) nats.Header {
	if h == nil {
		h = nats.Header{}
	}
	h.Set(hdrArrived, strconv.FormatInt(at.UnixNano(), 10))
	return h
}

// remainingExpiry is the interval to send for a message received with
// interval that arrived at arrived, and whether it has expired. When it has,
// remaining is 0.
func remainingExpiry(interval uint32, arrived, now time.Time) (remaining uint32, expired bool) {
	waitedSeconds := int64(now.Sub(arrived) / time.Second)
	if waitedSeconds <= 0 {
		return interval, false
	}
	if waitedSeconds >= int64(interval) {
		return 0, true
	}
	return interval - uint32(waitedSeconds), false
}

// applyExpiry sets the interval a delivery carries to what is left of it, or
// reports that it has expired. A delivery with no interval never expires. The
// interval the message arrived with is kept in d.expiry for a later resend.
func (d *delivery) applyExpiry(now time.Time) (expired bool) {
	if d.props == nil || d.props.MessageExpiryInterval == nil {
		return false
	}
	if d.expiry == nil {
		d.expiry = packet.Uint32(*d.props.MessageExpiryInterval)
	}
	remaining, expired := remainingExpiry(*d.expiry, d.arrived, now)
	if expired {
		return true
	}
	d.props.MessageExpiryInterval = packet.Uint32(remaining)
	return false
}
