package natsmqtt5

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5/packet"
)

// The session's own tests: the end-to-end suite in retransmit_test.go proves a
// client sees its unacknowledged messages come back, and this pins the ordering
// that the wire test can only sample.

// unacknowledged must return the in-flight set in the order it was sent, which
// is the order a resumed session has to resend it in [MQTT-4.6.0-5].
//
// Checking that takes more care than it looks, because Go randomises map
// iteration far less than "randomised" suggests. A set this small lives in one
// map bucket, and iterating one bucket starts at a random slot of eight — the
// slots at or past the last entry wrap round to the beginning and come out in
// insertion order. Measured against an implementation with the ordering
// removed: a single check passed 6 times in 10, and the five-message
// end-to-end order test passed 6 times in 8. So one call proves nothing, and
// this asks repeatedly; 64 consecutive passes by luck is about 1e-27.
func TestUnacknowledgedReturnsTheOrderMessagesWereSentIn(t *testing.T) {
	s := newSession("order")

	// Out of order and wrapping past the top of the range, so that neither map
	// iteration nor a sort by Packet Identifier can pass this by accident.
	ids := []uint16{40000, 65535, 1, 7, 12, 65000}
	for _, id := range ids {
		s.trackInflight(&outbound{
			packetID: id,
			qos:      packet.QoS1,
			publish:  &packet.Publish{PacketID: id, QoS: packet.QoS1},
		})
	}

	for i := 0; i < 64; i++ {
		got := make([]uint16, 0, len(ids))
		for _, o := range s.unacknowledged() {
			got = append(got, o.packetID)
		}
		require.Equal(t, ids, got, "call %d returned the set in the wrong order", i)
	}
}

// The entries are copies, so that a resend can set the DUP flag on the packet
// it writes without touching the one the session is holding for the next
// resumption — two connections' delivery loops can otherwise overlap briefly
// after a takeover.
func TestUnacknowledgedReturnsCopies(t *testing.T) {
	s := newSession("copies")
	s.trackInflight(&outbound{packetID: 1, qos: packet.QoS2, publish: &packet.Publish{PacketID: 1}})

	set := s.unacknowledged()
	set[0].awaitingPubcomp = true
	set[0].packetID = 99

	live, ok := s.inflightEntry(1)
	assert.True(t, ok, "the entry must still be keyed by its original Packet Identifier")
	assert.False(t, live.awaitingPubcomp, "the caller's copy must not reach the session")
}

// A PUBREC moves the exchange on to expecting a PUBCOMP, which is what decides
// that a resend sends PUBREL rather than the PUBLISH again [MQTT-4.4.0-1]. A
// PUBREC for an identifier the session does not hold changes nothing.
func TestAwaitPubcomp(t *testing.T) {
	s := newSession("pubcomp")
	s.trackInflight(&outbound{packetID: 3, qos: packet.QoS2, publish: &packet.Publish{PacketID: 3}})

	s.awaitPubcomp(3)
	o, ok := s.inflightEntry(3)
	assert.True(t, ok)
	assert.True(t, o.awaitingPubcomp)

	s.awaitPubcomp(4) // must not panic or invent an entry
	_, ok = s.inflightEntry(4)
	assert.False(t, ok)
}
