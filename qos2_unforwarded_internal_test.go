package natsmqtt5

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// [MQTT-4.3.3-10]: the stored record may hold a received QoS 2 identifier only
// once its message has been forwarded. A client whose broker died between
// receiving the PUBLISH and forwarding it never saw a PUBREC, and its resend
// would otherwise be taken for a repeat and dropped. This is the one ordering a
// SIGKILL cannot be aimed at, so it is held at the state the checkpoint reads.
func TestAReceivedQoS2IdentifierIsStoredOnlyOnceItsMessageIsForwarded(t *testing.T) {
	s := newSession("fwd")
	assert.False(t, s.markQoS2Received(7))
	_, received, _ := s.inflightState()
	assert.Empty(t, received, "not forwarded yet")

	s.qos2Forwarded(7)
	_, received, _ = s.inflightState()
	assert.Equal(t, []uint16{7}, received)

	assert.True(t, s.markQoS2Received(7), "a repeat")
	assert.True(t, s.releaseQoS2(7))
	_, received, _ = s.inflightState()
	assert.Empty(t, received)
}
