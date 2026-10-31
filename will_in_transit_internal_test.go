package natsmqtt5

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5/packet"
)

// A connection's Will is held by the session from its CONNECT to its publication. Between the connection's end
// and the delay timer there is a store write (willStore.ended), and a Broker.Reauthorize landing in it found the
// Will in neither place, discarded nothing, and the revoked Will was published when its delay ran out. The Will
// must be findable and discardable at every point of the hand-over.
func TestAWillTakenForPublicationCanStillBeDiscarded(t *testing.T) {
	c := &conn{}
	s := &session{}
	w := &packet.Will{Topic: "alerts/revoked"}
	lease := &willLease{}
	s.setWill(c, w, 0, lease)

	got, _, gotLease := s.takeWillToPublish(c)
	require.Same(t, w, got)
	require.Same(t, lease, gotLease)

	assert.Same(t, w, s.currentWill(), "a Reauthorize between the connection's end and the timer must find the Will")
	assert.Same(t, lease, s.discardWill(w), "and discarding it returns the stored record to remove")
	assert.False(t, s.takePendingWill(w), "so the timer finds it gone and publishes nothing")
}

func TestAWillOfAnotherConnectionIsNotTakenForPublication(t *testing.T) {
	s := &session{}
	s.setWill(&conn{}, &packet.Will{Topic: "t"}, 0, &willLease{})
	w, _, _ := s.takeWillToPublish(&conn{})
	assert.Nil(t, w)
	assert.NotNil(t, s.currentWill())
}
