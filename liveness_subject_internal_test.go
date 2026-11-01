package natsmqtt5

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// With QueuePlainPublishes off the liveness subject is the one v1.1.0 used, so a mixed v1.1 and
// v1.2 fleet agrees; with it on, the subject leaves the prefix the plain-publish queue group
// subscribes to (<prefix>.>), so a dead broker still answers "no responders" at once.
func TestLivenessSubjectMovesOnlyWithQueuePlainPublishes(t *testing.T) {
	assert.Equal(t, "mqtt.$broker.abc.ping", brokerPingSubject(livenessBase("mqtt", false), "abc"))
	assert.Equal(t, "mqtt$broker.abc.ping", brokerPingSubject(livenessBase("mqtt", true), "abc"))
}
