package natsmqtt5

import (
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
)

// The session watcher can hand a broker an old write of a record late, after
// the broker has claimed the record itself (a consumer recreated on a loaded
// server reads it again). That is not another broker taking the session
// [MQTT-3.1.4-3]: acting on it disconnected the client that had just arrived
// and dropped its subscriptions, so a message published next was never
// delivered (TestPersistentSessionMovesToAnotherLiveBroker on CI). Only a write
// newer than the broker's own claim is a loss.
func TestAStaleWriteOfTheSessionRecordIsNotATakeover(t *testing.T) {
	b := &Broker{
		sessions: map[string]*session{},
		logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	s := newSession("roamer")
	s.bindRecord(&sessionRecord{ClientID: "roamer"}, 10)
	b.sessions["roamer"] = s

	b.sessionLost("roamer", 4)
	b.sessionLost("roamer", 10)
	assert.Contains(t, b.sessions, "roamer", "a write at or before our own claim is history")

	b.sessionLost("roamer", 11)
	assert.NotContains(t, b.sessions, "roamer", "a write after our claim is another broker's")
}
