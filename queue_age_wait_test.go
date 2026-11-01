package natsmqtt5_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5/v2"
)

// queuedSkewMargin is what awaitQueuedOlderThanTheCheckpointSkew waits for: the broker's checkpoint skew
// (500 ms) and a little more. A checkpoint's replay time is that far before the checkpoint, so a message
// stored longer ago than this is not also replayed by the broker that takes the session over.
const queuedSkewMargin = 600 * time.Millisecond

// awaitQueuedOlderThanTheCheckpointSkew waits until the queue stream's newest copy for the topic was stored
// more than queuedSkewMargin ago, by the stream's own timestamp, rather than sleeping a fixed second after
// publishing. The topic is the published topic name, with "/" as in MQTT.
func awaitQueuedOlderThanTheCheckpointSkew(t *testing.T, natsURL, topicName string) {
	t.Helper()
	stream := queueStream(t, natsURL)
	subject := natsmqtt5.DefaultSubjectPrefix + ".$queue." + strings.ReplaceAll(topicName, "/", ".")
	var stored time.Time
	require.Eventually(t, func() bool {
		raw, err := stream.GetLastMsgForSubject(context.Background(), subject)
		if err != nil {
			return false
		}
		stored = raw.Time
		return true
	}, 10*time.Second, 10*time.Millisecond, "no queue copy for %s", topicName)
	if wait := queuedSkewMargin - time.Since(stored); wait > 0 {
		time.Sleep(wait) // the remainder of an age, which no event ends
	}
}
