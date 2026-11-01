package natsmqtt5_test

import (
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5/v2/packet"
)

// The read a resume makes of the offline queue, on a three-way replicated stream across three
// real NATS servers, where every read is a request to the stream's leader and every write waits
// for a majority. It logs what it measures; what it asserts is what a client observes: each
// session gets the message it was owed [MQTT-4.4.0-1] ("When a Client reconnects with Clean Start
// set to 0 and a session is present, both the Client and Server MUST resend any unacknowledged
// PUBLISH packets (where QoS > 0) ..."), and the session with a hole gets the hole and nothing it
// completed [MQTT-4.3.3-2].
//
// NATSMQTT5_REPLAY_COST (default 20000) is the number of unrelated messages stored before the
// filtered resume; NATSMQTT5_REPLAY_SESSIONS (default 20) how many sessions resume at once.
func TestClusterReplayReadCost_MQTT_4_4_0_1_MQTT_4_3_3_2(t *testing.T) {
	c := startNATSCluster(t)
	n, _ := strconv.Atoi(os.Getenv("NATSMQTT5_REPLAY_COST"))
	if n <= 0 {
		n = 20000
	}
	sessions, _ := strconv.Atoi(os.Getenv("NATSMQTT5_REPLAY_SESSIONS"))
	if sessions <= 0 {
		sessions = 20
	}

	slowest := measureReplayRead(t, c.URL(0), n, sessions, clusterReplicas)
	t.Logf("replicated queue: %d sessions resuming over %d unrelated messages: slowest %s to its owed message",
		sessions, n, slowest.Round(time.Millisecond))

	// A hole kept below 2000 messages the session completed on its own subject, and one owed
	// message above them: the resume reads the hole by its sequence and nothing of the span
	// between, so the second message is not behind 2000 reads.
	r := newCursorRig(t, c.URL(1), "cost-hole", packet.QoS2, clusterReplicas)
	lost := seqOf(t, r.sendWithheld("lost"))
	const above = 2000
	for i := range above {
		r.send("after" + strconv.Itoa(i))
	}
	awaitHole(t, r.b, r.id, lost)
	r.sendWithheld("last")
	r.dropPastTheRewind()

	back := dialRaw(t, r.addr)
	start := time.Now()
	require.True(t, back.connect(rawConnect(r.id, 300)).SessionPresent)
	var got []string
	var took time.Duration
	require.NoError(t, back.nc.SetReadDeadline(time.Now().Add(30*time.Second)))
	for len(got) < 2 {
		pk, err := packet.Read(back.r, 0)
		require.NoError(t, err)
		switch pk := pk.(type) {
		case *packet.Publish:
			got = append(got, string(pk.Payload))
			took = time.Since(start)
			back.send(&packet.Pubrec{Ack: packet.Ack{PacketID: pk.PacketID}})
		case *packet.Pubrel:
			back.send(&packet.Pubcomp{Ack: packet.Ack{PacketID: pk.PacketID}})
		}
	}
	assert.Equal(t, []string{"lost", "last"}, got, "[MQTT-4.4.0-1] the hole, then the message above the cursor")
	assert.Empty(t, back.completeFor(time.Second), "[MQTT-4.3.3-2] nothing the client completed comes back")
	t.Logf("replicated queue: a resume with a hole below %d completed messages: %s to the message above them",
		above, took.Round(time.Millisecond))
}
