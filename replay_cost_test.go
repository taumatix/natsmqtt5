package natsmqtt5_test

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// The read a resume makes of the offline queue, measured. Each resume replays every queued message stored
// within the rewind, whichever subject, and discards the ones the session did not subscribe to; this puts a
// number on that. Set NATSMQTT5_REPLAY_COST to the number of unrelated messages in the window:
//
//	NATSMQTT5_REPLAY_COST=1000000 go test -run TestReplayReadCost -v -timeout 20m
//
// It reports how long a resume takes to deliver the one message the session was owed, so subtract the run
// with 1 for the fixed cost. NATSMQTT5_REPLAY_SESSIONS (default 1) sets how many sessions resume at once, as
// after a broker restart; the slowest is reported. Skipped unless the first variable is set.
func TestReplayReadCost(t *testing.T) {
	n, _ := strconv.Atoi(os.Getenv("NATSMQTT5_REPLAY_COST"))
	if n <= 0 {
		t.Skip("set NATSMQTT5_REPLAY_COST to a message count to measure")
	}
	natsURL := startNATS(t)
	_, addr, _ := startBrokerHandle(t, natsURL, persistentWithQueue, rewindOf(time.Hour))

	sessions, _ := strconv.Atoi(os.Getenv("NATSMQTT5_REPLAY_SESSIONS"))
	if sessions <= 0 {
		sessions = 1
	}
	for i := 0; i < sessions; i++ {
		c := dialRaw(t, addr)
		c.connect(rawConnect(fmt.Sprintf("cost-sub-%d", i), 3600))
		c.subscribe(fmt.Sprintf("cost/mine/%d", i), packet.QoS1)
		c.drop()
	}
	time.Sleep(300 * time.Millisecond)

	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc, jetstream.WithPublishAsyncMaxPending(4096))
	require.NoError(t, err)
	payload := make([]byte, 128)
	for i := 0; i < n; i++ {
		_, err := js.PublishAsync(fmt.Sprintf("%s.$queue.cost.other.%d", natsmqtt5.DefaultSubjectPrefix, i%1000), payload)
		require.NoError(t, err)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(10 * time.Minute):
		t.Fatal("the filler messages were not stored")
	}

	pub, _ := connectClient(t, addr, connectOpts("cost-pub"))
	for i := 0; i < sessions; i++ {
		pub.publish(&paho.Publish{Topic: fmt.Sprintf("cost/mine/%d", i), QoS: 1, Payload: []byte("owed")})
	}

	took := make([]time.Duration, sessions)
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < sessions; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			back := dialRaw(t, addr)
			if !back.connect(rawConnect(fmt.Sprintf("cost-sub-%d", i), 3600)).SessionPresent {
				t.Errorf("session %d was not present", i)
				return
			}
			_ = back.nc.SetReadDeadline(time.Now().Add(15 * time.Minute))
			for {
				p, err := packet.Read(back.r, 0)
				if err != nil {
					t.Errorf("session %d: %v", i, err)
					return
				}
				if pub, ok := p.(*packet.Publish); ok && string(pub.Payload) == "owed" {
					took[i] = time.Since(start)
					return
				}
			}
		}()
	}
	wg.Wait()
	var slowest time.Duration
	for _, d := range took {
		slowest = max(slowest, d)
	}
	t.Logf("%d sessions resuming over %d unrelated messages: slowest %s to its owed message",
		sessions, n, slowest.Round(time.Millisecond))
}
