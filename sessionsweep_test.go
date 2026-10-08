package natsmqtt5_test

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// The detached-session sweep. A session whose client disconnected with a
// non-zero Session Expiry Interval is kept for that long [MQTT-3.1.2-23], and
// not longer than the broker needs to: after it, the session's NATS
// subscriptions must go. These tests run a real embedded nats-server with
// JetStream, a broker, and raw MQTT over TCP, and read what is left on the
// server through its monitoring data (Subsz), which is the only place a
// leaked subscription shows.
//
// The statement ids are cited without quoting: the specification is not in this
// repository (see CONTRIBUTING.md), so the wording is unverified here. What the
// tests assert is the observable consequence the CONFORMANCE.md row names: a
// session is present for a client that returns inside the interval, and is not
// for one that returns after it [MQTT-3.2.2-3].

const sweepEvery = 50 * time.Millisecond

// subjectsOf lists the subjects of the subscriptions the NATS server holds
// whose subject contains marker, with the queue group (empty if none) after a
// space. The broker's own subscriptions never contain a test's marker.
func subjectsOf(t *testing.T, srv *natsserver.Server, marker string) []string {
	t.Helper()
	sz, err := srv.Subsz(&natsserver.SubszOptions{Subscriptions: true, Limit: 100000})
	require.NoError(t, err)
	var found []string
	for _, d := range sz.Subs {
		if strings.Contains(d.Subject, marker) {
			found = append(found, d.Subject+" "+d.Queue)
		}
	}
	return found
}

func sweeping(o *natsmqtt5.Options) { o.SessionSweepInterval = sweepEvery }

func detach(c *rawClient) {
	c.send(&packet.Disconnect{})
	time.Sleep(100 * time.Millisecond)
}

// A session past its interval loses its NATS subscriptions, plain and shared,
// and a client that returns is told there is no session [MQTT-3.1.2-23],
// [MQTT-3.2.2-3]. Before the interval ends the subscriptions are still there:
// the sweep must not take what the statement says to keep.
func TestAnExpiredDetachedSessionLosesItsNATSSubscriptions_MQTT_3_1_2_23(t *testing.T) {
	for name, extra := range map[string]func(*natsmqtt5.Options){
		"in memory":           func(*natsmqtt5.Options) {},
		"persistent sessions": persistent,
	} {
		t.Run(name, func(t *testing.T) {
			srv := startNATSServer(t, 0)
			broker, addr, _ := startBrokerHandle(t, srv.ClientURL(), sweeping, extra)

			var mu sync.Mutex
			expired := map[string]int{}
			natsmqtt5.SetSessionExpiredHook(broker, func(id string, subs int) {
				mu.Lock()
				defer mu.Unlock()
				expired[id] = subs
			})

			c := dialRaw(t, addr)
			require.False(t, c.connect(rawConnect("sweep-gone", 2)).SessionPresent)
			c.subscribe("sweep/plain/#", packet.QoS1)
			c.subscribe("$share/sweepgroup/sweep/shared", packet.QoS1)
			require.NotEmpty(t, subjectsOf(t, srv, "sweep"), "the subscriptions exist while the client is connected")
			detach(c)

			assert.NotEmpty(t, subjectsOf(t, srv, "sweep"),
				"a detached session inside its Session Expiry Interval keeps its subscriptions [MQTT-3.1.2-23]")
			assert.Equal(t, 1, natsmqtt5.SessionCount(broker))

			require.Eventually(t, func() bool { return len(subjectsOf(t, srv, "sweep")) == 0 },
				10*time.Second, 50*time.Millisecond,
				"the sweep must unsubscribe an expired session from NATS; left: %v", subjectsOf(t, srv, "sweep"))
			assert.Equal(t, 0, natsmqtt5.SessionCount(broker), "the session is dropped from memory")

			// The hook runs after the session has left its shared subscription's
			// members, which is a round trip to JetStream after the NATS
			// subscriptions went.
			require.Eventually(t, func() bool {
				mu.Lock()
				defer mu.Unlock()
				return len(expired) == 1
			}, 5*time.Second, 20*time.Millisecond, "the hook is told the session expired")
			mu.Lock()
			assert.Equal(t, map[string]int{"sweep-gone": 2}, expired, "the hook is told which session and how many filters")
			mu.Unlock()

			back := dialRaw(t, addr)
			assert.False(t, back.connect(rawConnect("sweep-gone", 300)).SessionPresent,
				"Session Present 0 for a client returning after the interval [MQTT-3.2.2-3]")
		})
	}
}

// Messages published after the sweep reach nobody: the expired session's
// subscription is gone, so the returning client's fresh session has nothing
// queued or delivered for it.
func TestAnExpiredSessionIsNotDeliveredToAfterTheSweep_MQTT_3_1_2_23(t *testing.T) {
	srv := startNATSServer(t, 0)
	addr := startBroker(t, srv.ClientURL(), sweeping)

	c := dialRaw(t, addr)
	c.connect(rawConnect("sweep-silent", 1))
	c.subscribe("sweep/silent", packet.QoS1)
	detach(c)
	require.Eventually(t, func() bool { return len(subjectsOf(t, srv, "sweep")) == 0 },
		10*time.Second, 50*time.Millisecond)

	pub, _ := connectClient(t, addr, connectOpts("sweep-silent-pub"))
	pub.publish(&paho.Publish{Topic: "sweep/silent", QoS: 1, Payload: []byte("late")})

	back := dialRaw(t, addr)
	require.False(t, back.connect(rawConnect("sweep-silent", 300)).SessionPresent)
	back.expectNothing()
}

// A session that comes back inside its interval is kept, and the interval
// starts again from the next disconnect: the sweep must not act on the first
// disconnect's clock.
func TestAResumedSessionSurvivesTheSweepAndItsClockRestarts_MQTT_3_1_2_23(t *testing.T) {
	srv := startNATSServer(t, 0)
	addr := startBroker(t, srv.ClientURL(), sweeping)
	pub, _ := connectClient(t, addr, connectOpts("sweep-resume-pub"))

	c := dialRaw(t, addr)
	c.connect(rawConnect("sweep-resume", 3))
	c.subscribe("sweep/resume", packet.QoS1)
	detach(c)
	time.Sleep(1500 * time.Millisecond)

	// Back at 1.5 s of a 3 s interval.
	c = dialRaw(t, addr)
	require.True(t, c.connect(rawConnect("sweep-resume", 3)).SessionPresent,
		"inside the interval the session is present [MQTT-3.2.2-3]")

	// Connected past the first interval's end (1.5 s + 2 s > 3 s): a connected
	// session is never swept, and the subscription still works.
	time.Sleep(2 * time.Second)
	pub.publish(&paho.Publish{Topic: "sweep/resume", QoS: 1, Payload: []byte("alive")})
	assert.Equal(t, "alive", string(c.expectPublish().Payload))
	detach(c)

	// A new 3 s interval: 1.5 s later the session is still there, though it is
	// over 5 s since the first disconnect.
	time.Sleep(1500 * time.Millisecond)
	assert.NotEmpty(t, subjectsOf(t, srv, "sweep"))
	c = dialRaw(t, addr)
	assert.True(t, c.connect(rawConnect("sweep-resume", 3)).SessionPresent)
}

// A session with an interval of 0 ends with its connection and is never the
// sweep's to touch; a connected one is not swept however long it has been
// connected. Neither a connected session nor a 0-interval one loses its
// subscriptions to the sweep.
func TestTheSweepDoesNotTouchAConnectedSessionOrAZeroIntervalOne_MQTT_3_1_2_23(t *testing.T) {
	srv := startNATSServer(t, 0)
	broker, addr, _ := startBrokerHandle(t, srv.ClientURL(), sweeping)
	var swept sync.Map
	natsmqtt5.SetSessionExpiredHook(broker, func(id string, _ int) { swept.Store(id, true) })
	pub, _ := connectClient(t, addr, connectOpts("sweep-zero-pub"))

	zero := dialRaw(t, addr)
	zero.connect(rawConnect("sweep-zero", 0))
	zero.subscribe("sweep/zero", packet.QoS1)
	short := dialRaw(t, addr)
	short.connect(rawConnect("sweep-short", 1))
	short.subscribe("sweep/short", packet.QoS1)

	time.Sleep(2 * time.Second) // past the short session's interval, with both connected

	pub.publish(&paho.Publish{Topic: "sweep/zero", QoS: 1, Payload: []byte("z")})
	pub.publish(&paho.Publish{Topic: "sweep/short", QoS: 1, Payload: []byte("s")})
	assert.Equal(t, "z", string(zero.expectPublish().Payload))
	assert.Equal(t, "s", string(short.expectPublish().Payload))

	// The zero-interval session ends with its connection, by the connection's
	// own cleanup; the sweep never sees it.
	detach(zero)
	require.Eventually(t, func() bool {
		for _, s := range subjectsOf(t, srv, "sweep") {
			if strings.Contains(s, "zero") {
				return false
			}
		}
		return true
	}, 5*time.Second, 50*time.Millisecond)
	time.Sleep(3 * sweepEvery)
	_, zeroSwept := swept.Load("sweep-zero")
	_, shortSwept := swept.Load("sweep-short")
	assert.False(t, zeroSwept, "an interval of 0 is not the sweep's")
	assert.False(t, shortSwept, "a connected session is not swept")
}

// MaxSessionExpiry caps the interval a client asked for, the CONNACK says so,
// and the sweep honours the cap rather than the request [MQTT-3.1.2-23].
func TestTheSweepExpiresAtTheCappedIntervalNotTheRequestedOne_MQTT_3_1_2_23(t *testing.T) {
	srv := startNATSServer(t, 0)
	addr := startBroker(t, srv.ClientURL(), sweeping, func(o *natsmqtt5.Options) {
		o.MaxSessionExpiry = 2 * time.Second
	})

	c := dialRaw(t, addr)
	ack := c.connect(rawConnect("sweep-capped", 3600))
	require.NotNil(t, ack.Properties.SessionExpiryInterval, "the CONNACK reports a changed interval")
	require.Equal(t, uint32(2), *ack.Properties.SessionExpiryInterval)
	c.subscribe("sweep/capped", packet.QoS1)
	detach(c)

	require.Eventually(t, func() bool { return len(subjectsOf(t, srv, "sweep")) == 0 },
		10*time.Second, 50*time.Millisecond, "an hour was asked for, two seconds is what the session got")
	back := dialRaw(t, addr)
	assert.False(t, back.connect(rawConnect("sweep-capped", 300)).SessionPresent)
}

// The sweep interval defaults, so a broker built without the option still runs
// one: a Session Expiry Interval of 1 s is dropped in at most a default interval
// more. Checked through the default's value, not by waiting a minute.
func TestTheSessionSweepIntervalDefaultsToAMinute(t *testing.T) {
	assert.Equal(t, time.Minute, natsmqtt5.DefaultSessionSweepInterval)
}
