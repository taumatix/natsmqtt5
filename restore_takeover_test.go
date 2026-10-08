package natsmqtt5_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// A second CONNECT on a Client Identifier takes it over while the first is still
// restoring the session from its stored record [MQTT-3.1.4-3]. The restore is
// stopped in the Authorizer (the call on a resume is as long as the policy
// service makes it, and taking over does not interrupt a connection inside it),
// so the takeover lands between the Authorizer's answer and the subscription
// being installed. Nothing the first connection rebuilt may stay on NATS, and
// the session the second connection gets must still hold what the record
// stored: the CONNACK says Session Present 1 [MQTT-3.2.2-3] and the
// subscriptions are session state that outlives a connection [MQTT-3.1.2-23].
func TestATakeoverInTheMiddleOfAStoredRecordResumeKeepsTheSessionsSubscriptions(t *testing.T) {
	srv := startNATSServer(t, 0)
	addrA, stopA := startStoppableBroker(t, srv.ClientURL(), persistent)

	c := dialRaw(t, addrA)
	require.False(t, c.connect(rawConnect("takeover-restore", 300)).SessionPresent)
	c.subscribe("takeover/one", packet.QoS1)
	c.subscribe("takeover/two", packet.QoS1)
	detach(c)
	stopA()
	require.Eventually(t, func() bool { return len(subjectsOf(t, srv, "takeover")) == 0 },
		5*time.Second, 20*time.Millisecond, "broker A is gone, and took its subscriptions with it")

	// Broker B restores from the record. The first resume Authorizer call is
	// held until the takeover has happened.
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	_, addrB, _ := startBrokerHandle(t, srv.ClientURL(), persistent, func(o *natsmqtt5.Options) {
		o.Authorizer = natsmqtt5.AuthorizerFunc(func(ctx context.Context, req *natsmqtt5.AuthzRequest) error {
			if req.Action == natsmqtt5.ActionSubscribe && req.Resume {
				held := false
				once.Do(func() { close(entered); held = true })
				if held {
					select {
					case <-release:
					case <-ctx.Done():
					}
				}
			}
			return nil
		})
	})

	first := dialRaw(t, addrB)
	first.send(rawConnect("takeover-restore", 300))
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the restore never reached the Authorizer")
	}

	second := dialRaw(t, addrB)
	require.True(t, second.connect(rawConnect("takeover-restore", 300)).SessionPresent,
		"the takeover resumes the session [MQTT-3.2.2-3]")
	close(release)

	// The first connection is told it was taken over, or its socket is closed;
	// either way it is not given a CONNACK that claims the session.
	require.NoError(t, first.nc.SetReadDeadline(time.Now().Add(5*time.Second)))
	if p, err := packet.Read(first.r, 0); err == nil {
		if ack, ok := p.(*packet.Connack); ok {
			assert.True(t, ack.ReasonCode.IsError(), "the displaced restore must not succeed")
		}
	}

	pub, _ := connectClient(t, addrB, connectOpts("takeover-restore-pub"))
	for _, topic := range []string{"takeover/one", "takeover/two"} {
		pub.publish(&paho.Publish{Topic: topic, QoS: 1, Payload: []byte(topic)})
		got := second.expectPublish()
		assert.Equal(t, topic, got.Topic, "the stored filter survived the takeover [MQTT-3.1.2-23]")
		second.send(&packet.Puback{Ack: packet.Ack{PacketID: got.PacketID}})
	}

	// One NATS subscription per filter: what the displaced connection built was
	// torn down.
	assert.Len(t, subjectsOf(t, srv, "takeover.one"), 1)
	assert.Len(t, subjectsOf(t, srv, "takeover.two"), 1)
}
