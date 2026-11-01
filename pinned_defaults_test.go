package natsmqtt5_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// The three behaviours whose default flipped in v1.0.0 (MIGRATION.md) can be pinned
// either way with Options.PinDurableWills, PinRestrictDollarTopics and PinOutboundTopicAliases;
// a pin is also the only way back to the old behaviour. Each test drives a real broker both ways,
// and the "unset" case proves the new default.

func pin(b bool) *bool { return &b }

// allowDollarTopics is the pre-1.0 behaviour, for tests of what an application using "$app/..." sees.
func allowDollarTopics(o *natsmqtt5.Options) { o.PinRestrictDollarTopics = pin(false) }

// dollarAllowed reports whether a client can subscribe to and publish on "$app/x".
func dollarAllowed(t *testing.T, addr, id string) bool {
	t.Helper()
	c := dialRaw(t, addr)
	c.connect(rawConnect(id, 0))
	sub := c.subscribeCodes("$app/x", packet.QoS1)
	pubCode := c.publishCode("$app/x", packet.QoS1, 1)
	if sub[0] == packet.GrantedQoS1 && pubCode == packet.Success {
		return true
	}
	assert.Equal(t, []packet.ReasonCode{packet.TopicFilterInvalid}, sub)
	assert.Equal(t, packet.TopicNameInvalid, pubCode)
	return false
}

func TestPinRestrictDollarTopicsBothWaysAndTheDefaultIsOn(t *testing.T) {
	url := startNATS(t)
	cases := []struct {
		name    string
		opts    func(*natsmqtt5.Options)
		allowed bool
	}{
		{"unset refuses $ topics", func(*natsmqtt5.Options) {}, false},
		{"pinned false keeps $ topics open (the pre-1.0 behaviour)", func(o *natsmqtt5.Options) { o.PinRestrictDollarTopics = pin(false) }, true},
		{"pinned true refuses them", func(o *natsmqtt5.Options) { o.PinRestrictDollarTopics = pin(true) }, false},
		{"the plain bool, now redundant, still refuses them", func(o *natsmqtt5.Options) { o.RestrictDollarTopics = true }, false},
		{"a pin beats the old bool", func(o *natsmqtt5.Options) {
			o.RestrictDollarTopics = true
			o.PinRestrictDollarTopics = pin(false)
		}, true},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			addr := startBroker(t, url, tc.opts)
			assert.Equal(t, tc.allowed, dollarAllowed(t, addr, "pin-dollar-"+string(rune('a'+i))))
		})
	}
}

// aliasSent reports whether a repeated long topic reaches a subscriber that offered a Topic Alias
// Maximum as an alias.
func aliasSent(t *testing.T, addr, id string) bool {
	t.Helper()
	sub := dialRaw(t, addr)
	sub.connect(aliasConnect(id+"-sub", 5))
	sub.subscribe("alias/#", packet.QoS1)
	pub, _ := connectClient(t, addr, connectOpts(id+"-pub"))
	sent := false
	for range 2 {
		publishQoS1(t, pub, "alias/topic-one")
		sent = aliasOf(sub.expectPublish()) != 0
	}
	return sent
}

func TestPinOutboundTopicAliasesBothWaysAndTheDefaultIsOn(t *testing.T) {
	url := startNATS(t)
	cases := []struct {
		name string
		opts func(*natsmqtt5.Options)
		sent bool
	}{
		{"unset sends aliases", func(*natsmqtt5.Options) {}, true},
		{"pinned false sends no alias (the pre-1.0 behaviour)", func(o *natsmqtt5.Options) { o.PinOutboundTopicAliases = pin(false) }, false},
		{"pinned true sends aliases", func(o *natsmqtt5.Options) { o.PinOutboundTopicAliases = pin(true) }, true},
		{"the plain bool, now redundant, still sends aliases", withOutboundAliases, true},
		{"a pin beats the old bool", func(o *natsmqtt5.Options) {
			o.OutboundTopicAliases = true
			o.PinOutboundTopicAliases = pin(false)
		}, false},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			addr := startBroker(t, url, tc.opts)
			assert.Equal(t, tc.sent, aliasSent(t, addr, "pin-alias-"+string(rune('a'+i))))
		})
	}
}

// willRecords counts Will records in the default Will bucket, and says whether the bucket exists.
func willRecords(t *testing.T, natsURL string) (n int, bucket bool) {
	t.Helper()
	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	kv, err := js.KeyValue(context.Background(), "MQTT5_wills")
	if err != nil {
		return 0, false
	}
	keys, _ := kv.Keys(context.Background())
	for _, k := range keys {
		if strings.Contains(k, ".") {
			n++
		}
	}
	return n, true
}

func TestPinDurableWillsBothWaysAndTheDefaultIsOn(t *testing.T) {
	cases := []struct {
		name   string
		opts   func(*natsmqtt5.Options)
		stored bool
	}{
		{"unset stores the Will", func(*natsmqtt5.Options) {}, true},
		{"pinned false stores no Will (the pre-1.0 behaviour)", func(o *natsmqtt5.Options) { o.PinDurableWills = pin(false) }, false},
		{"pinned true stores the Will", func(o *natsmqtt5.Options) { o.PinDurableWills = pin(true) }, true},
		{"the plain bool, now redundant, still stores it", func(o *natsmqtt5.Options) { o.DurableWills = true }, true},
		{"a pin beats the old bool", func(o *natsmqtt5.Options) {
			o.DurableWills = true
			o.PinDurableWills = pin(false)
		}, false},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			url := startNATS(t)
			addr := startBroker(t, url, tc.opts)
			c := dialRaw(t, addr)
			cp := rawConnect("pin-will-"+string(rune('a'+i)), 0)
			cp.Will = &packet.Will{Topic: "status/x", Payload: []byte("bye")}
			c.connect(cp)
			if tc.stored {
				require.Eventually(t, func() bool { n, _ := willRecords(t, url); return n == 1 }, 5*time.Second, 20*time.Millisecond)
				return
			}
			_, bucket := willRecords(t, url)
			assert.False(t, bucket, "no Will bucket is created")
		})
	}
}

// The default follows JetStream: a broker that uses none (no retained store, sessions or
// offline queue) has no Will bucket unless the pin asks for one.
func TestDurableWillsDefaultFollowsJetStreamUse(t *testing.T) {
	noJetStream := func(o *natsmqtt5.Options) { o.DisableRetained = true }
	cases := []struct {
		name   string
		opts   func(*natsmqtt5.Options)
		bucket bool
	}{
		{"no JetStream user: no bucket", noJetStream, false},
		{"persistent sessions are a JetStream user", func(o *natsmqtt5.Options) {
			noJetStream(o)
			o.PersistentSessions = true
		}, true},
		{"the pin forces JetStream on", func(o *natsmqtt5.Options) {
			noJetStream(o)
			o.PinDurableWills = pin(true)
		}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			url := startNATS(t)
			addr := startBroker(t, url, tc.opts)
			c := dialRaw(t, addr)
			cp := rawConnect("jsfollow", 0)
			cp.Will = &packet.Will{Topic: "status/x", Payload: []byte("bye")}
			c.connect(cp)
			if tc.bucket {
				require.Eventually(t, func() bool { _, ok := willRecords(t, url); return ok }, 5*time.Second, 20*time.Millisecond)
				return
			}
			_, bucket := willRecords(t, url)
			assert.False(t, bucket)
		})
	}
}

// End to end over a real MQTT client (Paho): with nothing set, a client that uses a $ topic is
// refused, and one that sends a Will has it stored durably; pinned false, both behave as before 1.0.
func TestV1DefaultsOverARealMQTTClient(t *testing.T) {
	t.Run("a $ topic is refused by default and allowed when pinned off", func(t *testing.T) {
		url := startNATS(t)
		for _, tc := range []struct {
			name   string
			opts   func(*natsmqtt5.Options)
			sub    byte
			pubErr bool
		}{
			{"default", func(*natsmqtt5.Options) {}, byte(packet.TopicFilterInvalid), true},
			{"pinned false", func(o *natsmqtt5.Options) { o.PinRestrictDollarTopics = pin(false) }, byte(packet.GrantedQoS1), false},
		} {
			addr := startBroker(t, url, tc.opts)
			c, _ := connectClient(t, addr, connectOpts("e2e-dollar-"+tc.name))
			suback, err := c.Subscribe(context.Background(), &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{Topic: "$app/x", QoS: 1}}})
			if tc.pubErr {
				require.Error(t, err, tc.name) // Paho reports a refused subscription as an error
			} else {
				require.NoError(t, err, tc.name)
			}
			require.NotNil(t, suback, tc.name)
			assert.Equal(t, tc.sub, suback.Reasons[0], tc.name)
			puback, err := c.Publish(context.Background(), &paho.Publish{Topic: "$app/x", QoS: 1, Payload: []byte("p")})
			if tc.pubErr {
				if err == nil {
					assert.Equal(t, byte(packet.TopicNameInvalid), puback.ReasonCode, tc.name)
				}
			} else {
				require.NoError(t, err, tc.name)
				assert.Equal(t, byte(packet.Success), puback.ReasonCode, tc.name)
			}
		}
	})

	t.Run("a Will is stored durably by default and not when pinned off", func(t *testing.T) {
		for _, tc := range []struct {
			name   string
			opts   func(*natsmqtt5.Options)
			stored bool
		}{
			{"default", func(*natsmqtt5.Options) {}, true},
			{"pinned false", func(o *natsmqtt5.Options) { o.PinDurableWills = pin(false) }, false},
		} {
			url := startNATS(t)
			addr := startBroker(t, url, tc.opts)
			cp := connectOpts("e2e-will-" + tc.name)
			cp.WillMessage = &paho.WillMessage{Topic: "status/e2e", Payload: []byte("bye"), QoS: 1}
			connectClient(t, addr, cp)
			if tc.stored {
				require.Eventually(t, func() bool { n, _ := willRecords(t, url); return n == 1 }, 5*time.Second, 20*time.Millisecond, tc.name)
				continue
			}
			_, bucket := willRecords(t, url)
			assert.False(t, bucket, tc.name)
		}
	})

	t.Run("a repeated topic is aliased by default and not when pinned off", func(t *testing.T) {
		url := startNATS(t)
		assert.True(t, aliasSent(t, startBroker(t, url), "e2e-alias-default"))
		assert.False(t, aliasSent(t, startBroker(t, url, func(o *natsmqtt5.Options) { o.PinOutboundTopicAliases = pin(false) }), "e2e-alias-off"))
	})
}
