package natsmqtt5_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// The three behaviours whose default flips in the next major version (MIGRATION.md) can be
// pinned today with Options.PinDurableWills, PinRestrictDollarTopics and PinOutboundTopicAliases.
// Each test drives a real broker both ways, and the "unset" case proves the default is unchanged.

func pin(b bool) *bool { return &b }

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

func TestPinRestrictDollarTopicsBothWaysAndTheDefaultIsUnchanged(t *testing.T) {
	url := startNATS(t)
	cases := []struct {
		name    string
		opts    func(*natsmqtt5.Options)
		allowed bool
	}{
		{"unset keeps $ topics open", func(*natsmqtt5.Options) {}, true},
		{"pinned false keeps $ topics open", func(o *natsmqtt5.Options) { o.PinRestrictDollarTopics = pin(false) }, true},
		{"pinned true refuses them", func(o *natsmqtt5.Options) { o.PinRestrictDollarTopics = pin(true) }, false},
		{"the old bool still refuses them", func(o *natsmqtt5.Options) { o.RestrictDollarTopics = true }, false},
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

func TestPinOutboundTopicAliasesBothWaysAndTheDefaultIsUnchanged(t *testing.T) {
	url := startNATS(t)
	cases := []struct {
		name string
		opts func(*natsmqtt5.Options)
		sent bool
	}{
		{"unset sends no alias", func(*natsmqtt5.Options) {}, false},
		{"pinned false sends no alias", func(o *natsmqtt5.Options) { o.PinOutboundTopicAliases = pin(false) }, false},
		{"pinned true sends aliases", func(o *natsmqtt5.Options) { o.PinOutboundTopicAliases = pin(true) }, true},
		{"the old bool still sends aliases", withOutboundAliases, true},
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

func TestPinDurableWillsBothWaysAndTheDefaultIsUnchanged(t *testing.T) {
	cases := []struct {
		name   string
		opts   func(*natsmqtt5.Options)
		stored bool
	}{
		{"unset stores no Will", func(*natsmqtt5.Options) {}, false},
		{"pinned false stores no Will", func(o *natsmqtt5.Options) { o.PinDurableWills = pin(false) }, false},
		{"pinned true stores the Will", func(o *natsmqtt5.Options) { o.PinDurableWills = pin(true) }, true},
		{"the old bool still stores it", func(o *natsmqtt5.Options) { o.DurableWills = true }, true},
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

const flipNotice = "default changes in the next major version"

// One INFO line per process, only while a flipping option is left unset.
func TestTheFlipNoticeIsLoggedOncePerProcessAndOnlyForUnsetOptions(t *testing.T) {
	natsmqtt5.ResetFlipNotice(t)
	url := startNATS(t)

	var all syncBuffer
	startBroker(t, url, loggingBroker(t, &all), func(o *natsmqtt5.Options) {
		o.PinDurableWills = pin(false)
		o.PinRestrictDollarTopics = pin(false)
		o.PinOutboundTopicAliases = pin(false)
	})
	assert.NotContains(t, all.String(), flipNotice, "everything pinned: nothing to say")

	var first, second syncBuffer
	startBroker(t, url, loggingBroker(t, &first), func(o *natsmqtt5.Options) { o.PinDurableWills = pin(true) })
	startBroker(t, url, loggingBroker(t, &second))
	assert.Equal(t, 1, strings.Count(first.String(), flipNotice))
	assert.Contains(t, first.String(), "level=INFO")
	assert.Contains(t, first.String(), "PinRestrictDollarTopics")
	assert.Contains(t, first.String(), "PinOutboundTopicAliases")
	assert.NotContains(t, first.String(), "PinDurableWills", "the pinned option is not named")
	assert.Zero(t, strings.Count(second.String(), flipNotice), "once per process")
}
