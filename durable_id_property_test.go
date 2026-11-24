package natsmqtt5_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

func idProp(value string) *packet.Properties {
	return &packet.Properties{User: []packet.UserProperty{{Key: "msg-id", Value: value}}}
}

func withIDProperty(o *natsmqtt5.Options) { o.DurablePublishIDProperty = "msg-id" }

// A client that chose an id for its publish gets it deduplicated whatever else its retry changes: here
// the first attempt is refused after its queue copy was stored, and the retry has a new packet
// identifier and no DUP flag, which DurablePublishDedupe cannot match [MQTT-4.3.2-2].
func TestDurablePublishIDPropertyDropsARetryUnderANewPacketIDFromTheQueue(t *testing.T) {
	t.Parallel()
	stream, subject := refuseThenRetryWith(t, "dip-pub", "dip/retry", withIDProperty,
		func(pub *rawClient) {
			pub.send(&packet.Publish{Topic: "dip/retry", QoS: packet.QoS1, PacketID: 1, Payload: []byte("first"), Properties: idProp("k1")})
		},
		func(pub *rawClient) {
			pub.send(&packet.Publish{Topic: "dip/retry", QoS: packet.QoS1, PacketID: 2, Payload: []byte("first"), Properties: idProp("k1")})
		})
	assert.Equal(t, 1, queuedCopies(t, stream, subject))
}

// A different value is a different message, even with the same payload.
func TestDurablePublishIDPropertyKeepsAMessageWithAnotherValue(t *testing.T) {
	t.Parallel()
	stream, subject := refuseThenRetryWith(t, "dip-other-pub", "dip/other", withIDProperty,
		func(pub *rawClient) {
			pub.send(&packet.Publish{Topic: "dip/other", QoS: packet.QoS1, PacketID: 1, Payload: []byte("first"), Properties: idProp("k1")})
		},
		func(pub *rawClient) {
			pub.send(&packet.Publish{Topic: "dip/other", QoS: packet.QoS1, PacketID: 2, Payload: []byte("first"), Properties: idProp("k2")})
		})
	assert.Equal(t, 2, queuedCopies(t, stream, subject))
}

// Without the property the publish is handled as before: no id of the client's, so no dedupe.
func TestDurablePublishIDPropertyLeavesAPublishWithoutItAlone(t *testing.T) {
	t.Parallel()
	stream, subject := refuseThenRetryWith(t, "dip-none-pub", "dip/none", withIDProperty,
		func(pub *rawClient) { rawPublishQoS(pub, packet.QoS1, 1, "dip/none", "first") },
		func(pub *rawClient) { rawPublishQoS(pub, packet.QoS1, 2, "dip/none", "first") })
	assert.Equal(t, 2, queuedCopies(t, stream, subject))
}

func TestDurablePublishIDPropertyNeedsDurablePublish(t *testing.T) {
	t.Parallel()
	_, err := natsmqtt5.New(natsmqtt5.Options{DurablePublishIDProperty: "msg-id"})
	require.ErrorIs(t, err, natsmqtt5.ErrInvalidOptions)
}
