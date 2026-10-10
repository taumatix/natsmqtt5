package natsmqtt5_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

func denyOnResume(prefix string) func(*natsmqtt5.Options) {
	return func(o *natsmqtt5.Options) {
		o.Authorizer = natsmqtt5.AuthorizerFunc(func(_ context.Context, req *natsmqtt5.AuthzRequest) error {
			if req.Resume && strings.HasPrefix(req.Topic, prefix) {
				return errors.New("denied")
			}
			return nil
		})
	}
}

func droppedIn(ack *packet.Connack) []string {
	var out []string
	if ack.Properties == nil {
		return out
	}
	for _, u := range ack.Properties.User {
		if u.Key == natsmqtt5.DroppedFilterProperty {
			out = append(out, u.Value)
		}
	}
	return out
}

// The CONNACK of a resume that lost filters to the Authorizer names them, so the
// client can tell Session Present does not mean "everything is still subscribed".
func TestAResumeNamesTheFiltersTheAuthorizerDropped(t *testing.T) {
	t.Run("session held in memory", func(t *testing.T) {
		addr := startBroker(t, startNATS(t), denyOnResume("deny/"))
		r := dialRaw(t, addr)
		r.connect(rawConnect("drop-mem", 300))
		r.subscribe("deny/a", packet.QoS0)
		r.subscribe("deny/b/#", packet.QoS0)
		r.subscribe("ok/a", packet.QoS0)
		r.drop()

		ack := dialRaw(t, addr).connect(rawConnect("drop-mem", 300))
		require.True(t, ack.SessionPresent)
		assert.ElementsMatch(t, []string{"deny/a", "deny/b/#"}, droppedIn(ack))
	})

	t.Run("session restored from the stored record", func(t *testing.T) {
		natsURL := startNATS(t)
		addrA, stopA := startStoppableBroker(t, natsURL, persistent)
		r := dialRaw(t, addrA)
		r.connect(rawConnect("drop-stored", 300))
		r.subscribe("deny/a", packet.QoS0)
		r.subscribe("ok/a", packet.QoS0)
		r.drop()
		stopA()

		addrB := startBroker(t, natsURL, persistent, denyOnResume("deny/"))
		ack := dialRaw(t, addrB).connect(rawConnect("drop-stored", 300))
		require.True(t, ack.SessionPresent)
		assert.Equal(t, []string{"deny/a"}, droppedIn(ack))
	})

	t.Run("nothing dropped, nothing said", func(t *testing.T) {
		addr := startBroker(t, startNATS(t), denyOnResume("deny/"))
		r := dialRaw(t, addr)
		r.connect(rawConnect("drop-none", 300))
		r.subscribe("ok/a", packet.QoS0)
		r.drop()

		ack := dialRaw(t, addr).connect(rawConnect("drop-none", 300))
		require.True(t, ack.SessionPresent)
		assert.Empty(t, droppedIn(ack))
	})

	t.Run("a client that cannot take the CONNACK still connects", func(t *testing.T) {
		addr := startBroker(t, startNATS(t), denyOnResume("deny/"))
		r := dialRaw(t, addr)
		r.connect(rawConnect("drop-small", 300))
		r.subscribe("deny/some/long/filter/name", packet.QoS0)
		r.drop()

		cp := rawConnect("drop-small", 300)
		cp.Properties = &packet.Properties{MaximumPacketSize: packet.Uint32(16)}
		ack := dialRaw(t, addr).connect(cp)
		require.True(t, ack.SessionPresent)
		assert.Empty(t, droppedIn(ack), "left out rather than failing the connection")
	})
}
