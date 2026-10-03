package natsmqtt5_test

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// An acknowledgement still buffered on a connection that has been taken over is
// applied. The session belongs to the Client Identifier, not to one network
// connection (MQTT-5.0 §4.1), and the client that sent it did receive the
// message: v0.4.2 tried discarding such acknowledgements and the message was
// then resent to a client that had already settled it.
//
// What has to hold alongside that decision is the successor. By the time the
// old connection's PUBACK is decoded, the new one may already have resent the
// message [MQTT-4.4.0-1], and its client will acknowledge that copy too. That
// second PUBACK names an identifier the session no longer holds, and must be
// ignored rather than answered with 0x82 Protocol Error; and the send-quota
// slot the resend took on the new connection must come back to it, not to the
// old one. The new connection advertises a Receive Maximum of 1, so a slot lost
// there stops all delivery.
func TestADisplacedConnectionsAckOfAResentMessageDoesNotCostItsSuccessor(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var enterOnce sync.Once

	natsURL := startNATS(t)
	addr := startBroker(t, natsURL, func(o *natsmqtt5.Options) {
		o.Authorizer = natsmqtt5.AuthorizerFunc(func(ctx context.Context, req *natsmqtt5.AuthzRequest) error {
			if req.Action == natsmqtt5.ActionPublish && req.Topic == "hold" {
				enterOnce.Do(func() { close(entered) })
				select {
				case <-release:
				case <-ctx.Done():
				}
				return errors.New("held only to keep A busy")
			}
			return nil
		})
	})

	a := dialRaw(t, addr)
	cp := rawConnect("acked-twice", 300)
	// Keep Alive 0 so that A goes on decoding its buffered packets after the
	// takeover closes its socket; see TestADisplacedConnectionCannotUnsubscribeItsSuccessor.
	cp.KeepAlive = 0
	a.connect(cp)
	a.subscribe("acked/x", packet.QoS1)

	pub, _ := connectClient(t, addr, connectOpts("pub-acked"))
	pub.publish(&paho.Publish{Topic: "acked/x", QoS: 1, Payload: []byte("first")})
	first := a.expectPublish()

	// A's PUBACK goes out behind a PUBLISH the Authorizer holds, in one write.
	var backlog []byte
	for _, p := range []packet.Packet{
		&packet.Publish{Topic: "hold", Payload: []byte("held")},
		&packet.Puback{Ack: packet.Ack{PacketID: first.PacketID}},
	} {
		b, err := packet.Encode(p)
		require.NoError(t, err)
		backlog = append(backlog, b...)
	}
	_, err := a.nc.Write(backlog)
	require.NoError(t, err)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("A's held PUBLISH never reached the Authorizer")
	}

	b := dialRaw(t, addr)
	bcp := rawConnect("acked-twice", 300)
	bcp.Properties.ReceiveMaximum = natsmqtt5.Ptr(uint16(1))
	connack := b.connect(bcp)
	require.True(t, connack.SessionPresent)
	resent := b.expectPublish()
	require.Equal(t, first.PacketID, resent.PacketID)
	require.True(t, resent.Dup)

	// A's PUBACK is applied now, completing the exchange B was resent.
	close(release)
	time.Sleep(500 * time.Millisecond)

	// B's client acknowledges the copy it was sent, as it must.
	b.send(&packet.Puback{Ack: packet.Ack{PacketID: resent.PacketID}})

	pub.publish(&paho.Publish{Topic: "acked/x", QoS: 1, Payload: []byte("second")})
	require.NoError(t, b.nc.SetReadDeadline(time.Now().Add(5*time.Second)))
	p, err := packet.Read(b.r, 0)
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		t.Fatal("B received nothing more: its only send-quota slot was not returned")
	}
	require.NoError(t, err)
	if d, ok := p.(*packet.Disconnect); ok {
		t.Fatalf("B was disconnected with 0x%02X for acknowledging the message it was resent", byte(d.ReasonCode))
	}
	second, ok := p.(*packet.Publish)
	require.True(t, ok, "expected a PUBLISH, got %s", p.Type())
	assert.Equal(t, "second", string(second.Payload))
}
