package natsmqtt5_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// A connection displaced by a takeover goes on decoding the packets its socket
// had already buffered. v0.4.1 stopped such a packet installing a subscription
// into the session its successor owns; this is the same connection removing
// one. A queued an UNSUBSCRIBE for public/# before B took the Client Identifier
// over and subscribed to public/# itself; when A's backlog drained, the
// UNSUBSCRIBE tore down B's filter and B was never told.
//
// The packets go out in one write so they are certain to be buffered together,
// and the Authorizer holds A's first PUBLISH open while B takes over.
//
// Nothing after the UNSUBSCRIBE can prove it was processed: its UNSUBACK write
// fails on the socket the takeover closed, and that ends A's loop. The removal
// happens before that write, within microseconds of the release, so a fixed
// wait stands in for the proof. That the wait is long enough is shown by the
// test failing 3 of 3 against the unfixed broker.
func TestADisplacedConnectionCannotUnsubscribeItsSuccessor(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var enterOnce sync.Once

	addr := startBroker(t, startNATS(t), func(o *natsmqtt5.Options) {
		o.Authenticator = natsmqtt5.AuthenticatorFunc(func(_ context.Context, req *natsmqtt5.AuthRequest) (*natsmqtt5.AuthResult, error) {
			return &natsmqtt5.AuthResult{Identity: req.Username}, nil
		})
		o.Authorizer = natsmqtt5.AuthorizerFunc(func(ctx context.Context, req *natsmqtt5.AuthzRequest) error {
			switch {
			case req.Action == natsmqtt5.ActionPublish && req.Topic == "hold":
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

	// A, raw, so its backlog can be written in one go.
	a := dialRaw(t, addr)
	cp := rawConnect("shared-unsub", 300)
	cp.Username = "victim"
	// Keep Alive 0, because that is when the race is open. With a non-zero Keep
	// Alive the read loop sets a deadline on the socket before every read, the
	// takeover has closed that socket, and the loop ends there — before
	// decoding anything still buffered. With 0 there is no such call, and the
	// next packet comes straight out of the buffer.
	cp.KeepAlive = 0
	a.connect(cp)

	// The packet held open must be one that writes nothing back when refused:
	// a QoS 0 PUBLISH. A held SUBSCRIBE would fail writing its SUBACK to the
	// socket the takeover closed, and that error ends A's loop before the
	// UNSUBSCRIBE is decoded, so the race this test is about never opens.
	var backlog []byte
	for _, p := range []packet.Packet{
		&packet.Publish{Topic: "hold", Payload: []byte("held")},
		&packet.Unsubscribe{PacketID: 2, Filters: []string{"public/#"}},
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

	bcp := durableConnect("shared-unsub", 300)
	bcp.UsernameFlag, bcp.Username = true, "narrow"
	b, connack := connectClient(t, addr, bcp)
	require.True(t, connack.SessionPresent)
	b.subscribe(paho.SubscribeOptions{Topic: "public/#", QoS: 0})

	close(release)
	time.Sleep(500 * time.Millisecond)

	publisher, _ := connectClient(t, addr, connectOpts("publisher-unsub"))
	publisher.publish(&paho.Publish{Topic: "public/x", Payload: []byte("for narrow")})

	m := b.expectMessage()
	require.Equal(t, "public/x", m.Topic, "B's subscription was removed by the connection it displaced")
}
