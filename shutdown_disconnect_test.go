package natsmqtt5_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// Cancelling Serve's context and then calling Close must still tell every connected client
// why it is being dropped: a DISCONNECT with reason 0x8B, not a bare TCP close. The two
// shutdown paths race, so one run proves little; each round puts several clients on a
// fresh broker and cancels and closes back to back.
func TestShutdownAfterCancelledServeSendsDisconnect(t *testing.T) {
	url := startNATS(t)
	const rounds, clients = 25, 6
	for round := 0; round < rounds; round++ {
		b, err := natsmqtt5.New(natsmqtt5.Options{
			NATSURL: url,
			Listen:  "127.0.0.1:0",
			Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		})
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(context.Background())
		served := make(chan error, 1)
		go func() { served <- b.Serve(ctx) }()

		raws := make([]*rawClient, clients)
		for i := range raws {
			raws[i] = dialRaw(t, b.ListenAddr().String())
			raws[i].connect(rawConnect(fmt.Sprintf("r%d-c%d", round, i), 0))
		}

		cancel()
		require.NoError(t, b.Close())
		require.NoError(t, <-served)

		for i, c := range raws {
			p := c.read()
			d, ok := p.(*packet.Disconnect)
			require.True(t, ok, "round %d client %d: got %T, want a DISCONNECT", round, i, p)
			require.Equal(t, packet.ServerShuttingDown, d.ReasonCode, "round %d client %d", round, i)
		}
	}
}

// Cancelling Serve's context alone, with Close never called, ends the connections too and
// owes the same DISCONNECT.
func TestCancelledServeContextSendsDisconnect(t *testing.T) {
	url := startNATS(t)
	b, err := natsmqtt5.New(natsmqtt5.Options{
		NATSURL: url,
		Listen:  "127.0.0.1:0",
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- b.Serve(ctx) }()

	c := dialRaw(t, b.ListenAddr().String())
	c.connect(rawConnect("cancelled", 0))
	cancel()
	require.NoError(t, <-served)

	d, ok := c.read().(*packet.Disconnect)
	require.True(t, ok, "want a DISCONNECT")
	require.Equal(t, packet.ServerShuttingDown, d.ReasonCode)
}
