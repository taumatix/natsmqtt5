package natsmqtt5_test

import (
	"bufio"
	"context"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// BenchmarkQoS2DeliveryBurst is BenchmarkQoS2Delivery with the publisher ahead
// of the subscriber: 32 messages are published (QoS 2, pipelined) before
// the subscriber reads any, so they wait together in the connection's queue and
// share the records written for their QoS 2 delivery. The time is per message.
//
//	go test -run '^$' -bench QoS2DeliveryBurst -benchtime 3s
func BenchmarkQoS2DeliveryBurst(b *testing.B) {
	b.Run("Durable", func(b *testing.B) { benchQoS2Burst(b, time.Second) })
	b.Run("Tick", func(b *testing.B) { benchQoS2Burst(b, -1) })
}

func benchQoS2Burst(b *testing.B, checkpoint time.Duration) {
	const burst = 32
	srv, err := natsserver.NewServer(&natsserver.Options{
		Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: b.TempDir(), NoLog: true, NoSigs: true,
	})
	require.NoError(b, err)
	go srv.Start()
	require.True(b, srv.ReadyForConnections(10*time.Second))
	defer srv.Shutdown()

	br, err := natsmqtt5.New(natsmqtt5.Options{
		NATSURL: srv.ClientURL(), Listen: "127.0.0.1:0", PersistentSessions: true,
		SessionCheckpointInterval: checkpoint,
		Logger:                    slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
	})
	require.NoError(b, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = br.Serve(ctx); close(done) }()
	defer func() { cancel(); _ = br.Close(); <-done }()

	dial := func(id string, expiry uint32) (net.Conn, *bufio.Reader) {
		nc, err := net.Dial("tcp", br.ListenAddr().String())
		require.NoError(b, err)
		r := bufio.NewReader(nc)
		require.NoError(b, packet.Write(nc, &packet.Connect{ClientID: id, KeepAlive: 60,
			Properties: &packet.Properties{SessionExpiryInterval: packet.Uint32(expiry)}}))
		_, err = packet.Read(r, 0)
		require.NoError(b, err)
		return nc, r
	}
	sub, sr := dial("bench-sub", 300)
	defer sub.Close()
	require.NoError(b, packet.Write(sub, &packet.Subscribe{PacketID: 1,
		Subscriptions: []packet.Subscription{{Filter: "bench/#", QoS: packet.QoS2}}}))
	_, err = packet.Read(sr, 0)
	require.NoError(b, err)
	pub, pr := dial("bench-pub", 0)
	defer pub.Close()

	payload := make([]byte, 128)
	b.ResetTimer()
	for i := 0; i < b.N; i += burst {
		for k := 0; k < burst; k++ {
			require.NoError(b, packet.Write(pub, &packet.Publish{Topic: "bench/q2", QoS: packet.QoS2, PacketID: uint16(k + 1), Payload: payload}))
		}
		for k := 0; k < burst; k++ {
			_, err := packet.Read(pr, 0) // PUBREC
			require.NoError(b, err)
		}
		for k := 0; k < burst; k++ {
			require.NoError(b, packet.Write(pub, &packet.Pubrel{Ack: packet.Ack{PacketID: uint16(k + 1)}}))
		}
		for k := 0; k < burst; k++ {
			_, err := packet.Read(pr, 0) // PUBCOMP
			require.NoError(b, err)
		}
		var ids []uint16
		for len(ids) < burst { // PUBLISHes and PUBRELs interleave
			p, err := packet.Read(sr, 0)
			require.NoError(b, err)
			switch got := p.(type) {
			case *packet.Publish:
				require.NoError(b, packet.Write(sub, &packet.Pubrec{Ack: packet.Ack{PacketID: got.PacketID}}))
			case *packet.Pubrel:
				ids = append(ids, got.PacketID)
			default:
				b.Fatalf("unexpected %T", p)
			}
		}
		for _, id := range ids {
			require.NoError(b, packet.Write(sub, &packet.Pubcomp{Ack: packet.Ack{PacketID: id}}))
		}
	}
}
