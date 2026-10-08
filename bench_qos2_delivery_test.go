package natsmqtt5_test

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// BenchmarkQoS2Delivery is the cost of delivering one QoS 2 message to a
// persistent session, the whole exchange (PUBLISH, PUBREC, PUBREL, PUBCOMP) run
// by one client in lockstep, with the publisher's own QoS 2 exchange
// included in the time. "Tick" writes the session record on the checkpoint tick
// only; "Durable" is the default, which also writes it before the PUBLISH and
// before the PUBREL goes out, so a broker killed at either step is resumed
// correctly [MQTT-4.3.3-6]. The difference is what the two writes cost. Run it with
//
//	go test -run '^$' -bench QoS2Delivery -benchtime 3s
func BenchmarkQoS2Delivery(b *testing.B) {
	for _, tc := range []struct {
		name       string
		checkpoint time.Duration
	}{
		{"Durable", time.Second},
		{"Tick", -1},
	} {
		b.Run(tc.name, func(b *testing.B) {
			srv, err := natsserver.NewServer(&natsserver.Options{
				Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: b.TempDir(), NoLog: true, NoSigs: true,
			})
			require.NoError(b, err)
			go srv.Start()
			require.True(b, srv.ReadyForConnections(10*time.Second))
			defer srv.Shutdown()

			br, err := natsmqtt5.New(natsmqtt5.Options{
				NATSURL: srv.ClientURL(), Listen: "127.0.0.1:0", PersistentSessions: true,
				SessionCheckpointInterval: tc.checkpoint,
				Logger:                    slog.New(slog.NewTextHandler(io.Discard, nil)),
			})
			require.NoError(b, err)
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() { _ = br.Serve(ctx); close(done) }()
			defer func() { cancel(); _ = br.Close(); <-done }()

			dial := func(id string) (net.Conn, *bufio.Reader) {
				nc, err := net.Dial("tcp", br.ListenAddr().String())
				require.NoError(b, err)
				r := bufio.NewReader(nc)
				require.NoError(b, packet.Write(nc, &packet.Connect{ClientID: id, KeepAlive: 60,
					Properties: &packet.Properties{SessionExpiryInterval: packet.Uint32(300)}}))
				_, err = packet.Read(r, 0)
				require.NoError(b, err)
				return nc, r
			}
			sub, sr := dial("bench-sub")
			defer sub.Close()
			require.NoError(b, packet.Write(sub, &packet.Subscribe{PacketID: 1,
				Subscriptions: []packet.Subscription{{Filter: "bench/#", QoS: packet.QoS2}}}))
			_, err = packet.Read(sr, 0)
			require.NoError(b, err)
			pub, pr := dial("bench-pub")
			defer pub.Close()

			payload := make([]byte, 128)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				id := uint16(i%65535 + 1)
				require.NoError(b, packet.Write(pub, &packet.Publish{Topic: "bench/q2", QoS: packet.QoS2, PacketID: id, Payload: payload}))
				_, err := packet.Read(pr, 0) // PUBREC
				require.NoError(b, err)
				require.NoError(b, packet.Write(pub, &packet.Pubrel{Ack: packet.Ack{PacketID: id}}))
				_, err = packet.Read(pr, 0) // PUBCOMP
				require.NoError(b, err)
				p, err := packet.Read(sr, 0)
				require.NoError(b, err)
				got, ok := p.(*packet.Publish)
				require.True(b, ok)
				require.NoError(b, packet.Write(sub, &packet.Pubrec{Ack: packet.Ack{PacketID: got.PacketID}}))
				_, err = packet.Read(sr, 0)
				require.NoError(b, err)
				require.NoError(b, packet.Write(sub, &packet.Pubcomp{Ack: packet.Ack{PacketID: got.PacketID}}))
			}
		})
	}
}
