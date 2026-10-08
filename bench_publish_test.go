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

// BenchmarkQoS1Publish is the cost of one QoS 1 PUBLISH, sent and waited for
// its PUBACK by a single client over TCP, against an embedded NATS server whose
// JetStream stores to a file (the queue's default storage). The three cases are
// the broker without the offline queue, with it (the default), and with
// DurablePublish. The README quotes the numbers; run it with
//
//	go test -run '^$' -bench QoS1Publish -benchtime 3s
//
// One client in lockstep measures latency, not throughput: a PUBACK per round
// trip, so ns/op is the time a publisher waits.
func BenchmarkQoS1Publish(b *testing.B) {
	cases := []struct {
		name string
		set  func(*natsmqtt5.Options)
	}{
		{"NoQueue", func(o *natsmqtt5.Options) { o.DisableOfflineQueue = true }},
		{"Queue", func(o *natsmqtt5.Options) {}},
		{"DurablePublish", func(o *natsmqtt5.Options) { o.DurablePublish = true }},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			srv, err := natsserver.NewServer(&natsserver.Options{
				Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: b.TempDir(), NoLog: true, NoSigs: true,
			})
			require.NoError(b, err)
			go srv.Start()
			require.True(b, srv.ReadyForConnections(10*time.Second))
			defer srv.Shutdown()

			opts := natsmqtt5.Options{
				NATSURL: srv.ClientURL(), Listen: "127.0.0.1:0",
				Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
			}
			tc.set(&opts)
			br, err := natsmqtt5.New(opts)
			require.NoError(b, err)
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() { _ = br.Serve(ctx); close(done) }()
			defer func() { cancel(); _ = br.Close(); <-done }()

			nc, err := net.Dial("tcp", br.ListenAddr().String())
			require.NoError(b, err)
			defer nc.Close()
			r := bufio.NewReader(nc)
			require.NoError(b, packet.Write(nc, &packet.Connect{ClientID: "bench", KeepAlive: 60}))
			_, err = packet.Read(r, 0)
			require.NoError(b, err)

			payload := make([]byte, 128)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				id := uint16(i%65535 + 1)
				require.NoError(b, packet.Write(nc, &packet.Publish{Topic: "bench/q1", QoS: packet.QoS1, PacketID: id, Payload: payload}))
				p, err := packet.Read(r, 0)
				require.NoError(b, err)
				if ack, ok := p.(*packet.Puback); !ok || ack.ReasonCode != packet.Success {
					b.Fatalf("expected a successful PUBACK, got %+v", p)
				}
			}
		})
	}
}
