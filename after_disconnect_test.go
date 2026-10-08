package natsmqtt5_test

import (
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5/packet"
)

// "After sending a DISCONNECT packet the sender MUST NOT send any more MQTT
// Control Packets on that Network Connection" [MQTT-3.14.4-1] and "MUST close
// the Network Connection" [MQTT-3.14.4-3] (OASIS text read 2026-10-08).
//
// A server DISCONNECT is triggered while the broker is busy delivering to the
// same client: a flood of QoS 0 and QoS 1 messages is in flight to the
// subscriber, which then sends a PUBLISH carrying a Subscription Identifier (a
// Protocol Error, so the broker answers DISCONNECT 0x82). Everything the
// subscriber's socket yields after the DISCONNECT's bytes must be the end of
// the stream; a PUBLISH after them is the violation. The window is a race, so
// the scenario repeats.
func TestNothingFollowsAServerDisconnectOnTheWire(t *testing.T) {
	addr := startBroker(t, startNATS(t))
	pub := dialRaw(t, addr)
	pub.connect(rawConnect("flood-pub", 0))

	const rounds, burst = 25, 300
	for round := 0; round < rounds; round++ {
		sub := dialRaw(t, addr)
		cp := rawConnect(fmt.Sprintf("flood-sub-%d", round), 0)
		cp.CleanStart = true
		sub.connect(cp)
		sub.subscribe(fmt.Sprintf("flood/%d", round), packet.QoS1)

		// The flood runs while the subscriber misbehaves.
		stop := make(chan struct{})
		go func() {
			defer close(stop)
			for i := 0; i < burst; i++ {
				_ = packet.Write(pub.nc, &packet.Publish{Topic: fmt.Sprintf("flood/%d", round), QoS: packet.QoS0, Payload: []byte("m")})
			}
		}()
		time.Sleep(time.Millisecond)
		_, err := sub.nc.Write(rawPublish(1, []byte{0x0B, 0x01}))
		require.NoError(t, err)
		<-stop

		// Read to the end of the stream, noting where the DISCONNECT is.
		require.NoError(t, sub.nc.SetReadDeadline(time.Now().Add(10*time.Second)))
		seenDisconnect := false
		for {
			first, err := sub.r.ReadByte()
			if err != nil {
				require.True(t, err == io.EOF || isReset(err), "round %d: the stream ended with %v, not a close", round, err)
				break
			}
			require.False(t, seenDisconnect,
				"round %d: byte 0x%02X followed the DISCONNECT [MQTT-3.14.4-1]", round, first)
			length, shift := 0, 0
			for {
				b, err := sub.r.ReadByte()
				require.NoError(t, err)
				length |= int(b&0x7F) << shift
				if b&0x80 == 0 {
					break
				}
				shift += 7
			}
			body := make([]byte, length)
			_, err = io.ReadFull(sub.r, body)
			require.NoError(t, err)
			if first>>4 == 14 {
				seenDisconnect = true
				require.Equal(t, []byte{0x82}, body[:1], "Protocol Error")
			}
		}
		require.True(t, seenDisconnect, "round %d: the broker sent no DISCONNECT", round)
	}
}

func isReset(err error) bool {
	return strings.Contains(err.Error(), "reset by peer") || strings.Contains(err.Error(), "closed")
}
