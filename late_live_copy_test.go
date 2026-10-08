package natsmqtt5_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// A message whose copy is in the offline queue reaches a subscriber's broker
// twice: as the copy the replay reads, and as a live NATS message that may be
// late. Two publishers' copies are stored in one order and their live messages
// can arrive in the other, so a connection that ends owing the later one can be
// followed by the earlier one's live message, which finds nobody to take it.
// The replay for the next connection must still send both, earlier first.
//
//	[MQTT-4.4.0-1] "When a Client reconnects with Clean Start set to 0 and a
//	  session is present, both the Client and Server MUST resend any
//	  unacknowledged PUBLISH packets ..." and the messages the session was
//	  owed, which are delivered in the order they were received
//	  [MQTT-4.6.0-5] "When a Server processes ... QoS 1 and QoS 2 messages
//	  ... MUST send PUBLISH packets in the order in which the corresponding
//	  SUBSCRIBE ... [Ordered Topic]".

// queuedAndLive does what a publishing broker does with one message, in the two
// steps it takes and with the second left to the caller: it stores the offline
// queue's copy, and returns the live message carrying the copy's sequence.
func queuedAndLive(t *testing.T, js jetstream.JetStream, seed *nats.Msg, payload string) *nats.Msg {
	t.Helper()
	header := nats.Header{}
	for k, v := range seed.Header {
		header[k] = append([]string(nil), v...)
	}
	header.Set("Mqtt5-Msg-Id", "late-"+payload)
	header.Del("Mqtt5-Queue-Seq")
	header.Set("Mqtt5-Queued", "1")
	stored := &nats.Msg{
		Subject: strings.Replace(seed.Subject, natsmqtt5.DefaultSubjectPrefix+".", natsmqtt5.DefaultSubjectPrefix+".$queue.", 1),
		Header:  header,
		Data:    []byte(payload),
	}
	ack, err := js.PublishMsg(context.Background(), stored)
	require.NoError(t, err)
	live := &nats.Msg{Subject: seed.Subject, Header: header, Data: []byte(payload)}
	live.Header = nats.Header{}
	for k, v := range header {
		live.Header[k] = append([]string(nil), v...)
	}
	live.Header.Set("Mqtt5-Queue-Seq", uitoa(ack.Sequence))
	return live
}

func uitoa(n uint64) string {
	const digits = "0123456789"
	if n == 0 {
		return "0"
	}
	var b []byte
	for ; n > 0; n /= 10 {
		b = append([]byte{digits[n%10]}, b...)
	}
	return string(b)
}

// [MQTT-4.4.0-1]: the live message of an earlier-stored copy that arrives after
// the connection ended, and after a later-stored copy's did, is still sent to the
// session on resume, ahead of the later one.
func TestALiveCopyThatArrivesAfterTheConnectionEndedIsStillSentOnResume(t *testing.T) {
	natsURL := startNATS(t)
	addr := startBroker(t, natsURL, persistentWithQueue)

	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	spy, err := nc.SubscribeSync(natsmqtt5.DefaultSubjectPrefix + ".lc.>")
	require.NoError(t, err)
	require.NoError(t, nc.Flush())

	// A receive maximum of one: the seed message takes the only slot and stays
	// unacknowledged, so what arrives next waits for the client and is owed.
	c := dialRaw(t, addr)
	cp := rawConnect("late-sub", 300)
	cp.Properties.ReceiveMaximum = packet.Uint16(1)
	c.connect(cp)
	c.subscribe("lc/#", packet.QoS1)
	pub, _ := connectClient(t, addr, connectOpts("late-pub"))
	pub.publish(&paho.Publish{Topic: "lc/x", QoS: 1, Payload: []byte("seed")})
	seed := c.expectPublish()
	seedLive, err := spy.NextMsg(5 * time.Second)
	require.NoError(t, err)

	earlier := queuedAndLive(t, js, seedLive, "earlier")
	later := queuedAndLive(t, js, seedLive, "later")

	// The later copy's live message arrives first and waits for the client.
	require.NoError(t, nc.PublishMsg(later))
	require.NoError(t, nc.Flush())
	time.Sleep(300 * time.Millisecond)
	c.drop()
	time.Sleep(500 * time.Millisecond) // the connection has ended and let go of the session
	require.NoError(t, nc.PublishMsg(earlier))
	require.NoError(t, nc.Flush())
	time.Sleep(300 * time.Millisecond)

	back := dialRaw(t, addr)
	require.True(t, back.connect(rawConnect("late-sub", 300)).SessionPresent)
	var got []string
	for _, p := range back.p3Drain(1500 * time.Millisecond) {
		if pub, ok := p.(*packet.Publish); ok {
			got = append(got, string(pub.Payload))
			back.send(&packet.Puback{Ack: packet.Ack{PacketID: pub.PacketID}})
		}
	}
	assert.Equal(t, []string{"seed", "earlier", "later"}, got,
		"[MQTT-4.4.0-1] every message the session was owed, in the order it was stored (seed was sent as packet %d)", seed.PacketID)
}
