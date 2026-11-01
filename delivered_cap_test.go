package natsmqtt5_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5/v2"
	"github.com/taumatix/natsmqtt5/v2/packet"
)

// The session record lists the QoS 2 ids a client was sent, so a replay on the
// broker that restores the session does not send them again as new
// [MQTT-4.3.3-2, MQTT-4.3.3-6]. The list is bounded (maxStoredDelivered, 4096).
// This drives a client that is further ahead of the one it still owes an
// acknowledgement for than the bound, through a drop and a restore on another
// broker, and records what the bound costs: a warning on release, and a repeat
// of exactly the ids the record could not keep. The bound is lowered for the
// test, since 4100 deliveries take minutes under -race; that it is 4096 in
// production is TestTheStoredIDsAreBounded's.
func TestAClientAheadOfMoreThanTheStoredIDBoundIsWarnedAndRepeatedOnlyPastIt(t *testing.T) {
	const total = 60
	const stored = 20
	natsmqtt5.SetStoredDeliveredBound(t, stored)
	var logs syncBuffer
	natsURL := startNATS(t)
	// A rewind longer than the test, so which ids the record keeps does not
	// depend on how fast the machine delivers.
	longRewind := func(o *natsmqtt5.Options) { o.OfflineQueueRewind = 10 * time.Minute }
	addrA, stopA := startStoppableBroker(t, natsURL, persistentWithQueue, longRewind, loggingBroker(t, &logs))

	c, _ := sessionPresentOnConnect(t, addrA, "q2-cap", false, 300)
	c.subscribe("g/#", packet.QoS2)
	pub := dialRaw(t, addrA)
	pub.connect(rawConnect("pub-q2-cap", 0))

	pub.send(&packet.Publish{Topic: "g/owed", QoS: packet.QoS1, PacketID: 1, Payload: []byte("owed")})
	_, isPuback := pub.read().(*packet.Puback)
	require.True(t, isPuback)
	owed := c.expectPublish()
	require.Equal(t, "owed", string(owed.Payload))

	for sent := 0; sent < total; {
		batch := min(200, total-sent)
		for i := 0; i < batch; i++ {
			pub.send(&packet.Publish{Topic: "g/n", QoS: packet.QoS2, PacketID: uint16(i + 1), Payload: []byte(fmt.Sprint(sent + i))})
		}
		for i := 0; i < batch; i++ {
			rec, ok := pub.read().(*packet.Pubrec)
			require.True(t, ok)
			pub.send(&packet.Pubrel{Ack: packet.Ack{PacketID: rec.PacketID}})
		}
		for i := 0; i < batch; i++ {
			_, ok := pub.read().(*packet.Pubcomp)
			require.True(t, ok)
		}
		for completed := 0; completed < batch; {
			switch p := c.readWithin(10 * time.Second).(type) {
			case *packet.Publish:
				c.send(&packet.Pubrec{Ack: packet.Ack{PacketID: p.PacketID}})
			case *packet.Pubrel:
				c.send(&packet.Pubcomp{Ack: packet.Ack{PacketID: p.PacketID}})
				completed++
			default:
				t.Fatalf("unexpected %T", p)
			}
		}
		sent += batch
	}

	c.drop()
	waitDetached(t, &logs, "q2-cap")
	stopA()
	assert.Contains(t, logs.String(), "keeps only some of the delivered message ids",
		"releasing a session whose delivered ids exceed the bound says so")

	addrB := startBroker(t, natsURL, persistentWithQueue, longRewind, loggingBroker(t, &logs))
	back, present := sessionPresentOnConnect(t, addrB, "q2-cap", false, 300)
	require.True(t, present)

	type sent struct {
		payload string
		dup     bool
		qos     packet.QoS
	}
	var got []sent
	for {
		p, ok := tryRead(back, 2*time.Second)
		if !ok {
			break
		}
		switch p := p.(type) {
		case *packet.Publish:
			got = append(got, sent{string(p.Payload), p.Dup, p.QoS})
			if p.QoS == packet.QoS1 {
				back.send(&packet.Puback{Ack: packet.Ack{PacketID: p.PacketID}})
			} else {
				back.send(&packet.Pubrec{Ack: packet.Ack{PacketID: p.PacketID}})
			}
		case *packet.Pubrel:
			back.send(&packet.Pubcomp{Ack: packet.Ack{PacketID: p.PacketID}})
		default:
			t.Fatalf("unexpected %T on the restored session", p)
		}
	}

	// The record keeps stored ids: the owed message's and the lowest-sequence
	// QoS 2 ones. Everything past them is sent again as new.
	require.Len(t, got, 1+(total+1-stored), "the owed message, then the ids the record could not keep")
	assert.Equal(t, sent{"owed", true, packet.QoS1}, got[0], "the unacknowledged QoS 1 message is resent with DUP 1 [MQTT-4.4.0-1]")
	for i, g := range got[1:] {
		assert.Equal(t, sent{fmt.Sprint(stored - 1 + i), false, packet.QoS2}, g,
			"past the bound a delivered message repeats as new; none below it does")
	}
}

func tryRead(c *rawClient, d time.Duration) (packet.Packet, bool) {
	c.t.Helper()
	require.NoError(c.t, c.nc.SetReadDeadline(time.Now().Add(d)))
	p, err := packet.Read(c.r, 0)
	if err != nil {
		return nil, false
	}
	return p, true
}
