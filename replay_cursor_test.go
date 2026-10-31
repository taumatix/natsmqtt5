package natsmqtt5_test

import (
	"context"
	"errors"
	"net"
	"strconv"
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

// The replay cursor (cursor.go) against a real NATS server and real sockets.
// The queue copy of a message is stored before its live copy is published; a
// publisher that stops in between, or a live copy delayed past the session's
// release, leaves a message the session was owed that no live copy brings. A
// replay that starts at a time (Options.OfflineQueueRewind, here 200 ms)
// finds it only if it was stored within the rewind; one that starts at the
// cursor finds it however long ago, and still sends nothing twice.
//
//	[MQTT-4.4.0-1] "When a Client reconnects with Clean Start set to 0 and a
//	  session is present, both the Client and Server MUST resend any
//	  unacknowledged PUBLISH packets (where QoS > 0) and PUBREL packets using
//	  their original Packet Identifiers."
//	[MQTT-3.1.2-23] "The Client and Server MUST store the Session State after
//	  the Network Connection is closed if the Session Expiry Interval is
//	  greater than 0" — what the session was owed is Session State.
//	[MQTT-4.3.3-2] the QoS 2 sender "MUST send a PUBLISH packet containing this
//	  Packet Identifier with QoS 2 and DUP flag set to 0" once per new
//	  Application Message: a QoS 2 message the client completed must not come
//	  back as a new one.
//	[MQTT-4.3.3-6] the sender "MUST NOT re-send the PUBLISH once it has sent
//	  the corresponding PUBREL packet".
//
// The live copies are withheld by natsmqtt5.SetLiveCopyGate, which stands in
// for a publisher killed or stalled between its two writes: the broker stores
// the queue copy and publishes nothing live.

const cursorRewind = 200 * time.Millisecond

// cursorRig is a subscriber that completes everything it is sent, and a
// publisher, on one broker.
type cursorRig struct {
	t       *testing.T
	natsURL string
	b       *natsmqtt5.Broker
	addr    string
	logs    *syncBuffer
	id      string
	topic   string
	qos     packet.QoS
	sub     *rawClient
	pub     *testClient
}

func newCursorRig(t *testing.T, natsURL, id string, qos packet.QoS, customise ...func(*natsmqtt5.Options)) *cursorRig {
	t.Helper()
	r := &cursorRig{t: t, natsURL: natsURL, id: id, topic: id + "/x", qos: qos, logs: &syncBuffer{}}
	opts := append([]func(*natsmqtt5.Options){persistentWithQueue, rewindOf(cursorRewind), loggingBroker(t, r.logs)}, customise...)
	r.b, r.addr, _ = startBrokerHandle(t, natsURL, opts...)
	r.sub = dialRaw(t, r.addr)
	r.sub.connect(rawConnect(id, 300))
	r.sub.subscribe(id+"/#", qos)
	r.pub, _ = connectClient(t, r.addr, connectOpts(id+"-pub"))
	return r
}

// send publishes payload and has the subscriber receive and complete it.
func (r *cursorRig) send(payload string) {
	r.t.Helper()
	r.sendOn(r.topic, payload)
}

func (r *cursorRig) sendOn(topic, payload string) {
	r.t.Helper()
	r.pub.publish(&paho.Publish{Topic: topic, QoS: byte(r.qos), Payload: []byte(payload)})
	completeOne(r.t, r.sub, payload)
}

// sendWithheld publishes payload with its live copy left out, and returns the
// live copy the broker would have published, which carries its queue sequence.
func (r *cursorRig) sendWithheld(payload string) *nats.Msg {
	r.t.Helper()
	held := make(chan *nats.Msg, 1)
	natsmqtt5.SetLiveCopyGate(r.b, func(m *nats.Msg) bool {
		if string(m.Data) != payload {
			return true
		}
		cp := &nats.Msg{Subject: m.Subject, Header: nats.Header{}, Data: append([]byte(nil), m.Data...)}
		for k, v := range m.Header {
			cp.Header[k] = append([]string(nil), v...)
		}
		held <- cp
		return false
	})
	defer natsmqtt5.SetLiveCopyGate(r.b, nil)
	r.pub.publish(&paho.Publish{Topic: r.topic, QoS: byte(r.qos), Payload: []byte(payload)})
	select {
	case m := <-held:
		return m
	case <-time.After(5 * time.Second):
		r.t.Fatal("the publish never reached the live-copy gate")
		return nil
	}
}

// dropPastTheRewind waits until the withheld copy is older than the rewind
// reaches, then drops the subscriber's network connection and waits for the
// broker to let go of the session.
func (r *cursorRig) dropPastTheRewind() {
	r.t.Helper()
	time.Sleep(3 * cursorRewind)
	r.sub.drop()
	waitDetached(r.t, r.logs, r.id)
}

// resume connects back as the rig's client on addr and returns what it is
// sent, completing each; a payload sent with DUP set is marked.
func (r *cursorRig) resume(addr string) []string {
	r.t.Helper()
	back := dialRaw(r.t, addr)
	require.True(r.t, back.connect(rawConnect(r.id, 300)).SessionPresent)
	return back.completeFor(time.Second)
}

// completeOne reads one PUBLISH, which must carry payload, and completes its
// QoS 1 or QoS 2 exchange.
func completeOne(t *testing.T, c *rawClient, payload string) {
	t.Helper()
	p := c.expectPublish()
	require.Equal(t, payload, string(p.Payload))
	switch p.QoS {
	case packet.QoS1:
		c.send(&packet.Puback{Ack: packet.Ack{PacketID: p.PacketID}})
	case packet.QoS2:
		c.send(&packet.Pubrec{Ack: packet.Ack{PacketID: p.PacketID}})
		rel := c.expectPubrel()
		c.send(&packet.Pubcomp{Ack: packet.Ack{PacketID: rel.PacketID}})
	}
}

// completeFor completes every exchange the broker starts within window and
// returns the payloads, a resent one marked "(dup)".
func (c *rawClient) completeFor(window time.Duration) []string {
	c.t.Helper()
	var got []string
	deadline := time.Now().Add(window)
	for {
		require.NoError(c.t, c.nc.SetReadDeadline(deadline))
		p, err := packet.Read(c.r, 0)
		if err != nil {
			var ne net.Error
			require.True(c.t, errors.As(err, &ne) && ne.Timeout(), "reading: %v", err)
			return got
		}
		switch p := p.(type) {
		case *packet.Publish:
			label := string(p.Payload)
			if p.Dup {
				label += "(dup)"
			}
			got = append(got, label)
			switch p.QoS {
			case packet.QoS1:
				c.send(&packet.Puback{Ack: packet.Ack{PacketID: p.PacketID}})
			case packet.QoS2:
				c.send(&packet.Pubrec{Ack: packet.Ack{PacketID: p.PacketID}})
			}
		case *packet.Pubrel:
			c.send(&packet.Pubcomp{Ack: packet.Ack{PacketID: p.PacketID}})
		}
	}
}

func seqOf(t *testing.T, live *nats.Msg) uint64 {
	t.Helper()
	seq, err := strconv.ParseUint(live.Header.Get("Mqtt5-Queue-Seq"), 10, 64)
	require.NoError(t, err)
	return seq
}

// awaitHole waits for a scan to have moved the session's cursor past seq on b,
// leaving seq as a hole.
func awaitHole(t *testing.T, b *natsmqtt5.Broker, clientID string, seq uint64) {
	t.Helper()
	require.Eventually(t, func() bool {
		next, holes := natsmqtt5.ReplayCursor(b, clientID)
		return next > seq && len(holes) == 1 && holes[0] == seq
	}, 5*time.Second, 20*time.Millisecond, "a scan moves the cursor past the withheld copy and keeps it as the one hole")
}

// The message whose live copy never came was stored three rewinds before the
// connection ended; the session is resumed on the broker that held it.
func TestACopyWhoseLiveCopyNeverCameIsReplayedFromTheCursor_MQTT_4_4_0_1(t *testing.T) {
	natsURL := startNATS(t)
	r := newCursorRig(t, natsURL, "cur-mem", packet.QoS1)

	r.send("before")
	r.sendWithheld("lost")
	r.send("after")
	r.dropPastTheRewind()

	assert.Equal(t, []string{"lost"}, r.resume(r.addr),
		"[MQTT-4.4.0-1] the session was owed it; the rewind does not reach it and the cursor does")
}

// The same moved to another broker, at QoS 2, with scans running after every
// delivery: the cursor has passed the message and keeps it as a hole, and the
// record carries both. The other broker replays the hole and nothing the client
// completed [MQTT-4.3.3-2], [MQTT-4.3.3-6].
func TestASessionMovedToAnotherBrokerGetsTheHoleAndNothingItCompleted_MQTT_4_4_0_1_MQTT_4_3_3_2(t *testing.T) {
	natsmqtt5.SetCursorScanAfter(t, 1)
	natsURL := startNATS(t)
	r := newCursorRig(t, natsURL, "cur-moved", packet.QoS2)
	addrB := startBroker(t, natsURL, persistentWithQueue, rewindOf(cursorRewind))

	for i := range 5 {
		r.send("before" + strconv.Itoa(i))
	}
	lost := seqOf(t, r.sendWithheld("lost"))
	for i := range 5 {
		r.send("after" + strconv.Itoa(i))
	}
	awaitHole(t, r.b, r.id, lost)
	r.dropPastTheRewind()

	rec := storedSession(t, natsURL, r.id)
	assert.Greater(t, rec["ReplayCursor"], float64(lost), "the record is the new format: it carries the cursor")
	require.Len(t, rec["ReplayHoles"], 1)
	assert.Equal(t, float64(lost), rec["ReplayHoles"].([]any)[0].(map[string]any)["Seq"])

	assert.Equal(t, []string{"lost"}, r.resume(addrB),
		"[MQTT-4.4.0-1] the hole is replayed; [MQTT-4.3.3-2] no completed QoS 2 message comes back as new")
}

// A broker killed outright leaves the record its last checkpoint wrote, with the
// cursor and the hole; the broker that claims the session replays from it. The
// checkpoint runs every 2 ms so that one usually falls between the client's last
// acknowledgement and the scan that makes the hole: a cursor change must count as
// activity for the next checkpoint, or the record never gets the hole (seen on
// macOS CI).
func TestAKilledBrokersCheckpointCarriesTheCursor_MQTT_4_4_0_1_MQTT_4_3_3_6(t *testing.T) {
	natsmqtt5.SetCursorScanAfter(t, 1)
	natsURL := startNATS(t)
	r := newCursorRig(t, natsURL, "cur-killed", packet.QoS2, func(o *natsmqtt5.Options) {
		o.SessionCheckpointInterval = 2 * time.Millisecond
	})
	addrB := startBroker(t, natsURL, persistentWithQueue, rewindOf(cursorRewind))

	r.send("before")
	lost := seqOf(t, r.sendWithheld("lost"))
	r.send("after")
	awaitHole(t, r.b, r.id, lost)
	time.Sleep(3 * cursorRewind)
	require.Eventually(t, func() bool {
		rec := storedSession(t, natsURL, r.id)
		holes, _ := rec["ReplayHoles"].([]any)
		return rec["Attached"] == true && len(holes) == 1 && rec["Inflight"] == nil
	}, 5*time.Second, 20*time.Millisecond, "a checkpoint with the hole and nothing in flight")
	natsmqtt5.Kill(r.b)
	t.Cleanup(func() { natsmqtt5.Reap(r.b) })

	assert.Equal(t, []string{"lost"}, r.resume(addrB),
		"[MQTT-4.4.0-1] the dead broker's hole is replayed; [MQTT-4.3.3-6] nothing it completed is sent again")
}

// A live copy that reaches the broker after the session was released, below
// more than 8192 later sequences the session delivered past: the rule without a
// cursor (deliveredSeqWindow) refused to note it, and it was lost.
func TestALateCopyBelowMoreThan8192LaterSequencesIsStillReplayed_MQTT_4_4_0_1(t *testing.T) {
	natsURL := startNATS(t)
	r := newCursorRig(t, natsURL, "cur-late", packet.QoS1)

	r.send("before")
	late := r.sendWithheld("late")
	fillQueue(t, natsURL, 8300)
	r.send("after")
	assert.Greater(t, queueLastSeq(t, natsURL), seqOf(t, late)+8192)
	r.dropPastTheRewind()
	before := natsmqtt5.LiveCopies(r.b)
	publishLate(t, natsURL, late)
	awaitLiveCopies(t, r.b, before, 1)

	assert.Equal(t, []string{"late"}, r.resume(r.addr),
		"[MQTT-4.4.0-1] a copy delayed past 8192 later sequences and past the rewind is still owed")
}

// The same with the cursor already past the message (a scan made it a hole),
// and the session restored on another broker from the record the first broker
// rewrote when the late copy reached it.
func TestALateCopyBelowMoreThan8192LaterSequencesReachesASessionRestoredElsewhere_MQTT_4_4_0_1(t *testing.T) {
	natsmqtt5.SetCursorScanAfter(t, 1)
	natsURL := startNATS(t)
	r := newCursorRig(t, natsURL, "cur-late-moved", packet.QoS2)
	addrB := startBroker(t, natsURL, persistentWithQueue, rewindOf(cursorRewind))

	r.send("before")
	late := r.sendWithheld("late")
	fillQueue(t, natsURL, 8300)
	r.send("after")
	awaitHole(t, r.b, r.id, seqOf(t, late))
	r.dropPastTheRewind()
	before := natsmqtt5.LiveCopies(r.b)
	publishLate(t, natsURL, late)
	awaitLiveCopies(t, r.b, before, 1)

	assert.Equal(t, []string{"late"}, r.resume(addrB),
		"[MQTT-4.4.0-1] the late copy is replayed once; [MQTT-4.3.3-2] nothing completed comes back")
}

// A message stored before the session subscribed to a filter that matches it
// was never owed through that filter. A scan must pass it rather than keep it
// as a hole, which would send it on the next resume, and the cursor must still
// find the copy that was owed.
func TestAMessageStoredBeforeItsSubscriptionIsNotOwedAndDoesNotHoldTheCursor_MQTT_4_4_0_1(t *testing.T) {
	natsmqtt5.SetCursorScanAfter(t, 1)
	natsURL := startNATS(t)
	r := newCursorRig(t, natsURL, "cur-since", packet.QoS1)

	r.send("a0")
	r.pub.publish(&paho.Publish{Topic: "cur-since-b/x", QoS: 1, Payload: []byte("stored-before-subscribe")})
	r.sub.subscribe("cur-since-b/#", packet.QoS1)
	r.sendOn("cur-since-b/x", "b1")
	lost := seqOf(t, r.sendWithheld("lost"))
	r.send("a1")
	awaitHole(t, r.b, r.id, lost)
	r.dropPastTheRewind()

	assert.Equal(t, []string{"lost"}, r.resume(r.addr),
		"only what was owed: the message stored before its subscription is not sent, and the hole is")
}

// A record written by a broker that predates the cursor (no ReplayCursor,
// ReplayHoles or SinceSeq) is resumed as before, from the time rewind, and
// nothing the client completed is sent again. The replay that reads it to the
// end starts a cursor, so the session's next absence is replayed from it.
func TestARecordWithoutACursorIsResumedAsBeforeAndStartsOne_MQTT_4_4_0_1_MQTT_4_3_3_2(t *testing.T) {
	t.Parallel()
	natsURL := startNATS(t)
	r := newCursorRig(t, natsURL, "cur-old", packet.QoS2)
	var logsB syncBuffer
	bB, addrB, _ := startBrokerHandle(t, natsURL, persistentWithQueue, rewindOf(cursorRewind), loggingBroker(t, &logsB))

	r.send("one")
	r.sub.drop()
	waitDetached(t, r.logs, r.id)
	rewriteRecord(t, natsURL, r.id, func(rec map[string]any) {
		require.Contains(t, rec, "ReplayCursor", "the record was written in the new format")
		delete(rec, "ReplayCursor")
		delete(rec, "ReplayHoles")
		for _, s := range rec["Subscriptions"].([]any) {
			delete(s.(map[string]any), "SinceSeq")
		}
	})

	back := dialRaw(t, addrB)
	require.True(t, back.connect(rawConnect(r.id, 300)).SessionPresent)
	assert.Empty(t, back.completeFor(time.Second), "[MQTT-4.3.3-2] the completed QoS 2 message is not sent again")
	require.Eventually(t, func() bool {
		next, _ := natsmqtt5.ReplayCursor(bB, r.id)
		return next != 0
	}, 5*time.Second, 20*time.Millisecond, "the replay that read the stream to its end started a cursor")

	pubB, _ := connectClient(t, addrB, connectOpts(r.id+"-pubB"))
	r.b, r.sub, r.pub, r.logs = bB, back, pubB, &logsB
	r.send("two")
	r.sendWithheld("lost")
	r.send("three")
	r.dropPastTheRewind()

	assert.Equal(t, []string{"lost"}, r.resume(addrB),
		"[MQTT-4.4.0-1] after an old record, the next absence is replayed from the cursor")
}

// fillQueue stores n messages on a queue subject no session subscribes to,
// straight into the stream: other sessions' traffic, which moves the stream's
// sequence past what any one session is sent.
func fillQueue(t *testing.T, natsURL string, n int) {
	t.Helper()
	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	defer nc.Close()
	// Room for all n in flight: past the default 4000 an async publish waits 200 ms for an
	// acknowledgement and then fails with "stalled", which a loaded macOS runner reached.
	js, err := jetstream.New(nc, jetstream.WithPublishAsyncMaxPending(n))
	require.NoError(t, err)
	subject := natsmqtt5.DefaultSubjectPrefix + ".$queue.cur-filler.x"
	for i := 0; i < n; i++ {
		_, err := js.PublishAsync(subject, []byte("filler"))
		require.NoError(t, err)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(30 * time.Second):
		t.Fatal("the filler was not stored")
	}
}

func queueLastSeq(t *testing.T, natsURL string) uint64 {
	t.Helper()
	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	stream, err := js.Stream(context.Background(), natsmqtt5.DefaultStreamPrefix+"_queue")
	require.NoError(t, err)
	info, err := stream.Info(context.Background())
	require.NoError(t, err)
	return info.State.LastSeq
}

// publishLate publishes a withheld live copy, late.
func publishLate(t *testing.T, natsURL string, live *nats.Msg) {
	t.Helper()
	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	defer nc.Close()
	require.NoError(t, nc.PublishMsg(live))
	require.NoError(t, nc.Flush())
}

// restartAfterADrop leaves r's session connected with a cursor restarted after
// a drop: a withheld message makes a hole past a bound of none, which drops the
// cursor; once that message is older than the rewind, a delivery starts a
// restart scan, which reads from the rewind and finds nothing owed. It returns
// the withheld copy, which neither the rewind nor the restarted cursor reaches.
func restartAfterADrop(t *testing.T, r *cursorRig) *nats.Msg {
	t.Helper()
	natsmqtt5.SetMaxReplayHoles(t, 0)
	r.send("before")
	gone := r.sendWithheld("gone")
	r.send("drops-it")
	require.Eventually(t, func() bool {
		next, _ := natsmqtt5.ReplayCursor(r.b, r.id)
		return next == 0
	}, 5*time.Second, 20*time.Millisecond, "a hole past the bound drops the cursor")
	natsmqtt5.SetMaxReplayHoles(t, 1024)
	time.Sleep(3 * cursorRewind)
	r.send("restarts-it")
	require.Eventually(t, func() bool {
		next, holes := natsmqtt5.ReplayCursor(r.b, r.id)
		return next > seqOf(t, gone) && len(holes) == 0
	}, 5*time.Second, 20*time.Millisecond, "a restart scan starts the cursor again, past the message older than the rewind")
	return gone
}

// A cursor dropped while the client stays connected comes back without a
// resume, and the next absence is replayed from it: a message whose live copy
// never came, stored after the restart and three rewinds before the
// connection ended, is sent, which falling back to the rewind would not do;
// nothing the client completed comes back as new.
func TestACursorDroppedWhileConnectedRestartsAndReplaysTheHole_MQTT_4_4_0_1_MQTT_4_3_3_2(t *testing.T) {
	natsmqtt5.SetCursorScanAfter(t, 1)
	natsURL := startNATS(t)
	r := newCursorRig(t, natsURL, "cur-restart", packet.QoS2)
	restartAfterADrop(t, r)

	lost := seqOf(t, r.sendWithheld("lost"))
	r.send("after")
	awaitHole(t, r.b, r.id, lost)
	r.dropPastTheRewind()

	assert.Equal(t, []string{"lost"}, r.resume(r.addr),
		"[MQTT-4.4.0-1] the restarted cursor sends the hole the rewind no longer reaches; "+
			"[MQTT-4.3.3-2], [MQTT-4.3.3-6] nothing completed comes back")
}

// A live copy below a restarted cursor's base names a message the rewind would
// start at, with whatever lies between it and the base, which the restart did
// not read: the copy is delivered and the cursor goes, so the session falls
// back to the rewind rather than skip what that would send [MQTT-4.4.0-1].
func TestALiveCopyBelowARestartedCursorsBaseDropsIt_MQTT_4_4_0_1(t *testing.T) {
	natsmqtt5.SetCursorScanAfter(t, 1)
	natsURL := startNATS(t)
	r := newCursorRig(t, natsURL, "cur-restart-below", packet.QoS1)
	gone := restartAfterADrop(t, r)

	publishLate(t, natsURL, gone)
	completeOne(t, r.sub, "gone")
	require.Eventually(t, func() bool {
		next, _ := natsmqtt5.ReplayCursor(r.b, r.id)
		return next == 0
	}, 5*time.Second, 20*time.Millisecond, "the cursor goes")
}
