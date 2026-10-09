package natsmqtt5

import (
	"context"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"

	"github.com/taumatix/natsmqtt5/packet"
	"github.com/taumatix/natsmqtt5/topic"
)

// subscription is one entry in a session's subscription set. A Session cannot
// hold two non-shared subscriptions with the same Topic Filter, so the filter
// keys the set (MQTT-5.0 §4.8.1).
type subscription struct {
	filter string
	// subject is the NATS subject the filter maps to, prefix included.
	subject string
	// share is the ShareName of a shared subscription, empty otherwise. It
	// becomes the NATS queue group (MQTT-5.0 §4.8.2).
	share string
	opts  packet.Subscription
	// grantedQoS may be below the requested QoS [MQTT-3.8.4-7].
	grantedQoS packet.QoS
	// id is the Subscription Identifier to echo on matching PUBLISH packets,
	// or 0 when the SUBSCRIBE carried none (MQTT-5.0 §3.3.2.3.8).
	id int

	// natsSubs are the NATS subscriptions backing this filter. A filter ending
	// in '#' needs two: one on "x.>" and one on "x", because MQTT's '#' also
	// matches the parent level while NATS's '>' does not (MQTT-5.0 §4.7.1.2).
	natsSubs []*nats.Subscription
	// backlog is the durable consumer a shared subscription's QoS 1 and 2
	// messages are pulled from, when the offline queue is on; see shared.go.
	backlog jetstream.Consumer

	// live gates delivery. The NATS subscriptions are created before the
	// subscription is installed, and their handler resolves the session's
	// current connection — so until installSubscription has checked who that
	// is, a message must not go anywhere. Cleared again when the subscription
	// leaves the session.
	live atomic.Bool

	// replacedBy is the subscription that took this one's place when the client
	// subscribed again to the same Topic Filter [MQTT-3.8.4-3]. A message that
	// reaches or waits for the replaced one is delivered by its successor
	// instead of being dropped: "Application Messages MUST NOT be lost due to
	// replacing the Subscription" [MQTT-3.8.4-4].
	replacedBy atomic.Pointer[subscription]
	// cell is shared by a subscription and the ones that replace it on the same
	// NATS subscriptions (see subscribeOne), and always holds the newest. The
	// NATS handler reads it to find who is to deliver.
	cell *atomic.Pointer[subscription]
}

// latest follows the chain of replacements to the subscription now in force.
func (s *subscription) latest() *subscription {
	for {
		next := s.replacedBy.Load()
		if next == nil {
			return s
		}
		s = next
	}
}

// wanted reports whether a message queued for s is still wanted: s is live, or
// something that replaced it is.
func (s *subscription) wanted() bool {
	for ; s != nil; s = s.replacedBy.Load() {
		if s.live.Load() {
			return true
		}
	}
	return false
}

// outbound tracks a QoS 1 or QoS 2 message the broker has sent to the client
// and is still waiting to have acknowledged (MQTT-5.0 §4.3).
type outbound struct {
	packetID uint16
	qos      packet.QoS
	// msgID is the Mqtt5-Msg-Id of the queue copy a restored entry was read
	// back from, so that the replay does not deliver it a second time.
	msgID string
	// awaitingPubcomp is false while we expect a PUBREC (QoS 2) or PUBACK
	// (QoS 1), true once PUBREL has gone out and we expect PUBCOMP.
	awaitingPubcomp bool
	publish         *packet.Publish
	// arrived and expiry are when the broker took the message in and the
	// Message Expiry Interval it arrived with (nil for none), which a resend
	// needs to say how much of the interval is left; see expiry.go.
	arrived time.Time
	expiry  *uint32
	// seq counts sends, so the in-flight set can be put back in the order it
	// went out. Packet Identifiers cannot do that job: they cycle through
	// 1..65535 and wrap.
	seq uint64
	// queueSeq is the stream sequence of the message's copy in the offline
	// queue, or 0 when it has none. It is what a session record keeps in place
	// of the payload; see sessionRecord.Inflight.
	queueSeq uint64
	// blobKey and blobAt are where the PUBLISH of a message with no queue copy
	// and too large for the record was last written to the payload bucket, and
	// when, so a record is not followed by a rewrite of the payload each time.
	blobKey string
	blobAt  time.Time
	// quotaHeld records that this entry holds a slot in the current
	// connection's send quota, so that its acknowledgement returns that slot
	// and an acknowledgement for anything else does not.
	//
	// It has to be per entry rather than a bare count, because the quota is per
	// connection and the in-flight set is per session: an entry carried into a
	// new connection was paid for on the old one. Without this, a client that
	// reconnects and then acknowledges what it received before gets a slot
	// back for each — and the broker exceeds the Receive Maximum that same
	// client just advertised.
	quotaHeld bool
	// resentOn is the connection that last put this entry back on the wire
	// after a resumption. An acknowledgement that completes the entry on any
	// other connection (one displaced by a takeover, still draining its
	// socket) leaves resentOn's client owing an acknowledgement for the copy
	// it was sent; see completeInflight.
	resentOn *conn
	// ackSubject is the JetStream acknowledgement subject of the message a
	// shared subscription's member was given, still unacknowledged to the group's
	// backlog: acknowledged when this entry completes, handed back to the group
	// when the session ends with the entry in flight. Empty for any other.
	ackSubject string
}

// forgetStaleWithdrawals drops the withdrawn identifiers whose acknowledgement
// the client has had two connections to send.
//
// A timer would be the wrong instrument: the client may be offline, and MQTT
// puts no deadline on an acknowledgement. Two CONNECTs completed after the
// withdrawal without it is evidence it will not come: a client flushes what it
// owes when it reconnects. The identifier is then free again, and an
// acknowledgement that does arrive later is answered as for any unknown one.
// Called with s.mu held.
func (s *session) forgetStaleWithdrawals() {
	for id, w := range s.withdrawn {
		if s.attaches-w.madeAt >= 2 {
			delete(s.withdrawn, id)
		}
	}
}

// ackOwed names a resent identifier's connection and the acknowledgement the
// client owes for it.
type ackOwed struct {
	conn *conn
	typ  packet.Type
	// written is set once the resent PUBLISH is about to be written. An
	// acknowledgement that completes the entry before then can only be for the
	// original, since the client has not been sent the copy; after it, the same
	// acknowledgement could be either the original's or the copy's.
	written bool
	// quota is set when the original's acknowledgement overtook the resend: the
	// send-quota slot the resend took then belongs to the copy, and the client's
	// acknowledgement of the copy returns it [MQTT-3.3.4-9].
	quota bool
}

// owedAck is the acknowledgement that closes o's exchange from the client's
// side: a PUBACK for QoS 1, and for QoS 2 the PUBCOMP (the PUBREC before it is
// answered by handlePubrec, which does not disconnect).
func owedAck(o *outbound) packet.Type {
	if o.qos == packet.QoS1 {
		return packet.PUBACK
	}
	return packet.PUBCOMP
}

// withdrawal is an identifier the session no longer holds a message for, but
// whose acknowledgement the client may still send.
type withdrawal struct {
	// madeAt is the session's attach count when the identifier was withdrawn.
	madeAt uint64
	// owed is the acknowledgement that settles the identifier, from owedAck. An
	// acknowledgement of another type does not.
	owed packet.Type
	// quotaHolder is the connection whose send-quota slot the owed
	// acknowledgement returns, or nil when it returns none.
	quotaHolder *conn
}

// session is the MQTT Session State the broker holds for a Client Identifier
// (MQTT-5.0 §4.1).
//
// By default a session lives in the broker process: it survives a client
// reconnecting to the same broker within the Session Expiry Interval, but not
// a broker restart, and it is not shared between brokers. Subscriptions and
// retained messages do travel through NATS, so a client reconnecting to a
// different broker still reaches the same publishers and subscribers; it just
// has to re-subscribe.
//
// With Options.PersistentSessions the subscription set is mirrored into a
// JetStream key-value bucket, and rec and rev below are this session's half of
// that mirror.
type session struct {
	clientID string
	// instance tells this session from another with the same Client Identifier,
	// on this broker or another. A shared subscription's membership entry holds
	// it, so that the end of a session does not remove the entry of the one that
	// replaced it; see sharedmembers.go.
	instance string
	// holds acknowledges, or hands back to its group, the shared-subscription
	// backlog messages the in-flight entries carry; see outbound.ackSubject. Set
	// when the session is made, before anything else can reach it; nil in a
	// session that has no backlog, which is every one in a unit test.
	holds    holdSink
	identity string
	username string

	// persistMu serialises writes of the durable record, so two changes cannot
	// present their revisions to JetStream out of order. It is always taken
	// before mu and never held across a call that takes mu twice.
	persistMu sync.Mutex

	mu sync.Mutex
	// conn is the connection currently serving this session, or nil while the
	// session is disconnected but not yet expired.
	conn *conn

	subs map[string]*subscription
	// unrestored is what a connection restoring the session from its stored
	// record has yet to rebuild; see setUnrestored.
	unrestored []storedSubscription

	// nextPacketID cycles 1..65535 for broker-to-client packets
	// (MQTT-5.0 §2.2.1).
	nextPacketID uint16
	inflight     map[uint16]*outbound
	deadBlobs    []string
	// spillKeys are the payload-bucket values the last record named in
	// sessionRecord.Spill, and spillAt when they were written; zero when they
	// were read back from another broker's record, which makes them stale.
	spillKeys []string
	spillAt   time.Time
	// sendSeq numbers sends so that unacknowledged returns the in-flight set in
	// the order it left.
	sendSeq uint64
	// receivedQoS2 holds the Packet Identifiers of QoS 2 PUBLISH packets that
	// have been accepted and not yet released, so a redelivered PUBLISH is
	// acknowledged without being forwarded twice (MQTT-5.0 §4.3.3).
	receivedQoS2 map[uint16]struct{}
	// qos2Forwarding is those of receivedQoS2 whose message has not yet been
	// forwarded. They are not written to the session record: a client whose
	// broker died before forwarding must be able to send the PUBLISH again.
	qos2Forwarding map[uint16]struct{}
	// withdrawn holds the Packet Identifiers of in-flight messages the broker
	// took back rather than resent, because the filter that earned them was
	// denied on resume. The client was sent those messages and may still owe an
	// acknowledgement for them, so one has to be ignored rather than answered
	// with a protocol error.
	withdrawn map[uint16]withdrawal
	// attaches counts the connections attached to this session. A withdrawal
	// records the count it was made under, which is how attach finds the ones
	// the client has had two connections to settle; see forgetStaleWithdrawals.
	attaches uint64
	// resent records the Packet Identifiers the current connection put back on
	// the wire after a resumption, and the acknowledgement each one is owed.
	//
	// Reading the in-flight entry and writing the packet cannot be one step
	// without holding this lock across a socket write, so an acknowledgement
	// can complete the exchange in between. The copy then goes out for an
	// exchange the session has finished, and the client's acknowledgement of it
	// is unmatched. A client that did what [MQTT-4.4.0-1] asks of it must not be
	// disconnected for that, so an unmatched acknowledgement of the kind a
	// resend is owed is ignored; see forgetResent.
	//
	// It is not emptied by the ordinary acknowledgement, which cannot be told
	// from the late one, so an entry can outlive its exchange. The cost is that
	// a client acknowledging an identifier the broker resent, twice, is not
	// disconnected for the second; the set is per connection and is cleared by
	// the next attach.
	resent map[uint16]ackOwed

	will      *packet.Will
	willDelay time.Duration
	// willConn is the connection that set will. Only that connection's end
	// takes it: a connection that was replaced finishes after its successor
	// has set its own, and must neither publish nor cancel that one
	// [MQTT-3.1.2-8].
	willConn *conn
	// willLease is the stored record of the Will held in will or pendingWill,
	// or nil when Options.DurableWills is off; see willstore.go.
	willLease *willLease
	// awayAt is when the session's last connection ended, for the offline
	// queue's replay; zero when there is nothing to replay.
	awayAt time.Time
	// awayRestored marks an awayAt read from the session store. The ids the
	// previous connection delivered are not stored with it, so its replay
	// cannot rewind past them; see replayOffline.
	awayRestored bool
	// awayRewoundTo is the time the record's delivered ids reach back to, for
	// an absence read from the session store that has it (sessionRecord.DeliveredSince).
	awayRewoundTo time.Time
	// awayFromSeq is the lowest offline-queue sequence the last connection had
	// not delivered when it ended, or 0 when it did not note one. A replay
	// starts there rather than at a time; see conn.awayFloor.
	awayFromSeq uint64
	// awayLateSeq is the lowest queue sequence whose live copy reached the
	// session after its connection ended, while it waits for the next one. The
	// copy found no connection to take it, and nothing else says the session
	// was owed that message; see lateCopy.
	awayLateSeq uint64
	// lateSeq is the same for a copy that reached the connection as it was
	// ending, after it worked out where a replay starts and before the session
	// let go of it. detach folds it into the absence.
	lateSeq uint64
	// predecessor is the connection the latest one displaced.
	predecessor *conn
	// delivered is the offline-queue ids recently delivered to the client,
	// so a replay that rewinds past them does not repeat them. See
	// noteDelivered for how long one is kept.
	delivered map[string]deliveredMark
	// maxDeliveredSeq is the highest queue sequence noteDelivered has seen.
	maxDeliveredSeq uint64

	// pendingWill is a Will whose connection has gone and which is waiting
	// out its Will Delay Interval. It is published only if it is still here
	// when the delay ends.
	pendingWill   *packet.Will
	expirySeconds uint32
	// disconnectedAt is when the connection went away, used with
	// expirySeconds to decide whether the session may still be resumed.
	disconnectedAt time.Time
	// discarded marks a session that must not be resumed.
	discarded bool

	// rec is the durable record backing this session, nil when the broker does
	// not persist sessions. rev is the JetStream revision this broker's
	// ownership of the record rests on.
	rec *sessionRecord
	rev uint64
	// claimGen counts claims of the record. A connection remembers the
	// generation it claimed under and may only write the record while that is
	// still current, which is what stops a connection being displaced on this
	// broker from handing back the record its successor has just claimed — the
	// two run concurrently, and the displaced one holds a revision that has
	// since been superseded by a claim, not invalidated by one.
	claimGen uint64
	// rewind is how far this session's replays reach back (Options.OfflineQueueRewind).
	rewind time.Duration
}

// window is how far a replay for this session rewinds.
func (s *session) window() time.Duration {
	if s.rewind <= 0 {
		return offlineRewind
	}
	return s.rewind
}

// deliveredFloor is the time a record written now says its delivered ids reach
// back to: the replay's rewind before the connection's end, with the
// checkpoint's skew.
func (s *session) deliveredFloor() time.Time {
	return time.Now().Add(-s.window() - checkpointSkew)
}

func newSession(clientID string) *session {
	return &session{
		clientID:       clientID,
		instance:       nuid.Next(),
		subs:           make(map[string]*subscription),
		inflight:       make(map[uint16]*outbound),
		receivedQoS2:   make(map[uint16]struct{}),
		qos2Forwarding: make(map[uint16]struct{}),
		withdrawn:      make(map[uint16]withdrawal),
		resent:         make(map[uint16]ackOwed),
	}
}

// attach binds a connection to the session, returning the connection it
// displaced, if any.
func (s *session) attach(c *conn) *conn {
	s.mu.Lock()
	defer s.mu.Unlock()
	prev := s.conn
	if prev != nil {
		s.predecessor = prev
	}
	s.conn = c
	s.disconnectedAt = time.Time{}
	// A resumption cancels a delayed Will [MQTT-3.1.3-9]; clearing it here
	// keeps it cancelled even if this connection ends before the delay does.
	s.pendingWill = nil

	// "The send quota and Receive Maximum value are not preserved across
	// Network Connections, and are re-initialized with each new Network
	// Connection ... They are not part of the session state" (MQTT-5.0 §4.9).
	// So nothing carried into this connection holds a slot in its quota until a
	// resend takes one. This runs during the handshake, before the CONNACK, so
	// no acknowledgement can be in flight against the new quota yet.
	for _, o := range s.inflight {
		o.quotaHeld = false
	}
	s.resent = make(map[uint16]ackOwed)
	s.attaches++
	s.forgetStaleWithdrawals()
	return prev
}

// detach unbinds the connection and starts the expiry clock. A non-nil a is
// recorded as what the connection left undelivered, in the same step, so that
// no one sees the session unbound and without it.
func (s *session) detach(c *conn, a *away) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn == c {
		s.conn = nil
		s.disconnectedAt = time.Now()
	}
	if a != nil {
		s.markAwayLocked(*a)
	}
}

// awaitPredecessor returns once the connection this one displaced, if it did,
// has recorded what it left undelivered, or when c ends. A client that
// reconnects at once is attached before the old connection has finished; its
// replay must not run before that connection says where it stood.
func (s *session) awaitPredecessor(c *conn) {
	s.mu.Lock()
	prev := s.predecessor
	s.mu.Unlock()
	if prev == nil || prev == c {
		return
	}
	select {
	case <-prev.ended:
	case <-c.done:
	case <-time.After(predecessorWait):
	}
}

// awaitPredecessorLoop returns once the delivery goroutine of the connection
// this one displaced, if it ran one, has stopped, or when c ends. Until then
// it can still send and track a message the successor's resend would miss.
//
// It does not wait for the predecessor's reader, which awaitPredecessor does:
// that one can be held up arbitrarily by an Authorizer or a slow packet, and a
// resumed client must not wait for it. A PUBACK still unread there is applied
// when it is read, and the successor's resend of that message carries DUP
// [MQTT-4.4.0-1]; see completeInflight.
func (s *session) awaitPredecessorLoop(c *conn) {
	s.mu.Lock()
	prev := s.predecessor
	s.mu.Unlock()
	if prev == nil || prev == c || !prev.loopStarted.Load() {
		return
	}
	select {
	case <-prev.loopDone:
	case <-c.done:
	case <-time.After(predecessorWait):
	}
}

// predecessorWait bounds awaitPredecessor, which is only waiting for the
// displaced connection to stop its delivery goroutine.
const predecessorWait = 10 * time.Second

// takeOver tells the session's current connection that another CONNECT has
// claimed the Client Identifier [MQTT-3.1.4-3].
func (s *session) takeOver() {
	s.mu.Lock()
	c := s.conn
	s.mu.Unlock()
	if c != nil {
		c.shutdown(packet.SessionTakenOver, "another connection used this Client Identifier")
	}
}

// expired reports whether the Session Expiry Interval has elapsed since the
// connection went away (MQTT-5.0 §3.1.2.11.2).
func (s *session) expired() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.expiredLocked()
}

func (s *session) expiredLocked() bool {
	switch {
	case s.discarded:
		return true
	case s.conn != nil:
		return false
	case s.expirySeconds == 0:
		return true
	case s.expirySeconds == 0xFFFFFFFF:
		// UINT_MAX means the session never expires.
		return false
	}
	return time.Since(s.disconnectedAt) > time.Duration(s.expirySeconds)*time.Second
}

// claimForResume is the negation of expired for a CONNECT that is about to
// resume the session: it decides and restarts the expiry clock in one step, so
// the sweep cannot expire the session between the CONNECT deciding to resume it
// and attaching to it. Restarting the clock lengthens the session by the time
// the handshake takes, which [MQTT-3.1.2-23] allows: the session must live at
// least as long as the interval, not at most.
func (s *session) claimForResume() (resumable bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.expiredLocked() {
		return false
	}
	if s.conn == nil {
		s.disconnectedAt = time.Now()
	}
	return true
}

// expireIfDue discards a detached session whose Session Expiry Interval has
// passed and returns the subscriptions it held, or ok false when it is not due.
// Only a session that is disconnected, has a non-zero interval (an interval of 0
// ends with the connection, in conn.finish) and has been disconnected for longer
// than that interval is due [MQTT-3.1.2-23]. The NATS subscriptions are left
// for the caller to tear down, outside the session's lock.
func (s *session) expireIfDue() (subs []*subscription, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.discarded || s.conn != nil || s.disconnectedAt.IsZero() ||
		s.expirySeconds == 0 || !s.expiredLocked() {
		return nil, false
	}
	for _, sub := range s.subs {
		sub.live.Store(false)
		subs = append(subs, sub)
	}
	s.subs = make(map[string]*subscription)
	s.discarded = true
	return subs, true
}

// discard marks the session unusable and tears down its NATS subscriptions. The
// shared-subscription messages it still held go back to their groups.
//
// It returns the subscriptions the session held. Whether the session has ended
// or only moved to another broker is the caller's to say: an ended session
// leaves its shared subscriptions' members (Broker.leaveGroups), a moved one
// has not left them.
func (s *session) discard() []*subscription {
	s.mu.Lock()
	held := s.subs
	s.subs = make(map[string]*subscription)
	s.discarded = true
	s.mu.Unlock()

	subs := make([]*subscription, 0, len(held))
	for _, sub := range held {
		sub.live.Store(false)
		unsubscribeAll(sub)
		subs = append(subs, sub)
	}
	s.handBackHeld()
	return subs
}

// handBackHeld gives the shared subscriptions' backlogs back the messages the
// in-flight set holds for them, because the session that was given them has
// ended or no longer wants them. The group's other members can have them now:
// "If the Client's Session terminates before the Client reconnects, the Server
// SHOULD send the Application Message to another Client that is subscribed to
// the same Shared Subscription" (MQTT-5.0 §4.8.2).
func (s *session) handBackHeld() {
	s.mu.Lock()
	var held []string
	for _, o := range s.inflight {
		if o.ackSubject != "" {
			held = append(held, o.ackSubject)
			o.ackSubject = ""
		}
	}
	s.mu.Unlock()
	if len(held) > 0 && s.holds != nil {
		s.holds.handBack(held)
	}
}

// heldSubjects lists the acknowledgement subjects of the backlog messages the
// session is holding, for holdLoop.
func (s *session) heldSubjects() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var held []string
	for _, o := range s.inflight {
		if o.ackSubject != "" {
			held = append(held, o.ackSubject)
		}
	}
	return held
}

// holdSink is where a session's backlog messages go: acknowledged when the
// client has acknowledged them, handed back to the group when it will not.
type holdSink interface {
	ack(subject string)
	handBack(subjects []string)
}

func unsubscribeAll(sub *subscription) {
	for _, ns := range sub.natsSubs {
		_ = ns.Unsubscribe()
	}
	sub.natsSubs = nil
}

// drainAll ends sub's NATS subscriptions after their handlers have seen the
// messages already queued for them, which unsubscribeAll would discard.
func drainAll(sub *subscription) {
	for _, ns := range sub.natsSubs {
		_ = ns.Drain()
	}
	sub.natsSubs = nil
}

// nextID allocates a Packet Identifier that is not currently in flight. It
// returns false when all 65535 identifiers are in use, which the caller turns
// into back-pressure rather than a protocol error.
//
// A withdrawn identifier counts as in use. The client was sent that message and
// still owes an acknowledgement for it, so handing the identifier to a new
// message would let that acknowledgement complete the new one — which is then
// silently never resent.
func (s *session) nextID() (uint16, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := 0; i < 0xFFFF; i++ {
		s.nextPacketID++
		if s.nextPacketID == 0 {
			s.nextPacketID = 1
		}
		_, busy := s.inflight[s.nextPacketID]
		_, owed := s.withdrawn[s.nextPacketID]
		if !busy && !owed {
			return s.nextPacketID, true
		}
	}
	return 0, false
}

func (s *session) trackInflight(o *outbound) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sendSeq++
	o.seq = s.sendSeq
	s.inflight[o.packetID] = o
}

// unacknowledged returns the in-flight set in the order it was sent, which is
// the order it has to be resent in [MQTT-4.6.0-5].
//
// The entries are copies. The caller resends without holding the lock, and the
// live entries keep changing underneath as acknowledgements arrive — including
// being deleted, which is why a copy is safer than a slice of pointers. The
// packet each copy points at is not copied and must not be modified.
func (s *session) unacknowledged() []outbound {
	s.mu.Lock()
	out := make([]outbound, 0, len(s.inflight))
	for _, o := range s.inflight {
		out = append(out, *o)
	}
	s.mu.Unlock()

	// Sorted outside the lock: the slice is the caller's already, and the lock
	// is also taken on the NATS dispatcher path for every delivered message.
	sort.Slice(out, func(i, j int) bool { return out[i].seq < out[j].seq })
	return out
}

// takeQuotaSlot records that the entry for id now holds a slot in the current
// connection's send quota, and reports whether the entry was still there to
// record it against.
func (s *session) takeQuotaSlot(c *conn, id uint16) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.inflight[id]
	if ok {
		o.quotaHeld = true
		o.resentOn = c
		s.resent[id] = ackOwed{conn: c, typ: owedAck(o)}
	}
	return ok
}

// markResent records that c is about to resend the PUBREL for id, and reports
// whether the exchange is still in flight.
func (s *session) markResent(c *conn, id uint16) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.inflight[id]
	if ok {
		o.resentOn = c
		s.resent[id] = ackOwed{conn: c, typ: owedAck(o)}
	}
	return ok
}

// forgetResent reports whether c resent id and the acknowledgement of type t
// is the one owed for it, and forgets the record if so. quota reports that the
// copy holds a send-quota slot that this acknowledgement returns.
func (s *session) forgetResent(c *conn, id uint16, t packet.Type) (ok, quota bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, found := s.resent[id]
	if !found || r.conn != c || r.typ != t {
		return false, false
	}
	delete(s.resent, id)
	return true, r.quota
}

// markResentWritten records that c is about to write the copy of id, after which
// an acknowledgement for it is ambiguous; see ackOwed.written.
func (s *session) markResentWritten(c *conn, id uint16) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.resent[id]; ok && r.conn == c {
		r.written = true
		s.resent[id] = r
	}
}

// abandonResend forgets the record of a resend that did not go out after all.
func (s *session) abandonResend(c *conn, id uint16) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.resent[id]; ok && r.conn == c {
		delete(s.resent, id)
	}
}

// awaitPubcomp records that PUBREL has gone out for id, so the broker now
// expects PUBCOMP rather than PUBREC (MQTT-5.0 §4.3.3). What turns on it is
// which packet a resend after a reconnect has to be: past the PUBREC the client
// owns the message, so it is the PUBREL that is repeated and not the PUBLISH
// [MQTT-4.4.0-1].
func (s *session) awaitPubcomp(id uint16) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if o, ok := s.inflight[id]; ok {
		o.awaitingPubcomp = true
	}
}

// completeInflight removes the entry for id and reports whether one existed.
// The entry's backlog message, if it has one, is acknowledged to its group: the
// exchange is over, however it ended. That includes a PUBACK carrying a Reason
// Code of 0x80 or greater, after which the message must not be sent to any
// other subscriber [MQTT-4.8.2-6].
func (s *session) completeInflight(c *conn, id uint16) (outbound, bool) {
	done, ok := s.completeInflightLocked(c, id)
	if ok && done.ackSubject != "" && s.holds != nil {
		s.holds.ack(done.ackSubject)
	}
	return done, ok
}

func (s *session) completeInflightLocked(c *conn, id uint16) (outbound, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.inflight[id]
	if !ok {
		return outbound{}, false
	}
	delete(s.inflight, id)
	s.retireBlobLocked(o)
	done := *o
	if r, resent := s.resent[id]; resent && r.conn == c && !r.written && !r.quota && o.quotaHeld && o.resentOn == c {
		// The original's acknowledgement overtook a resend that has taken a
		// slot and not yet written the copy. The copy will go out, so the slot
		// is its, and the acknowledgement of the copy returns it; returning it
		// now would let the client hold one message more than its Receive
		// Maximum [MQTT-3.3.4-9].
		r.quota = true
		s.resent[id] = r
		done.quotaHeld = false
	}
	if o.resentOn != nil && o.resentOn != c {
		// The acknowledgement came from a connection displaced by a takeover,
		// for a message its successor has since resent. It stands: the client
		// did receive the message, and the session is the Client Identifier's,
		// not the connection's (MQTT-5.0 §4.1). But the successor's client will
		// acknowledge the copy it was sent too. Keep the identifier as owed,
		// so that acknowledgement is ignored rather than answered with 0x82 and
		// the identifier is not handed to a new message first; and leave the
		// send-quota slot for that acknowledgement to return, on the connection
		// that spent it.
		w := withdrawal{madeAt: s.attaches, owed: owedAck(o)}
		if o.quotaHeld {
			w.quotaHolder = o.resentOn
		}
		s.withdrawn[id] = w
		done.quotaHeld = false
	}
	return done, true
}

// inflightEntry returns a copy of the entry for id. It is a copy for the same
// reason unacknowledged returns copies: awaitingPubcomp is mutable state that
// two goroutines reach — the connection serving the session and, for as long as
// a displaced connection is still draining its socket, that one too.
func (s *session) inflightEntry(id uint16) (outbound, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.inflight[id]
	if !ok {
		return outbound{}, false
	}
	return *o, true
}

// withdrawInflight takes back the unacknowledged messages a denied filter
// earned, and returns the Packet Identifiers it took. It is how a subscription
// denied on resume stops the payloads it earned from being put back on the wire
// by the retransmission that follows.
//
// An entry has to match a denied filter and no surviving one. "Matches no
// surviving filter" alone is not the same test and would take back messages the
// broker MUST resend [MQTT-4.4.0-1]: handleUnsubscribe deliberately leaves the
// in-flight set alone, so a message earned by a filter the client has since
// unsubscribed from matches nothing live and is still owed.
//
// An entry past its PUBREC is left alone. The client took ownership of that
// message when it sent the PUBREC [MQTT-4.3.3-8], so what is outstanding is a
// PUBREL carrying no payload; withholding it would leave the client waiting for
// a PUBCOMP forever, and its Packet Identifier unusable, to prevent a
// disclosure that has already happened.
//
// At the handshake attach has just cleared every entry's claim on the send
// quota. From Broker.Reauthorize, on a live connection, an entry may hold one,
// and the withdrawal records it so the acknowledgement still returns it.
func (s *session) withdrawInflight(denied, surviving []string) []uint16 {
	var handBack []string
	taken := s.withdrawInflightLocked(denied, surviving, &handBack)
	if len(handBack) > 0 && s.holds != nil {
		// What was withdrawn is no longer this session's: the group's other
		// members can have it (MQTT-5.0 §4.8.2).
		s.holds.handBack(handBack)
	}
	return taken
}

func (s *session) withdrawInflightLocked(denied, surviving []string, handBack *[]string) []uint16 {
	s.mu.Lock()
	defer s.mu.Unlock()

	var taken []uint16
	for id, o := range s.inflight {
		if o.awaitingPubcomp {
			continue
		}
		name := o.publish.Topic
		if !topic.MatchAny(denied, name) || topic.MatchAny(surviving, name) {
			continue
		}
		delete(s.inflight, id)
		s.retireBlobLocked(o)
		if o.ackSubject != "" {
			*handBack = append(*handBack, o.ackSubject)
		}
		w := withdrawal{madeAt: s.attaches, owed: owedAck(o)}
		if o.quotaHeld {
			// Only on a live connection: attach clears every claim at the
			// handshake. The client's acknowledgement returns this slot.
			w.quotaHolder = s.conn
		}
		s.withdrawn[id] = w
		taken = append(taken, id)
	}
	return taken
}

// forgetWithdrawn reports whether id names a message the broker took back that
// is owed an acknowledgement of type t, and forgets it if so. That
// acknowledgement is the last one owed for it; one of another type leaves the
// record for the right one.
func (s *session) forgetWithdrawn(id uint16, t packet.Type) (withdrawal, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w, ok := s.withdrawn[id]
	if !ok || w.owed != t {
		return withdrawal{}, false
	}
	delete(s.withdrawn, id)
	return w, true
}

// markQoS2Received records a QoS 2 Packet Identifier and reports whether it
// was already present, i.e. whether this PUBLISH is a redelivery that must not
// be forwarded a second time (MQTT-5.0 §4.3.3).
func (s *session) markQoS2Received(id uint16) (duplicate bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, duplicate = s.receivedQoS2[id]
	s.receivedQoS2[id] = struct{}{}
	if !duplicate {
		s.qos2Forwarding[id] = struct{}{}
	}
	return duplicate
}

// qos2Forwarded says the message of a received QoS 2 PUBLISH has been
// forwarded, so a record may hold its identifier [MQTT-4.3.3-10].
func (s *session) qos2Forwarded(id uint16) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.qos2Forwarding, id)
}

// releaseQoS2 clears a Packet Identifier on PUBREL and reports whether it was
// outstanding. A PUBREL for an unknown identifier is answered with 0x92
// (Packet Identifier not found) (MQTT-5.0 §3.6.2.1).
func (s *session) releaseQoS2(id uint16) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.receivedQoS2[id]
	delete(s.receivedQoS2, id)
	delete(s.qos2Forwarding, id)
	return ok
}

func (s *session) subscription(filter string) (*subscription, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sub, ok := s.subs[filter]
	return sub, ok
}

// subscriptions returns the session's live subscription set. The slice is the
// caller's, so it can be walked while the set is being changed; the
// subscriptions it points at are the live ones and must not be modified.
func (s *session) subscriptions() []*subscription {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*subscription, 0, len(s.subs))
	for _, sub := range s.subs {
		out = append(out, sub)
	}
	return out
}

// installSubscription puts sub into the session on behalf of c, replacing and
// returning any subscription already held on the same filter. It refuses, and
// changes nothing, when c is no longer the session's connection.
//
// The check and the install are one critical section on purpose. The decision
// to allow sub was made by the Authorizer on c's principal, and a takeover can
// happen while that call is out: the new connection attaches, then sweeps the
// session's subscriptions past the Authorizer again. Checked under the same
// lock attach takes, an install either lands before the attach — and the sweep
// sees it — or after, and is refused here. Checked any earlier, it could land
// after the sweep had already run and be delivered to a principal nobody asked.
//
// A sub that has no NATS subscriptions of its own (natsSubs empty) takes over
// the ones the subscription it replaces holds, so a repeated SUBSCRIBE on a
// non-shared filter never leaves the NATS server: there is no instant when the
// interest is gone, or doubled. When there is nothing to take over, rebind is
// true and nothing is changed.
func (s *session) installSubscription(c *conn, sub *subscription) (old *subscription, installed, rebind bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != c {
		return nil, false, false
	}
	old = s.subs[sub.filter]
	if len(sub.natsSubs) == 0 {
		if old == nil || old.cell == nil || len(old.natsSubs) == 0 {
			return nil, false, true
		}
		sub.natsSubs, old.natsSubs = old.natsSubs, nil
		sub.cell = old.cell
	}
	s.subs[sub.filter] = sub
	// Live before the old one is retired, so a message arriving between the two
	// is delivered by one of them and not by neither.
	sub.live.Store(true)
	if old != nil {
		old.replacedBy.Store(sub)
		sub.cell.Store(sub)
		old.live.Store(false)
	}
	return old, true, false
}

// holdsBound reports whether the session holds a subscription on filter whose
// NATS subscriptions can be taken over by its replacement.
func (s *session) holdsBound(filter string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	old := s.subs[filter]
	return old != nil && old.cell != nil && len(old.natsSubs) > 0
}

// removeSubscriptionFor removes filter on behalf of c, which must still be the
// session's connection; owner is false, and nothing is changed, when it is not.
// The check and the removal share the lock attach takes, as they do in
// installSubscription.
func (s *session) removeSubscriptionFor(c *conn, filter string) (sub *subscription, ok, owner bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != c {
		return nil, false, false
	}
	sub, ok = s.subs[filter]
	if ok {
		delete(s.subs, filter)
		sub.live.Store(false)
	}
	return sub, ok, true
}

func (s *session) removeSubscription(filter string) (*subscription, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sub, ok := s.subs[filter]
	if ok {
		delete(s.subs, filter)
		sub.live.Store(false)
	}
	return sub, ok
}

// setWill makes w the Will of connection c. It returns the stored record of a
// Will it replaced, which the caller removes: left in the Will store, a broker
// that outlived this one would publish it [MQTT-3.1.2-10].
func (s *session) setWill(c *conn, w *packet.Will, delay time.Duration, lease *willLease) (replaced *willLease) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.will != nil {
		replaced = s.willLease
	}
	s.will, s.willDelay, s.willLease, s.willConn = w, delay, lease, c
	return replaced
}

// takeWill removes and returns connection c's Will Message, so it can only ever
// be published once, with the stored record that goes with it. A Will that
// another connection set is not c's to take.
func (s *session) takeWill(c *conn) (*packet.Will, time.Duration, *willLease) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.willConn != c {
		return nil, 0, nil
	}
	w, d, l := s.will, s.willDelay, s.willLease
	s.will, s.willDelay, s.willLease, s.willConn = nil, 0, nil, nil
	return w, d, l
}

// cancelWills forgets every Will the session holds, the connection's and one
// waiting out its delay, and returns their stored records, for a new connection
// that is about to be attached [MQTT-3.1.2-8], [MQTT-3.1.3-9]. The client is
// live again: none of them may be published.
func (s *session) cancelWills() *willLease {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.will == nil && s.pendingWill == nil {
		return nil
	}
	l := s.willLease
	s.will, s.willDelay, s.willLease, s.willConn = nil, 0, nil, nil
	s.pendingWill = nil
	return l
}

// markAwayLocked records that the last connection ended. A replay no connection has
// taken yet is still owed, and is kept: the earlier start wins, and the lower
// of two sequences.
func (s *session) markAwayLocked(a away) {
	a.lateSeq = lowestSeq(a.lateSeq, s.lateSeq, s.awayLateSeq)
	s.lateSeq = 0
	if !s.awayAt.IsZero() {
		a.at, a.restored, a.rewoundTo = s.awayAt, s.awayRestored, s.awayRewoundTo
		switch {
		case s.awayFromSeq == 0:
			a.fromSeq = 0
		case a.fromSeq == 0 || s.awayFromSeq < a.fromSeq:
			a.fromSeq = s.awayFromSeq
		}
	}
	s.awayAt, s.awayRestored, s.awayFromSeq, s.awayLateSeq = a.at, a.restored, a.fromSeq, a.lateSeq
	s.awayRewoundTo = a.rewoundTo
}

// lowestSeq is the lowest of the sequences that are set, or 0 when none is.
func lowestSeq(seqs ...uint64) uint64 {
	var lowest uint64
	for _, q := range seqs {
		if q != 0 && (lowest == 0 || q < lowest) {
			lowest = q
		}
	}
	return lowest
}

// markAwayRestored records the absence a session store record describes: when
// the session was released, and, if the record has them, the sequence the
// replay starts at and the ids delivered at or above it.
func (s *session) markAwayRestored(at time.Time, fromSeq, lateSeq uint64, rewoundTo time.Time, delivered []storedDelivered) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.awayAt, s.awayRestored, s.awayFromSeq, s.awayLateSeq = at, true, fromSeq, lateSeq
	s.awayRewoundTo = rewoundTo
	if len(delivered) == 0 {
		return
	}
	if s.delivered == nil {
		s.delivered = make(map[string]deliveredMark, len(delivered))
	}
	now := time.Now()
	for _, d := range delivered {
		s.delivered[d.ID] = deliveredMark{at: now, seq: d.Seq}
		if d.Seq > s.maxDeliveredSeq {
			s.maxDeliveredSeq = d.Seq
		}
	}
}

// maxStoredDelivered bounds the delivered ids a session record carries. The ids
// that matter are those at or above the replay's start, which is the messages
// the client was sent out of order or ahead of one it was not; that is a few
// hundred at most for a client behind by its connection's queue, and the bound
// keeps the record far below a key-value value's size limit however it came to
// be larger. Dropping one can repeat that message on a restored replay.
const maxStoredDelivered = 4096

// awayState is what the session record keeps of the last connection's absence
// beyond its time: the replay's starting sequence and the ids delivered at or
// above it, lowest sequence first. dropped counts ids left out for the bound.
func (s *session) awayState() (fromSeq, lateSeq uint64, delivered []storedDelivered, dropped int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	late := lowestSeq(s.awayLateSeq, s.lateSeq)
	from := lowestSeq(s.awayFromSeq, late)
	if from == 0 {
		from = math.MaxUint64 // no sequence: only the ids of the rewind's span
	}
	delivered, dropped = s.deliveredFromLocked(from, nil)
	return s.awayFromSeq, late, delivered, dropped
}

// lateRecord is the released record with a late copy's sequence folded in, the
// revision to write it at, and the claim it belongs to. It returns nil when the
// session has no released record to correct.
func (s *session) lateRecord() (rec *sessionRecord, rev, gen uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rec == nil || s.rec.Attached || s.awayLateSeq == 0 {
		return nil, 0, 0
	}
	cp := *s.rec
	cp.AwayFromSeq = s.awayFromSeq
	cp.AwayLateSeq = s.awayLateSeq
	// Ids past the record's bound are left out here as at release, which
	// already logged them.
	cp.Delivered, _ = s.deliveredFromLocked(lowestSeq(s.awayFromSeq, s.awayLateSeq), nil)
	cp.DeliveredSince = s.deliveredFloor()
	return &cp, s.rev, s.claimGen
}

// deliveredFromLocked lists the delivered ids whose queue copy is at or above
// fromSeq, lowest first and bounded by maxStoredDelivered, leaving out those
// whose sequence is in exclude. s.mu is held.
func (s *session) deliveredFromLocked(fromSeq uint64, exclude map[uint64]bool) (delivered []storedDelivered, dropped int) {
	floor := s.deliveredFloor()
	for id, m := range s.delivered {
		if (m.seq >= fromSeq || !m.at.Before(floor)) && !exclude[m.seq] {
			delivered = append(delivered, storedDelivered{ID: id, Seq: m.seq})
		}
	}
	sort.Slice(delivered, func(i, j int) bool { return delivered[i].Seq < delivered[j].Seq })
	if len(delivered) > maxStoredDelivered {
		dropped = len(delivered) - maxStoredDelivered
		delivered = delivered[:maxStoredDelivered]
	}
	return delivered, dropped
}

// deliveredSince is the delivered ids a checkpoint writes with a replay from
// fromSeq: those at or above it. A replay with no sequence starts at a time,
// and the ids delivered since that time are the ones it must not repeat. The
// unacknowledged messages are among them: they are resent from the record's
// in-flight list, not by the replay.
func (s *session) deliveredSince(fromSeq, lateSeq uint64, since time.Time) (delivered []storedDelivered, dropped int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	from := lowestSeq(fromSeq, lateSeq)
	for id, m := range s.delivered {
		if (from != 0 && m.seq >= from) || !m.at.Before(since) {
			delivered = append(delivered, storedDelivered{ID: id, Seq: m.seq})
		}
	}
	sort.Slice(delivered, func(i, j int) bool {
		if delivered[i].Seq != delivered[j].Seq {
			return delivered[i].Seq < delivered[j].Seq
		}
		return delivered[i].ID < delivered[j].ID
	})
	if len(delivered) > maxStoredDelivered {
		dropped = len(delivered) - maxStoredDelivered
		delivered = delivered[:maxStoredDelivered]
	}
	return delivered, dropped
}

// away is what takeAway hands the replay.
type away struct {
	at       time.Time
	restored bool // read from the session store; see session.awayRestored
	// rewoundTo is how far back the stored delivered ids reach, when restored
	// and the record says; zero otherwise.
	rewoundTo time.Time
	fromSeq   uint64
	// lateSeq is the lowest queue sequence a live copy reached the session with
	// after the connection that was owed it had ended; see session.lateCopy.
	lateSeq uint64
}

// lateCopy handles the live copy of a queued message (queue sequence seq) that
// reached a connection which has ended, or no connection at all. Its queue copy
// was stored before the live one was published, so a replay that starts above
// seq cannot find it, and nothing else says the session was owed it. It is
// noted so the next replay starts at or below it, however long the copy took
// [MQTT-4.4.0-1].
//
// from is the ended connection the copy reached, nil when it found none. It
// returns the session's connection when that is a different one: the copy
// belongs to it, as a live delivery. persist is true when the note changed a
// released session, whose stored record then has to say so.
func (s *session) lateCopy(from *conn, seq uint64) (successor *conn, persist bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case s.conn != nil && s.conn != from:
		return s.conn, false
	case s.conn != nil:
		// Still attached, and ending: detach folds it into the absence.
		if s.lateSeqSafeLocked(seq) {
			s.lateSeq = lowestSeq(s.lateSeq, seq)
		}
		return nil, false
	case s.awayAt.IsZero():
		// Nothing is waiting to replay: the session has no absence to correct.
		return nil, false
	}
	if !s.lateSeqSafeLocked(seq) || (s.awayLateSeq != 0 && s.awayLateSeq <= seq) {
		return nil, false
	}
	s.awayLateSeq = seq
	return nil, s.rec != nil
}

// lateSeqSafeLocked says whether a replay may start at seq: the ids delivered at
// or above it must still be known, or a QoS 2 message would be sent twice
// [MQTT-4.3.3-2]. They are kept for the span deliveredSeqWindow below the
// highest delivered sequence.
func (s *session) lateSeqSafeLocked(seq uint64) bool {
	return s.maxDeliveredSeq == 0 || seq+deliveredSeqWindow >= s.maxDeliveredSeq
}

// takeAway returns when the last connection ended, once.
func (s *session) takeAway() (a away, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a = away{at: s.awayAt, restored: s.awayRestored, rewoundTo: s.awayRewoundTo, fromSeq: s.awayFromSeq, lateSeq: s.awayLateSeq}
	s.awayAt, s.awayRestored, s.awayFromSeq, s.awayLateSeq = time.Time{}, false, 0, 0
	s.awayRewoundTo = time.Time{}
	return a, !a.at.IsZero()
}

// deliveredSeqWindow is how far below the highest delivered queue sequence a
// delivered id is kept however old it is. A replay may start at the lowest
// sequence not yet delivered, and delivered messages above it must not be sent
// again; those lie within the span two publishers' copies can arrive out of
// order by, plus the connection's waiting messages.
const deliveredSeqWindow = 8192

// deliveredMark is when a message was delivered and the queue sequence of its
// copy, 0 when it had none.
type deliveredMark struct {
	at  time.Time
	seq uint64
}

// noteDelivered records a delivered message id. It forgets those older than a
// time-based replay could rewind to, unless their queue sequence is near the
// newest: a replay starting at a sequence reaches those however old they are.
func (s *session) noteDelivered(id string, seq uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if s.delivered == nil {
		s.delivered = make(map[string]deliveredMark)
	}
	s.delivered[id] = deliveredMark{at: now, seq: seq}
	if seq > s.maxDeliveredSeq {
		s.maxDeliveredSeq = seq
	}
	if len(s.delivered) > 64 && len(s.delivered)%64 == 0 {
		for k, m := range s.delivered {
			if now.Sub(m.at) > 2*s.window() && (m.seq == 0 || m.seq+deliveredSeqWindow < s.maxDeliveredSeq) {
				delete(s.delivered, k)
			}
		}
	}
}

func (s *session) deliveredIDs() map[string]bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]bool, len(s.delivered))
	for k := range s.delivered {
		out[k] = true
	}
	return out
}

func (s *session) setPendingWill(w *packet.Will, lease *willLease) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pendingWill, s.willLease = w, lease
}

// takePendingWill reports whether w is still the pending Will, and clears it
// if so, so that it is published at most once.
func (s *session) takePendingWill(w *packet.Will) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pendingWill != w {
		return false
	}
	s.pendingWill = nil
	return true
}

// currentWill returns the Will that would be published for this session: the
// connection's, or one already waiting out its delay.
func (s *session) currentWill() *packet.Will {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.will != nil {
		return s.will
	}
	return s.pendingWill
}

// discardWill drops w, wherever it is held, so it is never published. It
// returns the stored record of w, which the caller removes.
func (s *session) discardWill(w *packet.Will) *willLease {
	s.mu.Lock()
	defer s.mu.Unlock()
	var l *willLease
	if s.will == w {
		s.will, s.willDelay = nil, 0
		l, s.willLease = s.willLease, nil
	}
	if s.pendingWill == w {
		s.pendingWill = nil
		l, s.willLease = s.willLease, nil
	}
	return l
}

// currentConn returns the connection serving the session, or nil while it is
// disconnected but not yet expired.
func (s *session) currentConn() *conn {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn
}

func (s *session) hasConn() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn != nil
}

// setUnrestored records the stored subscriptions a connection is about to
// rebuild into this session, and nil once it has. A second CONNECT can take the
// session over while the first is still restoring (the Authorizer call on a
// resume has no bound), find a session in memory with only some of its filters
// installed, and answer Session Present 1 for it; the filters not yet installed
// are then lost, and the empty set is written back to the record. Keeping the
// list here lets that connection finish the restore.
func (s *session) setUnrestored(stored []storedSubscription) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.unrestored = stored
}

// unrestoredSubscriptions is the stored subscriptions a displaced connection did
// not finish rebuilding; see setUnrestored.
func (s *session) unrestoredSubscriptions() []storedSubscription {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.unrestored
}

// setPrincipal records who the connection now serving this session
// authenticated as. It is guarded like every other mutable field, and for a
// sharper reason than most: a connection displaced by this one goes on decoding
// the packets its socket had already buffered, so it can be inside
// subscribeOne reading these two while the CONNECT that displaced it writes
// them. An unsynchronised string assignment can be observed half-written.
func (s *session) setPrincipal(identity, username string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.identity, s.username = identity, username
}

// principal returns the identity and User Name every authorisation decision is
// made against.
func (s *session) principal() (identity, username string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.identity, s.username
}

func (s *session) setExpiry(seconds uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expirySeconds = seconds
}

func (s *session) expiry() uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.expirySeconds
}

// bindRecord attaches the durable record this broker has just claimed and
// returns the generation the claiming connection must present to write it.
func (s *session) bindRecord(rec *sessionRecord, rev uint64) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rec, s.rev = rec, rev
	s.claimGen++
	return s.claimGen
}

// snapshot folds the session's live state into a copy of its record, together
// with the revision the write must present. It returns nil when the broker does
// not persist this session, or when gen is no longer the current claim — that
// is, when another connection has taken the record over since.
//
// The copy matters: the caller writes it to JetStream without holding the
// session lock, and the live session keeps changing underneath.
func (s *session) snapshot(gen uint64) (*sessionRecord, uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rec == nil || gen != s.claimGen {
		return nil, 0
	}

	subs := make([]storedSubscription, 0, len(s.subs))
	for _, sub := range s.subs {
		subs = append(subs, storedSubscription{
			Filter:     sub.filter,
			Opts:       sub.opts,
			GrantedQoS: sub.grantedQoS,
			ID:         sub.id,
		})
	}
	// Map iteration order is random, so without this the same subscription set
	// serialises differently on every write and no two records can be compared.
	sort.Slice(subs, func(i, j int) bool { return subs[i].Filter < subs[j].Filter })

	cp := *s.rec
	cp.Subscriptions = subs
	cp.ExpirySeconds = s.expirySeconds
	// Every write of the record carries the withdrawn set, so the one persistSession
	// makes after a withdrawal on resume (and the checkpoint after it) reaches a
	// successor of a broker that is killed outright.
	cp.Withdrawn = s.withdrawnLocked()
	return &cp, s.rev
}

// supersedes reports whether the record this broker holds for the session is
// newer than bucket revision rev, so a write at rev cannot be another broker
// taking the session away. A session with no bound record, or a rev of 0
// (unknown), is never superseded.
func (s *session) supersedes(rev uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rec != nil && rev != 0 && s.rev >= rev
}

// commitRecord records the state and revision a successful write leaves behind,
// unless the record was claimed again while the write was in flight.
func (s *session) commitRecord(gen uint64, rec *sessionRecord, rev uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if gen != s.claimGen {
		return
	}
	s.rec, s.rev = rec, rev
}

// inflightState is what the session record keeps of the unacknowledged messages
// and the received QoS 2 identifiers: the entries in the order they were sent,
// and the identifiers in ascending order. unrecorded counts in-flight messages
// left out because they have no copy in the offline queue to be read back and no
// way to keep their PUBLISH, so a restored session could not resend them.
//
// With blobs set, a PUBLISH too large for the record is not counted as left out
// but returned in pending, for the caller to write to the payload bucket and
// name in the entry at pending[i].index (stashBlobs): the write is a JetStream
// round trip and this runs under the session lock.
func (s *session) inflightState(blobs *sessionStore) (inflight []storedInflight, received []uint16, unrecorded int, pending []pendingBlob) {
	// Under a small max_payload a record of several 16 KiB PUBLISHes would not fit, so
	// the inline share shrinks with it and the rest goes to the payload bucket.
	// Together the inline PUBLISHes get a quarter of the value, so that many of them cannot push the
	// record past it and have fitRecord cut the newest.
	inlineLimit, inlineLeft := maxStoredPublish, int(^uint(0)>>1)
	if blobs != nil {
		if q := blobs.valueLimit() / 4; q < inlineLimit {
			inlineLimit = q
		}
		inlineLeft = blobs.valueLimit() / 4
	}
	s.mu.Lock()
	entries := make([]*outbound, 0, len(s.inflight))
	for _, o := range s.inflight {
		entries = append(entries, o)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].seq < entries[j].seq })
	for _, o := range entries {
		st := storedInflight{ID: o.packetID, QoS: o.qos}
		switch {
		case o.awaitingPubcomp:
			st.Rel = true
		case o.queueSeq != 0:
			st.Seq = o.queueSeq
			st.Ack = o.ackSubject
			st.Retain = o.publish.Retain
			if p := o.publish.Properties; p != nil && len(p.SubscriptionIdentifiers) > 0 {
				st.SubID = p.SubscriptionIdentifiers[0]
			}
		default:
			raw, ok := encodeStorable(o)
			switch {
			case !ok:
				unrecorded++
				continue
			case len(raw) <= inlineLimit && len(raw) <= inlineLeft:
				st.Pub = raw
				inlineLeft -= len(raw)
			case blobs == nil || len(raw) > blobs.blobLimit():
				unrecorded++
				continue
			case o.blobKey != "" && !blobs.blobStale(o.blobAt):
				st.Blob, st.BlobAt = o.blobKey, o.blobAt
			default:
				pending = append(pending, pendingBlob{index: len(inflight), o: o, raw: raw})
			}
			st.At, st.Exp = o.arrived, o.expiry
		}
		inflight = append(inflight, st)
	}
	for id := range s.receivedQoS2 {
		if _, forwarding := s.qos2Forwarding[id]; !forwarding {
			received = append(received, id)
		}
	}
	s.mu.Unlock()
	sort.Slice(received, func(i, j int) bool { return received[i] < received[j] })
	return inflight, received, unrecorded, pending
}

// withdrawnLocked is the withdrawn identifiers for the session record, in
// identifier order. Called with s.mu held.
func (s *session) withdrawnLocked() []storedWithdrawn {
	var out []storedWithdrawn
	for id, w := range s.withdrawn {
		out = append(out, storedWithdrawn{ID: id, Owed: w.owed, Age: s.attaches - w.madeAt})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// restoreWithdrawn puts back the withdrawn identifiers a claim found. The
// session is new, so its attach count is set to a base that leaves room for
// each entry's age; the connection that restores them then attaches, as for a
// session that never left memory. The send-quota slot a withdrawal held died
// with its connection (MQTT-5.0 §4.9), so none is restored.
func (s *session) restoreWithdrawn(ws []storedWithdrawn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	const base = 2
	if s.attaches < base {
		s.attaches = base
	}
	for _, w := range ws {
		if w.Age >= base {
			continue
		}
		if _, inflight := s.inflight[w.ID]; inflight {
			continue
		}
		s.withdrawn[w.ID] = withdrawal{madeAt: s.attaches - w.Age, owed: w.Owed}
	}
}

// blobsStale reports whether a payload this session keeps in the payload bucket
// is due to be written again, which a checkpoint does even when nothing else has
// moved: an unacknowledged message can stay unacknowledged for longer than the
// bucket keeps its payload.
func (s *session) blobsStale(store *sessionStore) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, o := range s.inflight {
		if o.blobKey != "" && store.blobStale(o.blobAt) {
			return true
		}
	}
	return len(s.spillKeys) > 0 && store.blobStale(s.spillAt)
}

// spillInto makes rec fit one value, keeping what it can in spilled values
// (sessionStore.spillRecord), and returns how many in-flight entries were left
// out.
func (s *session) spillInto(store *sessionStore, rec *sessionRecord) (lost int, err error) {
	return store.spillRecord(rec, store.valueLimit())
}

// commitSpill notes that rec, now written, names its spilled values as of
// wroteAt, and deletes those an earlier record named that this one does not.
func (s *session) commitSpill(store *sessionStore, rec *sessionRecord, wroteAt time.Time) {
	s.mu.Lock()
	old := s.spillKeys
	s.spillKeys, s.spillAt = rec.Spill, wroteAt
	s.mu.Unlock()
	named := make(map[string]struct{}, len(rec.Spill))
	for _, k := range rec.Spill {
		named[k] = struct{}{}
	}
	for _, k := range old {
		if _, still := named[k]; still {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), sessionOpTimeout)
		_ = store.deleteBlob(ctx, k)
		cancel()
	}
}

// retireBlobLocked notes that the payload an entry kept in the payload bucket is no
// longer needed. It is removed by reapBlobs once a record that does not name it
// has been written. s.mu is held.
func (s *session) retireBlobLocked(o *outbound) {
	if o.blobKey != "" {
		s.deadBlobs = append(s.deadBlobs, o.blobKey)
		o.blobKey = ""
	}
}

// reapBlobs deletes the payloads of entries that have completed, after a record
// that no longer names them was written. A key an entry holds again (the same
// message, with the same identifier, sent again) is kept. A failed delete is
// tried again at the next checkpoint, and the bucket's TTL removes the payload if
// none succeeds. persistMu is held, which is what keeps a payload from being
// written between the check and the delete.
func (s *session) reapBlobs(store *sessionStore) {
	s.mu.Lock()
	dead := s.deadBlobs
	s.deadBlobs = nil
	held := make(map[string]struct{}, len(s.inflight))
	for _, o := range s.inflight {
		if o.blobKey != "" {
			held[o.blobKey] = struct{}{}
		}
	}
	s.mu.Unlock()
	var failed []string
	for _, key := range dead {
		if _, still := held[key]; still {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), sessionOpTimeout)
		err := store.deleteBlob(ctx, key)
		cancel()
		if err != nil {
			failed = append(failed, key)
		}
	}
	if len(failed) > 0 {
		s.mu.Lock()
		s.deadBlobs = append(s.deadBlobs, failed...)
		s.mu.Unlock()
	}
}

// pendingBlob is an in-flight entry whose PUBLISH is to be written to the
// payload bucket before the record that names it.
type pendingBlob struct {
	index int
	o     *outbound
	raw   []byte
}

// stashBlobs writes the payloads in pending and names them in rec.Inflight. An
// entry whose payload could not be written is dropped from the record, as one
// too large to keep always was, and counted in the result.
//
// Each payload has a deadline of its own: they are up to a value each, and one
// budget for all of them would let a few starve the record write that follows.
func (s *session) stashBlobs(store *sessionStore, rec *sessionRecord, pending []pendingBlob) (unrecorded int, err error) {
	if len(pending) == 0 {
		return 0, nil
	}
	drop := make(map[int]struct{})
	for _, p := range pending {
		ctx, cancel := context.WithTimeout(context.Background(), sessionOpTimeout)
		wroteAt := time.Now()
		key, perr := store.putBlob(ctx, blobOwner(s.clientID), p.raw)
		cancel()
		if perr != nil {
			err = perr
			drop[p.index] = struct{}{}
			continue
		}
		rec.Inflight[p.index].Blob, rec.Inflight[p.index].BlobAt = key, wroteAt
		s.mu.Lock()
		p.o.blobKey, p.o.blobAt = key, wroteAt
		s.mu.Unlock()
	}
	if len(drop) > 0 {
		kept := rec.Inflight[:0]
		for i, st := range rec.Inflight {
			if _, gone := drop[i]; !gone {
				kept = append(kept, st)
			}
		}
		rec.Inflight = kept
	}
	return len(drop), err
}

// encodeStorable encodes the PUBLISH of an in-flight message that has no copy
// in the offline queue, as it should be resent: the DUP flag is the resend's to
// set. It reports false for a message that cannot be encoded.
func encodeStorable(o *outbound) ([]byte, bool) {
	if o.publish == nil {
		return nil, false
	}
	p := *o.publish
	p.Dup = false
	raw, err := packet.Encode(&p)
	if err != nil {
		return nil, false
	}
	return raw, true
}

// restoreReceivedQoS2 puts back the identifiers of QoS 2 PUBLISH packets the
// previous broker had received and not released.
func (s *session) restoreReceivedQoS2(ids []uint16) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range ids {
		s.receivedQoS2[id] = struct{}{}
	}
}

// restoreInflight puts back one unacknowledged message. sendSeq is advanced as
// for a send, so the entries are resent in the order they are restored in.
func (s *session) restoreInflight(o *outbound) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, dup := s.inflight[o.packetID]; dup {
		return
	}
	s.sendSeq++
	o.seq = s.sendSeq
	s.inflight[o.packetID] = o
}
