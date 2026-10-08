package natsmqtt5

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

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
}

// outbound tracks a QoS 1 or QoS 2 message the broker has sent to the client
// and is still waiting to have acknowledged (MQTT-5.0 §4.3).
type outbound struct {
	packetID uint16
	qos      packet.QoS
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

	// nextPacketID cycles 1..65535 for broker-to-client packets
	// (MQTT-5.0 §2.2.1).
	nextPacketID uint16
	inflight     map[uint16]*outbound
	// sendSeq numbers sends so that unacknowledged returns the in-flight set in
	// the order it left.
	sendSeq uint64
	// receivedQoS2 holds the Packet Identifiers of QoS 2 PUBLISH packets that
	// have been accepted and not yet released, so a redelivered PUBLISH is
	// acknowledged without being forwarded twice (MQTT-5.0 §4.3.3).
	receivedQoS2 map[uint16]struct{}
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
	// awayAt is when the session's last connection ended, for the offline
	// queue's replay; zero when there is nothing to replay.
	awayAt time.Time
	// awayRestored marks an awayAt read from the session store. The ids the
	// previous connection delivered are not stored with it, so its replay
	// cannot rewind past them; see replayOffline.
	awayRestored bool
	// awayFromSeq is the lowest offline-queue sequence the last connection had
	// not delivered when it ended, or 0 when it did not note one. A replay
	// starts there rather than at a time; see conn.awayFloor.
	awayFromSeq uint64
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
}

func newSession(clientID string) *session {
	return &session{
		clientID:     clientID,
		subs:         make(map[string]*subscription),
		inflight:     make(map[uint16]*outbound),
		receivedQoS2: make(map[uint16]struct{}),
		withdrawn:    make(map[uint16]withdrawal),
		resent:       make(map[uint16]ackOwed),
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

// discard marks the session unusable and tears down its NATS subscriptions.
func (s *session) discard() {
	s.mu.Lock()
	subs := s.subs
	s.subs = make(map[string]*subscription)
	s.discarded = true
	s.mu.Unlock()

	for _, sub := range subs {
		unsubscribeAll(sub)
	}
}

func unsubscribeAll(sub *subscription) {
	for _, ns := range sub.natsSubs {
		_ = ns.Unsubscribe()
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
// is the one owed for it, and forgets the record if so.
func (s *session) forgetResent(c *conn, id uint16, t packet.Type) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.resent[id]
	if !ok || r.conn != c || r.typ != t {
		return false
	}
	delete(s.resent, id)
	return true
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
func (s *session) completeInflight(c *conn, id uint16) (outbound, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.inflight[id]
	if !ok {
		return outbound{}, false
	}
	delete(s.inflight, id)
	done := *o
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
	return duplicate
}

// releaseQoS2 clears a Packet Identifier on PUBREL and reports whether it was
// outstanding. A PUBREL for an unknown identifier is answered with 0x92
// (Packet Identifier not found) (MQTT-5.0 §3.6.2.1).
func (s *session) releaseQoS2(id uint16) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.receivedQoS2[id]
	delete(s.receivedQoS2, id)
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
func (s *session) installSubscription(c *conn, sub *subscription) (old *subscription, installed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != c {
		return nil, false
	}
	old = s.subs[sub.filter]
	if old != nil {
		old.live.Store(false)
	}
	s.subs[sub.filter] = sub
	sub.live.Store(true)
	return old, true
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

func (s *session) setWill(w *packet.Will, delay time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.will, s.willDelay = w, delay
}

// takeWill removes and returns the Will Message, so it can only ever be
// published once.
func (s *session) takeWill() (*packet.Will, time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w, d := s.will, s.willDelay
	s.will, s.willDelay = nil, 0
	return w, d
}

// markAwayLocked records that the last connection ended. A replay no connection has
// taken yet is still owed, and is kept: the earlier start wins, and the lower
// of two sequences.
func (s *session) markAwayLocked(a away) {
	if !s.awayAt.IsZero() {
		a.at, a.restored = s.awayAt, s.awayRestored
		switch {
		case s.awayFromSeq == 0:
			a.fromSeq = 0
		case a.fromSeq == 0 || s.awayFromSeq < a.fromSeq:
			a.fromSeq = s.awayFromSeq
		}
	}
	s.awayAt, s.awayRestored, s.awayFromSeq = a.at, a.restored, a.fromSeq
}

// markAwayRestored records the absence a session store record describes: when
// the session was released, and, if the record has them, the sequence the
// replay starts at and the ids delivered at or above it.
func (s *session) markAwayRestored(at time.Time, fromSeq uint64, delivered []storedDelivered) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.awayAt, s.awayRestored, s.awayFromSeq = at, true, fromSeq
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
func (s *session) awayState() (fromSeq uint64, delivered []storedDelivered, dropped int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.awayFromSeq == 0 {
		return 0, nil, 0
	}
	for id, m := range s.delivered {
		if m.seq >= s.awayFromSeq {
			delivered = append(delivered, storedDelivered{ID: id, Seq: m.seq})
		}
	}
	sort.Slice(delivered, func(i, j int) bool { return delivered[i].Seq < delivered[j].Seq })
	if len(delivered) > maxStoredDelivered {
		dropped = len(delivered) - maxStoredDelivered
		delivered = delivered[:maxStoredDelivered]
	}
	return s.awayFromSeq, delivered, dropped
}

// away is what takeAway hands the replay.
type away struct {
	at       time.Time
	restored bool // read from the session store; see session.awayRestored
	fromSeq  uint64
}

// takeAway returns when the last connection ended, once.
func (s *session) takeAway() (a away, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a = away{at: s.awayAt, restored: s.awayRestored, fromSeq: s.awayFromSeq}
	s.awayAt, s.awayRestored, s.awayFromSeq = time.Time{}, false, 0
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
			if now.Sub(m.at) > 2*offlineRewind && (m.seq == 0 || m.seq+deliveredSeqWindow < s.maxDeliveredSeq) {
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

func (s *session) setPendingWill(w *packet.Will) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pendingWill = w
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

// discardWill drops w, wherever it is held, so it is never published.
func (s *session) discardWill(w *packet.Will) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.will == w {
		s.will, s.willDelay = nil, 0
	}
	if s.pendingWill == w {
		s.pendingWill = nil
	}
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
	return &cp, s.rev
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
// left out because they have no copy in the offline queue to be read back, so a
// restored session could not resend them.
func (s *session) inflightState() (inflight []storedInflight, received []uint16, unrecorded int) {
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
			st.Retain = o.publish.Retain
			if p := o.publish.Properties; p != nil && len(p.SubscriptionIdentifiers) > 0 {
				st.SubID = p.SubscriptionIdentifiers[0]
			}
		default:
			unrecorded++
			continue
		}
		inflight = append(inflight, st)
	}
	for id := range s.receivedQoS2 {
		received = append(received, id)
	}
	s.mu.Unlock()
	sort.Slice(received, func(i, j int) bool { return received[i] < received[j] })
	return inflight, received, unrecorded
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
