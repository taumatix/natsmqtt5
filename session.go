package natsmqtt5

import (
	"sort"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

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
	// seq counts sends, so the in-flight set can be put back in the order it
	// went out. Packet Identifiers cannot do that job: they cycle through
	// 1..65535 and wrap.
	seq uint64
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
	withdrawn map[uint16]struct{}

	will          *packet.Will
	willDelay     time.Duration
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
		withdrawn:    make(map[uint16]struct{}),
	}
}

// attach binds a connection to the session, returning the connection it
// displaced, if any.
func (s *session) attach(c *conn) *conn {
	s.mu.Lock()
	defer s.mu.Unlock()
	prev := s.conn
	s.conn = c
	s.disconnectedAt = time.Time{}

	// "The send quota and Receive Maximum value are not preserved across
	// Network Connections, and are re-initialized with each new Network
	// Connection ... They are not part of the session state" (MQTT-5.0 §4.9).
	// So nothing carried into this connection holds a slot in its quota until a
	// resend takes one. This runs during the handshake, before the CONNACK, so
	// no acknowledgement can be in flight against the new quota yet.
	for _, o := range s.inflight {
		o.quotaHeld = false
	}
	return prev
}

// detach unbinds the connection and starts the expiry clock.
func (s *session) detach(c *conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn == c {
		s.conn = nil
		s.disconnectedAt = time.Now()
	}
}

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
func (s *session) nextID() (uint16, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := 0; i < 0xFFFF; i++ {
		s.nextPacketID++
		if s.nextPacketID == 0 {
			s.nextPacketID = 1
		}
		if _, busy := s.inflight[s.nextPacketID]; !busy {
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
func (s *session) takeQuotaSlot(id uint16) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.inflight[id]
	if ok {
		o.quotaHeld = true
	}
	return ok
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
func (s *session) completeInflight(id uint16) (outbound, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.inflight[id]
	if !ok {
		return outbound{}, false
	}
	delete(s.inflight, id)
	return *o, true
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

// withdrawInflight takes back the unacknowledged messages whose Topic Name
// matches none of filters, and returns the Packet Identifiers it took. It is
// how a subscription denied on resume stops the payloads it earned from being
// put back on the wire by the retransmission that follows.
//
// An entry past its PUBREC is left alone. The client took ownership of that
// message when it sent the PUBREC [MQTT-4.3.3-8], so what is outstanding is a
// PUBREL carrying no payload; withholding it would leave the client waiting for
// a PUBCOMP forever, and its Packet Identifier unusable, to prevent a
// disclosure that has already happened.
//
// It runs during the handshake, where attach has just cleared every entry's
// claim on the send quota and the delivery goroutine has not started, so no
// entry it removes is holding a slot that would have to be returned.
func (s *session) withdrawInflight(filters []string) []uint16 {
	s.mu.Lock()
	defer s.mu.Unlock()

	var taken []uint16
	for id, o := range s.inflight {
		if o.awaitingPubcomp || matchesAny(filters, o.publish.Topic) {
			continue
		}
		delete(s.inflight, id)
		s.withdrawn[id] = struct{}{}
		taken = append(taken, id)
	}
	return taken
}

// forgetWithdrawn reports whether id names a message the broker took back, and
// forgets it if so. The acknowledgement that asks is the last one owed for it.
func (s *session) forgetWithdrawn(id uint16) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.withdrawn[id]
	delete(s.withdrawn, id)
	return ok
}

func matchesAny(filters []string, name string) bool {
	for _, f := range filters {
		if topic.Match(f, name) {
			return true
		}
	}
	return false
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

func (s *session) putSubscription(sub *subscription) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subs[sub.filter] = sub
}

func (s *session) removeSubscription(filter string) (*subscription, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sub, ok := s.subs[filter]
	if ok {
		delete(s.subs, filter)
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
