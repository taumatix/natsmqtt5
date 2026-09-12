package natsmqtt5

import (
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/taumatix/natsmqtt5/packet"
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
}

// session is the MQTT Session State the broker holds for a Client Identifier
// (MQTT-5.0 §4.1).
//
// v0.1.0 keeps sessions in the broker process. They survive a client
// reconnecting to the same broker within the Session Expiry Interval, but not
// a broker restart, and they are not shared between brokers. Subscriptions and
// retained messages do travel through NATS, so a client reconnecting to a
// different broker still reaches the same publishers and subscribers; it just
// has to re-subscribe.
type session struct {
	clientID string
	identity string
	username string

	mu sync.Mutex
	// conn is the connection currently serving this session, or nil while the
	// session is disconnected but not yet expired.
	conn *conn

	subs map[string]*subscription

	// nextPacketID cycles 1..65535 for broker-to-client packets
	// (MQTT-5.0 §2.2.1).
	nextPacketID uint16
	inflight     map[uint16]*outbound
	// receivedQoS2 holds the Packet Identifiers of QoS 2 PUBLISH packets that
	// have been accepted and not yet released, so a redelivered PUBLISH is
	// acknowledged without being forwarded twice (MQTT-5.0 §4.3.3).
	receivedQoS2 map[uint16]struct{}

	will          *packet.Will
	willDelay     time.Duration
	expirySeconds uint32
	// disconnectedAt is when the connection went away, used with
	// expirySeconds to decide whether the session may still be resumed.
	disconnectedAt time.Time
	// discarded marks a session that must not be resumed.
	discarded bool
}

func newSession(clientID string) *session {
	return &session{
		clientID:     clientID,
		subs:         make(map[string]*subscription),
		inflight:     make(map[uint16]*outbound),
		receivedQoS2: make(map[uint16]struct{}),
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
	s.inflight[o.packetID] = o
}

// completeInflight removes the entry for id and reports whether one existed.
func (s *session) completeInflight(id uint16) (*outbound, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.inflight[id]
	if ok {
		delete(s.inflight, id)
	}
	return o, ok
}

func (s *session) inflightEntry(id uint16) (*outbound, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.inflight[id]
	return o, ok
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
