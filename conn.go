package natsmqtt5

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/taumatix/natsmqtt5/packet"
)

// deliveryQueueDepth bounds how many outbound PUBLISH packets may be waiting
// for a single slow client before the broker starts dropping them. Without a
// bound, one client that stops reading would grow the broker's heap without
// limit.
const deliveryQueueDepth = 2048

// delivery is a message on its way to a client, after the subscription that
// matched it has been resolved.
type delivery struct {
	// sub is the subscription that earned the message. A delivery queued
	// before its subscription was removed is dropped rather than sent; see
	// deliverLoop.
	sub *subscription
	// id is the message's Mqtt5-Msg-Id, which ties its live delivery to its
	// offline-queue copy. Empty for a message published straight onto NATS.
	id string
	// seq is the stream sequence of the message's offline-queue copy, or 0
	// when it has none; see catchup.go.
	seq     uint64
	topic   string
	payload []byte
	qos     packet.QoS
	retain  bool
	props   *packet.Properties
	// arrived is when the broker took the message in, and expiry the Message
	// Expiry Interval it arrived with (nil for none); see expiry.go.
	arrived time.Time
	expiry  *uint32
	// quotaHeld is set when the send-quota slot for this delivery was taken
	// before it was queued, as a shared-subscription puller does.
	quotaHeld bool
	// ackSubject is the JetStream acknowledgement subject of the backlog message
	// a shared subscription's member pulled, which the in-flight entry carries
	// until the PUBACK; empty for anything else. held reports, once deliver has
	// returned, that the entry now carries it.
	ackSubject string
	held       bool
	// recordSeq is the queue sequence of a shared subscription's message, which
	// the session record keeps so that a restored session can send it again. It
	// is not seq, because a delivery with a seq is one the replay of the offline
	// queue has to account for (see session.noteDelivered), and the backlog
	// delivers its messages without it.
	recordSeq uint64
}

// conn is one MQTT network connection and the state that belongs to it rather
// than to the session: the topic alias map, the keep-alive clock and the
// write side of the socket (MQTT-5.0 §3.3.2.3.4 makes topic aliases
// per-connection explicitly).
type conn struct {
	broker *Broker
	nc     net.Conn
	r      *bufio.Reader
	logger *slog.Logger

	writeMu sync.Mutex

	sess *session
	// claimGen is the session-record claim this connection made. Only writes
	// presenting the session's current generation are allowed, so a connection
	// displaced by a later CONNECT cannot write over its successor's claim.
	claimGen uint64

	// clientMaxPacketSize is the client's Maximum Packet Size, or 0 for no
	// limit. A packet over it is discarded rather than sent [MQTT-3.1.2-25].
	clientMaxPacketSize uint32
	// clientReceiveMax bounds our in-flight QoS 1 and QoS 2 publications
	// towards the client (MQTT-5.0 §4.9).
	clientReceiveMax uint16
	// requestProblemInfo mirrors the client's Request Problem Information: at
	// 0 we must not attach a Reason String or User Properties to anything but
	// PUBLISH, CONNACK and DISCONNECT [MQTT-3.1.2-29].
	requestProblemInfo bool

	// aliases maps an inbound Topic Alias to the Topic Name it was set with.
	// It lives only for this network connection [MQTT-3.3.2-7].
	aliasMu sync.Mutex
	aliases map[uint16]string

	deliveries chan *delivery
	// replayed holds the ids replayOffline delivered from the offline queue,
	// so the live copy of one is skipped. Only the delivery goroutine uses it.
	replayed map[string]bool
	// replayedBefore is the previous catch-up's replayed set, kept one round
	// longer so a late live copy from it is still recognised.
	replayedBefore map[string]bool
	quota          chan struct{}

	// Catching up from the queue stream after falling behind; see
	// catchup.go. behind and fromSeq are guarded by catchMu; catchup wakes
	// the delivery goroutine.
	catchMu sync.Mutex
	behind  bool
	fromSeq uint64
	catchup chan struct{}

	// Where this connection stands in the queue stream, for the replay after
	// it ends; see awayFloor. All but loopDone belong to the delivery
	// goroutine, and finish reads them once loopDone is closed.
	//
	// inHand is the sequence of the delivery being sent, streamNext the
	// sequence the stream read (resume replay or catch-up round) has yet to
	// handle, 0 for none. resume is the replay's start, kept until it has
	// handled a message, so a connection that ends before that does not lose
	// the replay it was owed.
	//
	// They are atomic because the session checkpoint (checkpoint.go) reads them
	// while the connection lives.
	inHand     atomic.Uint64
	streamNext atomic.Uint64
	resume     atomic.Pointer[away]
	// pending is the queue sequences waiting in deliveries, for the checkpoint.
	pending seqSet
	// positioned is closed once the replay position the session was resumed with
	// has been taken (replayOffline); the checkpoint waits for it, because
	// writing the connection's own position sooner would overwrite the one it was
	// resumed with.
	positioned     chan struct{}
	positionedOnce sync.Once
	// activity counts deliveries enqueued and sent, so the checkpoint writes
	// only when something moved.
	activity atomic.Uint64
	loopDone chan struct{}
	// loopStarted is set by serve before it starts deliverLoop, which is the
	// goroutine that closes loopDone.
	loopStarted atomic.Bool
	// ended is closed once finish has handed the session what a replay needs.
	ended     chan struct{}
	endedOnce sync.Once

	// shared carries messages pulled from shared-subscription backlogs to the
	// delivery goroutine, and pulling records which subscriptions this
	// connection is pulling for; see shared.go.
	shared  chan *sharedItem
	pullMu  sync.Mutex
	pulling map[*subscription]bool

	// disconnectSent is set, under writeMu, once the server's DISCONNECT has
	// been written: nothing may follow it [MQTT-3.14.4-1].
	disconnectSent bool

	closeOnce sync.Once
	done      chan struct{}
	// connected is set once CONNACK with a success code has gone out. Until
	// then the broker must not send a DISCONNECT [MQTT-3.14.0-1].
	connected atomic.Bool
}

func newConn(b *Broker, nc net.Conn) *conn {
	return &conn{
		broker:     b,
		nc:         nc,
		r:          bufio.NewReaderSize(nc, 4096),
		logger:     b.logger.With("remote", nc.RemoteAddr().String()),
		aliases:    make(map[uint16]string),
		deliveries: make(chan *delivery, deliveryQueueDepth),
		replayed:   make(map[string]bool),
		shared:     make(chan *sharedItem),
		catchup:    make(chan struct{}, 1),
		loopDone:   make(chan struct{}),
		ended:      make(chan struct{}),
		positioned: make(chan struct{}),
		pulling:    make(map[*subscription]bool),
		done:       make(chan struct{}),
	}
}

func (c *conn) serve(ctx context.Context) {
	defer c.close()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-ctx.Done():
		case <-c.done:
		}
		c.nc.Close()
	}()

	keepAlive, err := c.handshake(ctx)
	if err != nil {
		c.logger.Debug("mqtt handshake failed", "error", err)
		// A handshake can fail after the session has been set up — a CONNACK
		// the client is no longer there to read, or stored subscriptions that
		// could not be restored. finish is a no-op before that point, and the
		// work it does after it is what stops a claimed session record
		// outliving the connection that claimed it.
		c.finish(err)
		return
	}
	c.logger = c.logger.With("client_id", c.sess.clientID)
	c.logger.Info("mqtt client connected")

	c.loopStarted.Store(true)
	go c.deliverLoop()
	if iv := c.broker.opts.SessionCheckpointInterval; iv > 0 && c.broker.store != nil && c.broker.queue != nil {
		go c.checkpointLoop(iv)
	}

	err = c.readLoop(ctx, keepAlive)
	c.finish(err)
}

// readLoop reads packets until the connection ends. keepAlive is the
// negotiated interval; the broker closes the connection if nothing arrives
// within 1.5 times it [MQTT-3.1.2-22].
func (c *conn) readLoop(ctx context.Context, keepAlive time.Duration) error {
	// "A Keep Alive value of 0 has the effect of turning off the Keep Alive
	// mechanism" (MQTT-5.0 §3.1.2.10). The deadline the handshake set to bound
	// the wait for CONNECT has to be cleared, or the connection would die when
	// it expires.
	if keepAlive == 0 {
		if err := c.nc.SetReadDeadline(time.Time{}); err != nil {
			return err
		}
	}

	for {
		if keepAlive > 0 {
			if err := c.nc.SetReadDeadline(time.Now().Add(keepAlive)); err != nil {
				return err
			}
		}
		pkt, err := packet.Read(c.r, c.broker.opts.maximumPacketSize)
		if err != nil {
			return c.readError(err)
		}
		// A displaced connection's buffer can still hold packets its client
		// sent before the takeover, and they are handled. That is deliberate
		// for acknowledgements: they come from the same client about messages
		// it received, and dropping one makes the successor resend a message
		// the client already acknowledged or refused (v0.4.2 tried, and
		// TestARefusedMessageIsNotResent caught it). Handlers that must not act
		// for a displaced connection check ownership themselves, as
		// handleUnsubscribe and subscribeOne do.
		if err := c.handle(ctx, pkt); err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

// readError turns a read failure into the DISCONNECT the spec prescribes,
// sends it, and returns the error that ends the connection.
func (c *conn) readError(err error) error {
	var ne net.Error
	switch {
	case errors.Is(err, io.EOF):
		return errClientGone
	case errors.As(err, &ne) && ne.Timeout():
		c.sendDisconnect(packet.KeepAliveTimeout, "no packet received within 1.5x the Keep Alive interval")
		return fmt.Errorf("keep alive timeout: %w", err)
	case errors.Is(err, packet.ErrPacketTooLarge):
		c.sendDisconnect(packet.PacketTooLarge, "packet exceeds the advertised Maximum Packet Size")
		return err
	case errors.Is(err, packet.ErrMalformed):
		c.sendDisconnect(packet.MalformedPacket, err.Error())
		return err
	case errors.Is(err, packet.ErrProtocol):
		c.sendDisconnect(packet.ProtocolError, err.Error())
		return err
	}
	return err
}

// errClientGone marks a connection the peer closed without a DISCONNECT, which
// is a normal outcome that must still publish the Will Message
// (MQTT-5.0 §3.1.2.5).
var errClientGone = errors.New("client closed the connection")

// errNormalDisconnect marks a clean DISCONNECT with reason 0x00, which deletes
// the Will Message [MQTT-3.1.2-8].
var errNormalDisconnect = errors.New("client disconnected normally")

// errConnClosed ends the read loop of a connection the broker has already
// closed, a displaced one for instance, before it handles anything it buffered.
var errConnClosed = errors.New("connection already closed")

func (c *conn) handle(ctx context.Context, pkt packet.Packet) error {
	switch p := pkt.(type) {
	case *packet.Connect:
		// "A Client can only send the CONNECT packet once over a Network
		// Connection" [MQTT-3.1.0-2].
		c.sendDisconnect(packet.ProtocolError, "a second CONNECT was received")
		return fmt.Errorf("second CONNECT from %s", c.sess.clientID)
	case *packet.Publish:
		return c.handlePublish(ctx, p)
	case *packet.Puback:
		return c.handlePuback(p)
	case *packet.Pubrec:
		return c.handlePubrec(p)
	case *packet.Pubrel:
		return c.handlePubrel(p)
	case *packet.Pubcomp:
		return c.handlePubcomp(p)
	case *packet.Subscribe:
		return c.handleSubscribe(ctx, p)
	case *packet.Unsubscribe:
		return c.handleUnsubscribe(p)
	case *packet.Pingreq:
		return c.write(&packet.Pingresp{})
	case *packet.Disconnect:
		return c.handleDisconnect(p)
	case *packet.Auth:
		// The broker advertises no authentication method, so an AUTH can only
		// be out of sequence (MQTT-5.0 §4.12).
		c.sendDisconnect(packet.ProtocolError, "this broker does not support enhanced authentication")
		return errors.New("unexpected AUTH")
	default:
		c.sendDisconnect(packet.ProtocolError, "unexpected packet type "+pkt.Type().String())
		return fmt.Errorf("unexpected %s from client", pkt.Type())
	}
}

func (c *conn) handleDisconnect(p *packet.Disconnect) error {
	// A client may extend, but never lengthen beyond zero, its session expiry
	// at disconnect time: "If the Session Expiry Interval in the CONNECT was
	// 0, then it is a Protocol Error to set a non-zero value here"
	// (MQTT-5.0 §3.14.2.2.2).
	if p.Properties != nil && p.Properties.SessionExpiryInterval != nil {
		if c.sess.expiry() == 0 && *p.Properties.SessionExpiryInterval != 0 {
			c.sendDisconnect(packet.ProtocolError,
				"cannot set a non-zero Session Expiry Interval on DISCONNECT when CONNECT requested 0")
			return errors.New("illegal Session Expiry Interval on DISCONNECT")
		}
		c.sess.setExpiry(c.cappedExpiry(*p.Properties.SessionExpiryInterval))
	}

	if p.ReasonCode == packet.DisconnectWithWillMessage {
		// The client wants the Will published even though it is disconnecting
		// cleanly (MQTT-5.0 §3.14.2.1, reason 0x04).
		return errClientGone
	}
	return errNormalDisconnect
}

// finish runs the end-of-connection work: publish or drop the Will, then
// either keep the session for a later reconnect or release it.
func (c *conn) finish(cause error) {
	if c.sess == nil {
		return
	}
	if errors.Is(cause, errNormalDisconnect) {
		// A DISCONNECT with reason 0x00 deletes the Will Message
		// [MQTT-3.1.2-8].
		_, _, lease := c.sess.takeWill()
		c.broker.wills.drop(lease)
	} else {
		c.scheduleWill()
	}

	// Worked out before the session lets go of the connection, and recorded
	// with it, so a successor never finds the session without the replay it
	// is owed; see session.awaitPredecessor.
	defer c.markEnded()
	var a *away
	if c.broker.queue != nil && c.sess.expiry() > 0 {
		floor := c.awayFloor()
		a = &floor
	} else if c.loopStarted.Load() {
		// No replay position to record, but a successor still resends what the
		// session holds, and that is only settled once this connection's
		// delivery goroutine has stopped tracking messages.
		c.close()
		<-c.loopDone
	}
	c.sess.detach(c, a)
	c.markEnded()
	// Hand the durable record back before tearing the session down, so the
	// snapshot it writes still describes the session that existed.
	c.broker.releaseStoredSession(c)
	if c.sess.expiry() == 0 {
		c.broker.leaveGroups(c.sess, c.sess.discard())
		c.broker.releaseSession(c.sess)
	}
	c.logger.Info("mqtt client disconnected", "cause", causeString(cause))
}

func causeString(err error) string {
	switch {
	case err == nil:
		return "closed"
	case errors.Is(err, errNormalDisconnect):
		return "normal disconnect"
	case errors.Is(err, errClientGone):
		return "client gone"
	}
	return err.Error()
}

// scheduleWill publishes the Will Message, honouring the Will Delay Interval:
// the server waits for the delay or the end of the session, whichever comes
// first, and cancels if a new connection claims the session in the meantime
// [MQTT-3.1.3-9].
func (c *conn) scheduleWill() {
	will, delay, lease := c.sess.takeWill()
	if will == nil {
		return
	}
	wills := c.broker.wills

	// "The Server delays publishing the Client's Will Message until the Will
	// Delay Interval has passed or the Session ends, whichever happens first"
	// (MQTT-5.0 §3.1.3.2.2). A Session Expiry Interval of zero ends the session
	// as soon as the connection does, so the delay collapses to nothing however
	// long the client asked for.
	sess := c.sess
	if expiry := time.Duration(sess.expiry()) * time.Second; expiry < delay {
		delay = expiry
	}
	if delay == 0 {
		if wills.fire(lease) {
			c.broker.publishWill(sess, will)
		}
		return
	}

	// The stored copy now says the connection is over, so that a broker which
	// outlives this one can tell a Will waiting out its delay from a live
	// connection's.
	if !wills.ended(lease, time.Now().Add(delay)) {
		return
	}

	// Held where Broker.Reauthorize can find it, so a Will whose permission
	// is revoked during the delay can be discarded before it fires.
	sess.setPendingWill(will, lease)
	go func() {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-c.broker.closing():
		}
		if sess.hasConn() {
			// The session was resumed before the delay elapsed
			// [MQTT-3.1.3-9].
			wills.drop(lease)
			return
		}
		if !sess.takePendingWill(will) {
			// Discarded by Broker.Reauthorize, or cancelled by a resumption
			// that has since ended in a disconnect of its own.
			return
		}
		if wills.fire(lease) {
			c.broker.publishWill(sess, will)
		}
	}()
}

// write serialises a packet onto the socket. It enforces the client's Maximum
// Packet Size: an oversized packet is discarded and the broker behaves as if
// it had been sent [MQTT-3.1.2-25].
func (c *conn) write(p packet.Packet) error {
	_, err := c.writeReportingDiscard(p)
	return err
}

// writePublish is write for a PUBLISH at QoS 1 or 2, reporting whether the
// packet reached the socket.
//
// The caller has to know, because a discarded one will never be acknowledged.
// "Where a Packet is too large to send, the Server MUST discard it without
// sending it and then behave as if it had completed sending that Application
// Message" [MQTT-3.1.2-25] — so the exchange ends there. Waiting for the
// acknowledgement instead would strand a Packet Identifier and a send-quota
// slot for the life of the session, and a session that is resumed would resend
// the same undeliverable packet on every resumption.
func (c *conn) writePublish(p *packet.Publish) (sent bool, err error) {
	discarded, err := c.writeReportingDiscard(p)
	return !discarded, err
}

func (c *conn) writeReportingDiscard(p packet.Packet) (discarded bool, err error) {
	return c.writePacket(p, false)
}

// writePacket is the one place a packet reaches the socket. final marks the
// server's DISCONNECT: once it has been written, or decided against, no later
// packet may follow it on this connection [MQTT-3.14.4-1].
func (c *conn) writePacket(p packet.Packet, final bool) (discarded bool, err error) {
	raw, err := packet.Encode(p)
	if err != nil {
		return false, fmt.Errorf("encoding %s: %w", p.Type(), err)
	}
	if c.clientMaxPacketSize > 0 && uint32(len(raw)) > c.clientMaxPacketSize {
		// A Reason String or User Property "MUST NOT" be sent if it would take
		// the packet past the limit [MQTT-3.2.2-19], [MQTT-3.2.2-20],
		// [MQTT-3.4.2-2], [MQTT-3.5.2-2], [MQTT-3.14.2-3]. The rest of the packet
		// is still owed: a lost PUBACK or PUBREC leaves the client's exchange
		// unfinished.
		if bare, ok := withoutOptionalProperties(p); ok {
			if bareRaw, err := packet.Encode(bare); err == nil && uint32(len(bareRaw)) <= c.clientMaxPacketSize {
				p, raw = bare, bareRaw
			}
		}
	}
	if c.clientMaxPacketSize > 0 && uint32(len(raw)) > c.clientMaxPacketSize {
		// [MQTT-3.1.2-24]: the server must not send it.
		c.logger.Warn("discarding a packet larger than the client's Maximum Packet Size",
			"type", p.Type().String(), "size", len(raw), "limit", c.clientMaxPacketSize)
		if final {
			// Not sent, but the broker has decided to end the connection.
			c.writeMu.Lock()
			c.disconnectSent = true
			c.writeMu.Unlock()
		}
		return true, nil
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	select {
	case <-c.done:
		return false, net.ErrClosed
	default:
	}
	if c.disconnectSent {
		// The delivery goroutine and the read loop write independently, so a
		// PUBLISH can be waiting on the lock while the DISCONNECT goes out.
		return false, net.ErrClosed
	}
	if final {
		c.disconnectSent = true
	}
	if _, err := c.nc.Write(raw); err != nil {
		return false, fmt.Errorf("writing %s: %w", p.Type(), err)
	}
	return false, nil
}

// withoutOptionalProperties returns a copy of an acknowledgement-shaped packet
// with its Reason String and User Properties removed, the two properties the
// specification forbids sending when they would exceed the client's Maximum
// Packet Size. ok is false for a packet that has nothing to drop.
func withoutOptionalProperties(p packet.Packet) (packet.Packet, bool) {
	strip := func(in *packet.Properties) (*packet.Properties, bool) {
		if in == nil || (in.ReasonString == "" && len(in.User) == 0) {
			return nil, false
		}
		out := *in
		out.ReasonString, out.User = "", nil
		return &out, true
	}
	switch v := p.(type) {
	case *packet.Connack:
		if props, ok := strip(v.Properties); ok {
			out := *v
			out.Properties = props
			return &out, true
		}
	case *packet.Puback:
		if props, ok := strip(v.Properties); ok {
			out := *v
			out.Properties = props
			return &out, true
		}
	case *packet.Pubrec:
		if props, ok := strip(v.Properties); ok {
			out := *v
			out.Properties = props
			return &out, true
		}
	case *packet.Pubrel:
		if props, ok := strip(v.Properties); ok {
			out := *v
			out.Properties = props
			return &out, true
		}
	case *packet.Pubcomp:
		if props, ok := strip(v.Properties); ok {
			out := *v
			out.Properties = props
			return &out, true
		}
	case *packet.Disconnect:
		if props, ok := strip(v.Properties); ok {
			out := *v
			out.Properties = props
			return &out, true
		}
	}
	return nil, false
}

// sendDisconnect sends a server DISCONNECT, best effort. It is a no-op before
// a successful CONNACK, which the spec forbids following [MQTT-3.14.0-1].
func (c *conn) sendDisconnect(code packet.ReasonCode, reason string) {
	if !c.connected.Load() {
		return
	}
	d := &packet.Disconnect{ReasonCode: code}
	// A Reason String is always permitted on DISCONNECT, whatever Request
	// Problem Information said [MQTT-3.1.2-29].
	if reason != "" {
		d.Properties = &packet.Properties{ReasonString: reason}
	}
	_, _ = c.writePacket(d, true)
}

// shutdown disconnects the client with a reason and closes the socket.
func (c *conn) shutdown(code packet.ReasonCode, reason string) {
	c.sendDisconnect(code, reason)
	c.close()
}

// markEnded releases whoever waits in session.awaitPredecessor.
func (c *conn) markEnded() {
	c.endedOnce.Do(func() { close(c.ended) })
}

func (c *conn) close() {
	c.closeOnce.Do(func() {
		close(c.done)
		c.nc.Close()
	})
}

// tlsState returns the TLS handshake state, or nil on a plaintext connection.
func (c *conn) tlsState() *tls.ConnectionState {
	tc, ok := c.nc.(*tls.Conn)
	if !ok {
		return nil
	}
	st := tc.ConnectionState()
	return &st
}

// cappedExpiry applies Options.MaxSessionExpiry. The broker returns the capped
// value in CONNACK so the client knows what it actually got
// (MQTT-5.0 §3.2.2.3.2).
func (c *conn) cappedExpiry(requested uint32) uint32 {
	max := uint32(c.broker.opts.maxSessionExpiry / time.Second)
	if requested > max {
		return max
	}
	return requested
}
