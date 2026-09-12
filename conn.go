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
	topic   string
	payload []byte
	qos     packet.QoS
	retain  bool
	props   *packet.Properties
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
	quota      chan struct{}

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
		return
	}
	c.logger = c.logger.With("client_id", c.sess.clientID)
	c.logger.Info("mqtt client connected")

	go c.deliverLoop()

	err = c.readLoop(ctx, keepAlive)
	c.finish(err)
}

// readLoop reads packets until the connection ends. keepAlive is the
// negotiated interval; the broker closes the connection if nothing arrives
// within 1.5 times it [MQTT-3.1.2-22].
func (c *conn) readLoop(ctx context.Context, keepAlive time.Duration) error {
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
		c.sess.takeWill()
	} else {
		c.scheduleWill()
	}

	c.sess.detach(c)
	if c.sess.expiry() == 0 {
		c.sess.discard()
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
	will, delay := c.sess.takeWill()
	if will == nil {
		return
	}
	if delay == 0 {
		c.broker.publishWill(c.sess, will)
		return
	}

	sess := c.sess
	expiry := time.Duration(sess.expiry()) * time.Second
	if expiry > 0 && delay > expiry {
		delay = expiry
	}
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
			return
		}
		c.broker.publishWill(sess, will)
	}()
}

// write serialises a packet onto the socket. It enforces the client's Maximum
// Packet Size: an oversized packet is discarded and the broker behaves as if
// it had been sent [MQTT-3.1.2-25].
func (c *conn) write(p packet.Packet) error {
	raw, err := packet.Encode(p)
	if err != nil {
		return fmt.Errorf("encoding %s: %w", p.Type(), err)
	}
	if c.clientMaxPacketSize > 0 && uint32(len(raw)) > c.clientMaxPacketSize {
		c.logger.Warn("discarding a packet larger than the client's Maximum Packet Size",
			"type", p.Type().String(), "size", len(raw), "limit", c.clientMaxPacketSize)
		return nil
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	select {
	case <-c.done:
		return net.ErrClosed
	default:
	}
	if _, err := c.nc.Write(raw); err != nil {
		return fmt.Errorf("writing %s: %w", p.Type(), err)
	}
	return nil
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
	_ = c.write(d)
}

// shutdown disconnects the client with a reason and closes the socket.
func (c *conn) shutdown(code packet.ReasonCode, reason string) {
	c.sendDisconnect(code, reason)
	c.close()
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
