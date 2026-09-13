package natsmqtt5

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/taumatix/natsmqtt5/packet"
)

// Broker is an MQTT v5 broker backed by a NATS server. Create one with New,
// run it with Serve, and release its resources with Close.
//
// A Broker is safe for concurrent use.
type Broker struct {
	opts *resolved

	nc     *nats.Conn
	ownsNC bool
	js     jetstream.JetStream
	retain *retainedStore
	// store is nil unless Options.PersistentSessions is set.
	store  *sessionStore
	logger *slog.Logger

	listener net.Listener

	mu       sync.Mutex
	sessions map[string]*session
	conns    map[*conn]struct{}
	closed   bool
	// shutdown closes when Close is called, so background work such as a
	// delayed Will publication stops rather than firing into a dead broker.
	shutdown chan struct{}

	// serveDone closes when Serve returns, so Close can wait for the accept
	// loop rather than racing it.
	wg sync.WaitGroup
}

// New connects to NATS, provisions the JetStream assets the broker needs and
// binds the MQTT listener. It does not accept connections until Serve is
// called, so a caller can read ListenAddr first.
//
// The returned Broker must be closed with Close.
func New(opts Options) (*Broker, error) {
	return NewWithContext(context.Background(), opts)
}

// NewWithContext is New with a context bounding the NATS connection and
// JetStream setup.
func NewWithContext(ctx context.Context, opts Options) (*Broker, error) {
	r, err := opts.resolve()
	if err != nil {
		return nil, err
	}

	b := &Broker{
		opts:     r,
		logger:   r.logger,
		sessions: make(map[string]*session),
		conns:    make(map[*conn]struct{}),
		shutdown: make(chan struct{}),
	}

	if r.Conn != nil {
		b.nc = r.Conn
	} else {
		natsOpts := append([]nats.Option{nats.Name("natsmqtt5")}, r.NATSOptions...)
		nc, err := nats.Connect(r.NATSURL, natsOpts...)
		if err != nil {
			return nil, fmt.Errorf("natsmqtt5: connecting to NATS at %s: %w", r.NATSURL, err)
		}
		b.nc = nc
		b.ownsNC = true
	}

	if !r.DisableRetained || r.PersistentSessions {
		js, err := jetstream.New(b.nc)
		if err != nil {
			b.closeNATS()
			return nil, fmt.Errorf("natsmqtt5: initialising JetStream: %w", err)
		}
		b.js = js
	}
	if !r.DisableRetained {
		var err error
		if b.retain, err = newRetainedStore(ctx, b.js, r, b.logger); err != nil {
			b.closeNATS()
			return nil, fmt.Errorf("natsmqtt5: setting up the retained-message store: %w", err)
		}
	}
	if r.PersistentSessions {
		var err error
		if b.store, err = newSessionStore(ctx, b.js, b.nc, r, b.logger, b.sessionLost); err != nil {
			b.closeRetain()
			b.closeNATS()
			return nil, fmt.Errorf("natsmqtt5: setting up the session store: %w", err)
		}
	}

	if err := b.listen(); err != nil {
		b.closeSessionStore()
		b.closeRetain()
		b.closeNATS()
		return nil, err
	}
	return b, nil
}

func (b *Broker) listen() error {
	ln := b.opts.Listener
	if ln == nil {
		var err error
		ln, err = net.Listen("tcp", b.opts.Listen)
		if err != nil {
			return fmt.Errorf("natsmqtt5: listening on %s: %w", b.opts.Listen, err)
		}
	}
	if b.opts.TLSConfig != nil {
		ln = tls.NewListener(ln, b.opts.TLSConfig)
	}
	b.listener = ln
	return nil
}

// ListenAddr reports the address the broker accepts MQTT connections on. It is
// valid as soon as New returns, which lets a test bind :0 and discover the
// port.
func (b *Broker) ListenAddr() net.Addr { return b.listener.Addr() }

// NATS returns the NATS connection the broker uses, so an embedding
// application can share it rather than opening a second one.
func (b *Broker) NATS() *nats.Conn { return b.nc }

// Serve accepts MQTT connections until ctx is cancelled or Close is called.
// It returns nil on a clean shutdown.
func (b *Broker) Serve(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Unblock Accept when the context is cancelled.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			b.listener.Close()
		case <-stop:
		}
	}()

	b.logger.Info("mqtt broker listening",
		"addr", b.listener.Addr().String(),
		"nats", b.nc.ConnectedUrl(),
		"subject_prefix", b.opts.SubjectPrefix)

	for {
		nc, err := b.listener.Accept()
		if err != nil {
			if b.isClosed() || ctx.Err() != nil {
				b.drainConns()
				return nil
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			b.drainConns()
			return fmt.Errorf("natsmqtt5: accept: %w", err)
		}

		c := newConn(b, nc)
		b.trackConn(c, true)
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			defer b.trackConn(c, false)
			c.serve(ctx)
		}()
	}
}

// Close stops the broker: it closes the listener, disconnects every client
// with 0x8B (Server shutting down) and releases the NATS resources it owns. A
// NATS connection passed in through Options.Conn is left open.
func (b *Broker) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	close(b.shutdown)
	b.mu.Unlock()

	if b.listener != nil {
		b.listener.Close()
	}
	b.drainConns()
	// Waiting for the connections means every session has been through finish
	// and has released its stored record, so a broker that shuts down cleanly
	// leaves no record claiming it is still being served.
	b.wg.Wait()
	b.closeSessionStore()
	b.closeRetain()
	b.closeNATS()
	return nil
}

// closing is closed when the broker starts shutting down.
func (b *Broker) closing() <-chan struct{} { return b.shutdown }

func (b *Broker) isClosed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closed
}

func (b *Broker) trackConn(c *conn, add bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if add {
		b.conns[c] = struct{}{}
		return
	}
	delete(b.conns, c)
}

func (b *Broker) drainConns() {
	b.mu.Lock()
	conns := make([]*conn, 0, len(b.conns))
	for c := range b.conns {
		conns = append(conns, c)
	}
	b.mu.Unlock()

	for _, c := range conns {
		c.shutdown(packet.ServerShuttingDown, "broker shutting down")
	}
}

func (b *Broker) closeSessionStore() {
	if b.store != nil {
		b.store.close()
		b.store = nil
	}
}

func (b *Broker) closeRetain() {
	if b.retain != nil {
		b.retain.close()
		b.retain = nil
	}
}

func (b *Broker) closeNATS() {
	if b.ownsNC && b.nc != nil {
		b.nc.Drain()
		b.nc = nil
	}
}

// retainAvailable reports whether the broker supports retained messages, which
// is advertised in CONNACK (MQTT-5.0 §3.2.2.3.5).
func (b *Broker) retainAvailable() bool { return b.retain != nil }

// takeOverSession implements step 1 of MQTT-5.0 §3.1.4: a CONNECT using a
// Client Identifier that is already connected displaces the existing
// connection with 0x8E (Session taken over) [MQTT-3.1.4-3].
//
// It returns the stored session when the new connection may resume it, that is
// when the client did not ask for a clean start and the stored session had not
// expired.
func (b *Broker) takeOverSession(clientID string, cleanStart bool) (*session, bool) {
	b.mu.Lock()
	existing, ok := b.sessions[clientID]
	b.mu.Unlock()

	if ok {
		existing.takeOver()
	}

	if cleanStart || !ok || existing.expired() {
		if ok {
			existing.discard()
		}
		b.mu.Lock()
		delete(b.sessions, clientID)
		b.mu.Unlock()
		return nil, false
	}
	return existing, true
}

func (b *Broker) registerSession(s *session) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.sessions[s.clientID] = s
}

// releaseSession drops a session once its connection has gone and it cannot be
// resumed, i.e. its Session Expiry Interval is zero (MQTT-5.0 §3.1.2.11.2).
func (b *Broker) releaseSession(s *session) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if cur, ok := b.sessions[s.clientID]; ok && cur == s {
		delete(b.sessions, s.clientID)
	}
}

// persistsSessions reports whether session state is mirrored into JetStream.
func (b *Broker) persistsSessions() bool { return b.store != nil }

// persistSession writes the session's current subscription set to JetStream.
//
// A failure is logged and not reported to the client. The subscriptions are
// live on this broker either way, so refusing them would turn a JetStream
// hiccup into an outage for a client that would otherwise be served exactly as
// a broker without persistence would serve it. The one failure that matters —
// the compare-and-swap losing to another broker — is already being handled by
// the bucket watcher, which disconnects this client with 0x8E.
func (b *Broker) persistSession(c *conn) {
	if b.store == nil {
		return
	}
	s := c.sess
	s.persistMu.Lock()
	defer s.persistMu.Unlock()

	rec, rev := s.snapshot(c.claimGen)
	if rec == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), sessionOpTimeout)
	defer cancel()

	newRev, err := b.store.save(ctx, rec, rev)
	if err != nil {
		b.logger.Warn("could not store the session state",
			"client_id", s.clientID, "error", err)
		return
	}
	s.commitRecord(c.claimGen, rec, newRev)
}

// releaseStoredSession hands the record back when the connection ends, so that
// the next broker to read it knows nobody is serving the session and when it
// stops being resumable.
//
// A connection that was displaced rather than closed has nothing to hand back:
// snapshot refuses it, because the claim it wrote under is no longer the
// current one.
func (b *Broker) releaseStoredSession(c *conn) {
	if b.store == nil {
		return
	}
	s := c.sess
	s.persistMu.Lock()
	defer s.persistMu.Unlock()

	rec, rev := s.snapshot(c.claimGen)
	if rec == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), sessionOpTimeout)
	defer cancel()

	newRev, err := b.store.release(ctx, rec, rev)
	if err != nil {
		// Losing the compare-and-swap here is the ordinary outcome of a client
		// that moved to another broker: that broker owns the record now and
		// there is nothing to hand back.
		b.logger.Debug("could not release the stored session",
			"client_id", s.clientID, "error", err)
		return
	}
	s.commitRecord(c.claimGen, rec, newRev)
}

// sessionLost runs when another broker claims a Client Identifier this one is
// holding. The client is told 0x8E [MQTT-3.1.4-3] and the session is torn down,
// which also unsubscribes it from NATS — without that, a session that moved
// away would leave this broker subscribed to its subjects for good.
func (b *Broker) sessionLost(clientID string) {
	b.mu.Lock()
	s, ok := b.sessions[clientID]
	if ok {
		delete(b.sessions, clientID)
	}
	b.mu.Unlock()
	if !ok {
		return
	}

	b.logger.Info("another broker claimed this session", "client_id", clientID)
	s.takeOver()
	s.discard()
}
