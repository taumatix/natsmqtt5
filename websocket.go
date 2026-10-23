package natsmqtt5

import (
	"crypto/tls"
	"net"
	"net/http"

	"github.com/coder/websocket"
)

// webSocketSubprotocol is the subprotocol name MQTT 5 requires of a WebSocket
// client (MQTT-5.0 §6, [MQTT-6.0.0-4]).
const webSocketSubprotocol = "mqtt"

// WebSocketHandler returns an http.Handler that serves MQTT over WebSocket
// (MQTT-5.0 §6). Mount it on any path of an http.Server (plain or TLS) and
// browsers and hosted clients can connect to the same broker, sessions and
// subscriptions as the TCP listener. Each upgraded connection is handed to the
// same state machine as a TCP one; nothing else differs.
//
// The handler negotiates the "mqtt" subprotocol and refuses a client that
// offers none [MQTT-6.0.0-4]; every MQTT packet travels in a binary frame
// [MQTT-6.0.0-1]. A request whose Origin header names another host is refused
// unless Options.WebSocketOriginPatterns allows it.
//
// The handler does not close the connections it serves when the http.Server
// shuts down (a hijacked connection is outside the server's reach); Close does,
// with the usual DISCONNECT 0x8B. A request made after Close is answered 503.
func (b *Broker) WebSocketHandler() http.Handler {
	return http.HandlerFunc(b.serveWebSocket)
}

func (b *Broker) serveWebSocket(w http.ResponseWriter, r *http.Request) {
	if b.isClosed() {
		http.Error(w, "broker closed", http.StatusServiceUnavailable)
		return
	}
	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		Subprotocols:   []string{webSocketSubprotocol},
		OriginPatterns: b.opts.WebSocketOriginPatterns,
	})
	if err != nil {
		// Accept has already answered the request.
		b.logger.Debug("refused a WebSocket upgrade", "remote", r.RemoteAddr, "error", err)
		return
	}
	if ws.Subprotocol() != webSocketSubprotocol {
		// [MQTT-6.0.0-4]: a client that does not offer "mqtt" is not speaking it.
		_ = ws.Close(websocket.StatusPolicyViolation, "the mqtt subprotocol is required")
		return
	}
	// Frames are bounded by the largest packet the broker accepts, which the
	// packet reader already enforces; the library's own default would cut a
	// large PUBLISH short.
	ws.SetReadLimit(-1)

	nc := &webSocketConn{
		Conn:   websocket.NetConn(r.Context(), ws, websocket.MessageBinary),
		remote: remoteAddr(r.RemoteAddr),
		tls:    r.TLS,
	}

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		_ = ws.Close(websocket.StatusGoingAway, "broker closed")
		return
	}
	c := newConn(b, nc)
	b.conns[c] = struct{}{}
	b.wg.Add(1)
	b.mu.Unlock()

	defer b.wg.Done()
	defer b.trackConn(c, false)
	c.serve(r.Context())
}

// webSocketConn is a WebSocket connection seen as a net.Conn, with the peer's
// real address (the library reports a placeholder) and the TLS state of the
// HTTP request that was upgraded.
type webSocketConn struct {
	net.Conn
	remote net.Addr
	tls    *tls.ConnectionState
}

func (c *webSocketConn) RemoteAddr() net.Addr { return c.remote }

// tlsConnectionState is read by conn.tlsState.
func (c *webSocketConn) tlsConnectionState() *tls.ConnectionState { return c.tls }

// remoteAddr is a net.Addr for the "host:port" an http.Request carries.
type remoteAddr string

func (remoteAddr) Network() string  { return "tcp" }
func (a remoteAddr) String() string { return string(a) }
