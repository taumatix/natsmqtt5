package main

import (
	"crypto/tls"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/taumatix/natsmqtt5"
)

// serveWebSocket serves the broker's WebSocket handler at cfg.wsPath on cfg.wsListen, over TLS
// when cfg carries a certificate. The address is bound before it returns, so a port already in
// use fails start-up. The returned function stops the HTTP server; the connections it carries
// are the broker's, closed by broker.Close with a DISCONNECT 0x8B.
func serveWebSocket(broker *natsmqtt5.Broker, cfg *config, logger *slog.Logger) (stop func(), err error) {
	ln, err := net.Listen("tcp", cfg.wsListen)
	if err != nil {
		return nil, err
	}
	if cfg.tls != nil {
		ln = tls.NewListener(ln, cfg.tls)
	}
	mux := http.NewServeMux()
	mux.Handle(cfg.wsPath, broker.WebSocketHandler())
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("the WebSocket listener stopped", "error", err)
		}
	}()
	logger.Info("serving WebSocket", "listen", ln.Addr().String(), "path", cfg.wsPath, "tls", cfg.tls != nil)
	return func() { _ = srv.Close() }, nil
}
