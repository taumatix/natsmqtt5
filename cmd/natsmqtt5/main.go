// Command natsmqtt5 runs an MQTT v5 broker against an existing NATS server.
//
//	natsmqtt5 -nats nats://localhost:4222 -listen :1883
//
// It is a thin wrapper over the natsmqtt5 package; anything it can do, an
// application embedding the package can do too.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/taumatix/natsmqtt5"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "natsmqtt5:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		natsURL       = flag.String("nats", nats.DefaultURL, "URL of the NATS server to use")
		listen        = flag.String("listen", natsmqtt5.DefaultListen, "address to accept MQTT connections on")
		subjectPrefix = flag.String("subject-prefix", natsmqtt5.DefaultSubjectPrefix, "NATS subject prefix for MQTT traffic")
		streamPrefix  = flag.String("stream-prefix", natsmqtt5.DefaultStreamPrefix, "name prefix for the JetStream assets")
		creds         = flag.String("creds", "", "path to a NATS credentials file")
		natsUser      = flag.String("nats-user", "", "NATS username")
		natsPass      = flag.String("nats-password", "", "NATS password")
		noRetained    = flag.Bool("no-retained", false, "disable retained messages, so JetStream is not required")
		maxQoS        = flag.Int("max-qos", 2, "highest QoS the broker accepts (0, 1 or 2)")
		keepAlive     = flag.Int("server-keep-alive", 0, "override the client's Keep Alive, in seconds; 0 leaves it to the client")
		logLevel      = flag.String("log-level", "info", "debug, info, warn or error")
	)
	flag.Parse()

	level, err := parseLevel(*logLevel)
	if err != nil {
		return err
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	if *maxQoS < 0 || *maxQoS > 2 {
		return fmt.Errorf("-max-qos must be 0, 1 or 2, got %d", *maxQoS)
	}
	if *keepAlive < 0 || *keepAlive > 65535 {
		return fmt.Errorf("-server-keep-alive must be between 0 and 65535, got %d", *keepAlive)
	}

	var natsOpts []nats.Option
	if *creds != "" {
		natsOpts = append(natsOpts, nats.UserCredentials(*creds))
	}
	if *natsUser != "" {
		natsOpts = append(natsOpts, nats.UserInfo(*natsUser, *natsPass))
	}

	opts := natsmqtt5.Options{
		NATSURL:         *natsURL,
		NATSOptions:     natsOpts,
		Listen:          *listen,
		SubjectPrefix:   *subjectPrefix,
		StreamPrefix:    *streamPrefix,
		DisableRetained: *noRetained,
		MaximumQoS:      natsmqtt5.Ptr(uint8(*maxQoS)),
		ServerKeepAlive: uint16(*keepAlive),
		Logger:          logger,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	startCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	broker, err := natsmqtt5.NewWithContext(startCtx, opts)
	cancel()
	if err != nil {
		return err
	}
	defer broker.Close()

	if err := broker.Serve(ctx); err != nil {
		return err
	}
	logger.Info("shut down cleanly")
	return nil
}

func parseLevel(s string) (slog.Level, error) {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	}
	return 0, fmt.Errorf("unknown -log-level %q", s)
}
