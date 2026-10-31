// Command natsmqtt5 runs an MQTT v5 broker against an existing NATS server.
//
//	natsmqtt5 -nats nats://localhost:4222 -listen :1883
//
// Every flag has an environment variable twin, named by upper-casing it and
// replacing '-' with '_' behind a NATSMQTT5_ prefix, so the container image is
// configurable without a command line:
//
//	NATSMQTT5_NATS=nats://nats:4222 natsmqtt5
//
// It is a thin wrapper over the natsmqtt5 package; anything it can do, an
// application embedding the package can do too.
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/taumatix/natsmqtt5"
)

// version is stamped at build time with -ldflags "-X main.version=v1.2.3",
// which is how the container image knows which release it is. Left empty for
// every other build, where binaryVersion falls back to what Go recorded.
var version = ""

// binaryVersion is what -version reports. A `go install ...@v0.2.0` build
// carries no ldflags but does record the module version, so reporting "dev"
// for it would be a lie the user cannot check.
func binaryVersion() string {
	if version != "" {
		return version
	}
	if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "" {
		return info.Main.Version
	}
	return "dev"
}

// envPrefix is prepended to every flag's environment variable twin.
const envPrefix = "NATSMQTT5_"

func main() {
	err := run(os.Args[1:], os.Getenv, os.Stdout, os.Stderr)
	switch {
	case err == nil:
		return
	case errors.Is(err, flag.ErrHelp):
		os.Exit(0)
	default:
		fmt.Fprintln(os.Stderr, "natsmqtt5:", err)
		os.Exit(1)
	}
}

func run(args []string, getenv func(string) string, stdout, stderr io.Writer) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return runContext(ctx, args, getenv, stdout, stderr)
}

// runContext is run until ctx is cancelled, which a test does in place of a signal.
func runContext(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) error {
	cfg, err := parseConfig(args, getenv, stderr)
	if err != nil {
		return err
	}
	if cfg.showVersion {
		fmt.Fprintln(stdout, "natsmqtt5", binaryVersion())
		return nil
	}

	cfg.broker.Logger = slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: cfg.level}))

	startCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	broker, err := natsmqtt5.NewWithContext(startCtx, cfg.broker)
	cancel()
	if err != nil {
		return err
	}
	defer func() {
		if err := broker.Close(); err != nil {
			cfg.broker.Logger.Warn("closing the broker", "error", err)
		}
	}()

	cfg.broker.Logger.Info("starting", "version", binaryVersion(), "listen", cfg.broker.Listen, "nats", cfg.broker.NATSURL)
	if cfg.wsListen != "" {
		stopWS, err := serveWebSocket(broker, cfg, cfg.broker.Logger)
		if err != nil {
			return err
		}
		defer stopWS()
	}
	if err := broker.Serve(ctx); err != nil {
		return err
	}
	cfg.broker.Logger.Info("shut down cleanly")
	return nil
}

// config is the command's settings, resolved from the flags and the
// environment. The Logger is left for run to fill in from level.
type config struct {
	broker      natsmqtt5.Options
	level       slog.Level
	showVersion bool
	// wsListen is the address to serve MQTT over WebSocket on, "" for none; wsPath is the URL
	// path it is mounted at; tls is shared by the TCP listener and the WebSocket one.
	wsListen string
	wsPath   string
	tls      *tls.Config
}

// parseConfig resolves the command line against the environment. An explicit
// flag beats the environment, which beats the default.
func parseConfig(args []string, getenv func(string) string, stderr io.Writer) (*config, error) {
	fs := flag.NewFlagSet("natsmqtt5", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var (
		natsURL         = fs.String("nats", nats.DefaultURL, "URL of the NATS server to use")
		listen          = fs.String("listen", natsmqtt5.DefaultListen, "address to accept MQTT connections on")
		subjectPrefix   = fs.String("subject-prefix", natsmqtt5.DefaultSubjectPrefix, "NATS subject prefix for MQTT traffic")
		streamPrefix    = fs.String("stream-prefix", natsmqtt5.DefaultStreamPrefix, "name prefix for the JetStream assets")
		creds           = fs.String("creds", "", "path to a NATS credentials file")
		natsUser        = fs.String("nats-user", "", "NATS username")
		natsPass        = fs.String("nats-password", "", "NATS password")
		noRetained      = fs.Bool("no-retained", false, "disable retained messages, so JetStream is not required")
		persistSess     = fs.Bool("persistent-sessions", false, "keep session state in JetStream, so it survives a restart and moves between brokers")
		durableWills    = fs.Bool("durable-wills", false, "keep Will Messages in JetStream so the Will of a killed broker is published by a surviving one")
		willCheck       = fs.Duration("will-check-interval", 0, "how often to look for Wills whose broker has died, with -durable-wills; 0 means 5s")
		checkpoint      = fs.Duration("session-checkpoint-interval", 0, "with -persistent-sessions, how often a connected client's replay position is written to its session record, so a broker that is killed costs a client that long of repeats and not the whole queue; 0 means 1s, negative turns it off")
		offlineQueue    = fs.Bool("offline-queue", false, "require the offline queue, failing to start without it (it is on by default whenever JetStream is used)")
		noOffline       = fs.Bool("no-offline-queue", false, "do not keep QoS 1 and 2 messages for disconnected sessions; a resumed session misses what was published while it was away")
		offlineMaxAge   = fs.Duration("offline-queue-max-age", 24*time.Hour, "how long a queued message is kept for a disconnected session")
		durableDedupe   = fs.Bool("durable-publish-dedupe", false, "with -durable-publish, let the queue stream drop the copy a client's retry of a refused publish would add (inside its duplicate window)")
		durablePub      = fs.Bool("durable-publish", false, "acknowledge QoS 1 and 2 publishes only once the offline queue has stored them and NATS has confirmed the live copy (implies -offline-queue)")
		escapeWildcards = fs.Bool("escape-nats-wildcards", false, "accept topics containing *, >, spaces and other whitespace by escaping them in the NATS subject (not readable by nats-server's own MQTT listener; choose before you have data)")
		restrictDollar  = fs.Bool("restrict-dollar-topics", false, "refuse publishes and subscriptions on topics starting with $ (MQTT 5.0 section 4.7.2), except $share/ subscriptions")
		maxQoS          = fs.Int("max-qos", 2, "highest QoS the broker accepts (0, 1 or 2)")
		sharedQoS2      = fs.Bool("shared-qos2", false, "grant QoS 2 on shared subscriptions (default grants QoS 1; the guarantee is incomplete, see ROADMAP.md)")
		keepAlive       = fs.Int("server-keep-alive", 0, "override the client's Keep Alive, in seconds; 0 leaves it to the client")
		wsListen        = fs.String("ws-listen", "", "address to accept MQTT over WebSocket on (for example :8083); empty serves none")
		wsPath          = fs.String("ws-path", "/mqtt", "URL path the WebSocket handler is mounted at")
		tlsCert         = fs.String("tls-cert", "", "path to a PEM certificate chain; with -tls-key, the MQTT listener and the -ws-listen one serve TLS (mqtts:// and wss://)")
		tlsKey          = fs.String("tls-key", "", "path to the PEM private key for -tls-cert")
		logLevel        = fs.String("log-level", "info", "debug, info, warn or error")
		showVersion     = fs.Bool("version", false, "print the version and exit")
	)

	fs.Usage = func() {
		fmt.Fprintf(stderr, "Usage of natsmqtt5:\n")
		fs.PrintDefaults()
		fmt.Fprintf(stderr, "\nEvery flag can also be set from the environment: -subject-prefix is %sSUBJECT_PREFIX.\n", envPrefix)
	}

	if err := applyEnv(fs, getenv); err != nil {
		return nil, err
	}
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	if fs.NArg() > 0 {
		return nil, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}

	level, err := parseLevel(*logLevel)
	if err != nil {
		return nil, err
	}
	if *maxQoS < 0 || *maxQoS > 2 {
		return nil, fmt.Errorf("-max-qos must be 0, 1 or 2, got %d", *maxQoS)
	}
	if *keepAlive < 0 || *keepAlive > 65535 {
		return nil, fmt.Errorf("-server-keep-alive must be between 0 and 65535, got %d", *keepAlive)
	}

	if !strings.HasPrefix(*wsPath, "/") {
		return nil, fmt.Errorf("-ws-path must start with /, got %q", *wsPath)
	}
	if (*tlsCert == "") != (*tlsKey == "") {
		return nil, errors.New("-tls-cert and -tls-key go together")
	}
	var tlsConfig *tls.Config
	if *tlsCert != "" {
		pair, err := tls.LoadX509KeyPair(*tlsCert, *tlsKey)
		if err != nil {
			return nil, fmt.Errorf("-tls-cert/-tls-key: %w", err)
		}
		tlsConfig = &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}
	}

	var natsOpts []nats.Option
	if *creds != "" {
		natsOpts = append(natsOpts, nats.UserCredentials(*creds))
	}
	if *natsUser != "" {
		natsOpts = append(natsOpts, nats.UserInfo(*natsUser, *natsPass))
	}

	return &config{
		broker: natsmqtt5.Options{
			NATSURL:                   *natsURL,
			NATSOptions:               natsOpts,
			Listen:                    *listen,
			TLSConfig:                 tlsConfig,
			SubjectPrefix:             *subjectPrefix,
			StreamPrefix:              *streamPrefix,
			DisableRetained:           *noRetained,
			PersistentSessions:        *persistSess,
			DurableWills:              *durableWills,
			RestrictDollarTopics:      *restrictDollar,
			EscapeNATSWildcards:       *escapeWildcards,
			WillCheckInterval:         *willCheck,
			SessionCheckpointInterval: *checkpoint,
			DurablePublish:            *durablePub,
			DurablePublishDedupe:      *durableDedupe,
			OfflineQueue:              *offlineQueue,
			DisableOfflineQueue:       *noOffline,
			OfflineQueueMaxAge:        *offlineMaxAge,
			MaximumQoS:                natsmqtt5.Ptr(uint8(*maxQoS)),
			SharedQoS2:                *sharedQoS2,
			ServerKeepAlive:           uint16(*keepAlive),
		},
		level:       level,
		showVersion: *showVersion,
		wsListen:    *wsListen,
		wsPath:      *wsPath,
		tls:         tlsConfig,
	}, nil
}

// applyEnv seeds each flag from its environment twin before parsing, which
// leaves an explicitly given flag winning. An empty variable counts as unset,
// so an orchestrator that interpolates a missing value gets the default rather
// than an empty NATS URL.
func applyEnv(fs *flag.FlagSet, getenv func(string) string) error {
	var err error
	fs.VisitAll(func(f *flag.Flag) {
		name := envName(f.Name)
		value := getenv(name)
		if err != nil || value == "" {
			return
		}
		if setErr := f.Value.Set(value); setErr != nil {
			err = fmt.Errorf("%s=%q: %w", name, value, setErr)
		}
	})
	return err
}

// envName is the environment variable a flag can be set from: -nats-password
// becomes NATSMQTT5_NATS_PASSWORD.
func envName(flagName string) string {
	return envPrefix + strings.ToUpper(strings.ReplaceAll(flagName, "-", "_"))
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
