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
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/taumatix/natsmqtt5"
)

// version is stamped at build time with -ldflags "-X main.version=v1.2.3". It
// is "dev" for a plain `go build` and for `go install` of a pseudo-version.
var version = "dev"

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
	cfg, err := parseConfig(args, getenv, stderr)
	if err != nil {
		return err
	}
	if cfg.showVersion {
		fmt.Fprintln(stdout, "natsmqtt5", version)
		return nil
	}

	cfg.broker.Logger = slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: cfg.level}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	startCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	broker, err := natsmqtt5.NewWithContext(startCtx, cfg.broker)
	cancel()
	if err != nil {
		return err
	}
	defer broker.Close()

	cfg.broker.Logger.Info("starting", "version", version, "listen", cfg.broker.Listen, "nats", cfg.broker.NATSURL)
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
}

// parseConfig resolves the command line against the environment. An explicit
// flag beats the environment, which beats the default.
func parseConfig(args []string, getenv func(string) string, stderr io.Writer) (*config, error) {
	fs := flag.NewFlagSet("natsmqtt5", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var (
		natsURL       = fs.String("nats", nats.DefaultURL, "URL of the NATS server to use")
		listen        = fs.String("listen", natsmqtt5.DefaultListen, "address to accept MQTT connections on")
		subjectPrefix = fs.String("subject-prefix", natsmqtt5.DefaultSubjectPrefix, "NATS subject prefix for MQTT traffic")
		streamPrefix  = fs.String("stream-prefix", natsmqtt5.DefaultStreamPrefix, "name prefix for the JetStream assets")
		creds         = fs.String("creds", "", "path to a NATS credentials file")
		natsUser      = fs.String("nats-user", "", "NATS username")
		natsPass      = fs.String("nats-password", "", "NATS password")
		noRetained    = fs.Bool("no-retained", false, "disable retained messages, so JetStream is not required")
		maxQoS        = fs.Int("max-qos", 2, "highest QoS the broker accepts (0, 1 or 2)")
		keepAlive     = fs.Int("server-keep-alive", 0, "override the client's Keep Alive, in seconds; 0 leaves it to the client")
		logLevel      = fs.String("log-level", "info", "debug, info, warn or error")
		showVersion   = fs.Bool("version", false, "print the version and exit")
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

	var natsOpts []nats.Option
	if *creds != "" {
		natsOpts = append(natsOpts, nats.UserCredentials(*creds))
	}
	if *natsUser != "" {
		natsOpts = append(natsOpts, nats.UserInfo(*natsUser, *natsPass))
	}

	return &config{
		broker: natsmqtt5.Options{
			NATSURL:         *natsURL,
			NATSOptions:     natsOpts,
			Listen:          *listen,
			SubjectPrefix:   *subjectPrefix,
			StreamPrefix:    *streamPrefix,
			DisableRetained: *noRetained,
			MaximumQoS:      natsmqtt5.Ptr(uint8(*maxQoS)),
			ServerKeepAlive: uint16(*keepAlive),
		},
		level:       level,
		showVersion: *showVersion,
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
