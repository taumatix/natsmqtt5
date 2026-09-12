package main

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
)

// env builds a getenv function over a map, so a test never touches the real
// environment and cases can run in parallel.
func env(pairs map[string]string) func(string) string {
	return func(name string) string { return pairs[name] }
}

func TestParseConfigDefaults(t *testing.T) {
	cfg, err := parseConfig(nil, env(nil), io.Discard)
	require.NoError(t, err)

	assert.Equal(t, "nats://127.0.0.1:4222", cfg.broker.NATSURL)
	assert.Equal(t, natsmqtt5.DefaultListen, cfg.broker.Listen)
	assert.Equal(t, natsmqtt5.DefaultSubjectPrefix, cfg.broker.SubjectPrefix)
	assert.False(t, cfg.broker.DisableRetained)
	require.NotNil(t, cfg.broker.MaximumQoS)
	assert.Equal(t, uint8(2), *cfg.broker.MaximumQoS)
}

// The container image is configured through the environment, so every flag has
// to be reachable that way — including the bool and int ones.
func TestParseConfigFromEnvironment(t *testing.T) {
	cfg, err := parseConfig(nil, env(map[string]string{
		"NATSMQTT5_NATS":              "nats://nats:4222",
		"NATSMQTT5_LISTEN":            "0.0.0.0:8883",
		"NATSMQTT5_SUBJECT_PREFIX":    "mqtt5",
		"NATSMQTT5_STREAM_PREFIX":     "MQTT5",
		"NATSMQTT5_NO_RETAINED":       "true",
		"NATSMQTT5_MAX_QOS":           "1",
		"NATSMQTT5_SERVER_KEEP_ALIVE": "60",
		"NATSMQTT5_LOG_LEVEL":         "debug",
	}), io.Discard)
	require.NoError(t, err)

	assert.Equal(t, "nats://nats:4222", cfg.broker.NATSURL)
	assert.Equal(t, "0.0.0.0:8883", cfg.broker.Listen)
	assert.Equal(t, "mqtt5", cfg.broker.SubjectPrefix)
	assert.Equal(t, "MQTT5", cfg.broker.StreamPrefix)
	assert.True(t, cfg.broker.DisableRetained)
	require.NotNil(t, cfg.broker.MaximumQoS)
	assert.Equal(t, uint8(1), *cfg.broker.MaximumQoS)
	assert.Equal(t, uint16(60), cfg.broker.ServerKeepAlive)
	assert.Equal(t, "DEBUG", cfg.level.String())
}

// Overriding one setting on the command line of an otherwise env-configured
// container must work, so the flag has to win.
func TestFlagBeatsEnvironment(t *testing.T) {
	cfg, err := parseConfig(
		[]string{"-listen", ":1884"},
		env(map[string]string{"NATSMQTT5_LISTEN": ":9999", "NATSMQTT5_NATS": "nats://nats:4222"}),
		io.Discard,
	)
	require.NoError(t, err)

	assert.Equal(t, ":1884", cfg.broker.Listen)
	assert.Equal(t, "nats://nats:4222", cfg.broker.NATSURL, "an unrelated variable still applies")
}

// A variable an orchestrator interpolated from a missing value arrives empty;
// that must not blank out the default and leave the broker with no NATS URL.
func TestEmptyEnvironmentVariableIsIgnored(t *testing.T) {
	cfg, err := parseConfig(nil, env(map[string]string{"NATSMQTT5_NATS": ""}), io.Discard)
	require.NoError(t, err)

	assert.Equal(t, "nats://127.0.0.1:4222", cfg.broker.NATSURL)
}

func TestParseConfigRejectsBadValues(t *testing.T) {
	cases := map[string]struct {
		args []string
		env  map[string]string
	}{
		"max QoS above 2":        {args: []string{"-max-qos", "3"}},
		"negative keep alive":    {args: []string{"-server-keep-alive", "-1"}},
		"keep alive above 65535": {args: []string{"-server-keep-alive", "70000"}},
		"unknown log level":      {args: []string{"-log-level", "chatty"}},
		"stray argument":         {args: []string{"serve"}},
		"unparseable env int":    {env: map[string]string{"NATSMQTT5_MAX_QOS": "loud"}},
		"unparseable env bool":   {env: map[string]string{"NATSMQTT5_NO_RETAINED": "perhaps"}},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := parseConfig(tc.args, env(tc.env), io.Discard)
			assert.Error(t, err)
		})
	}
}

func TestEnvName(t *testing.T) {
	assert.Equal(t, "NATSMQTT5_NATS", envName("nats"))
	assert.Equal(t, "NATSMQTT5_NATS_PASSWORD", envName("nats-password"))
	assert.Equal(t, "NATSMQTT5_SERVER_KEEP_ALIVE", envName("server-keep-alive"))
}

func TestVersionFlag(t *testing.T) {
	cfg, err := parseConfig([]string{"-version"}, env(nil), io.Discard)
	require.NoError(t, err)
	assert.True(t, cfg.showVersion)
}

// The container image stamps the version with ldflags. Every other build has
// to get it from what Go recorded, or `go install ...@v0.1.1` would report
// "dev" for a binary that is demonstrably a release.
func TestBinaryVersion(t *testing.T) {
	t.Run("a stamped build reports the stamp", func(t *testing.T) {
		stamped := version
		t.Cleanup(func() { version = stamped })

		version = "v9.9.9"
		assert.Equal(t, "v9.9.9", binaryVersion())
	})

	t.Run("an unstamped build reports what Go recorded", func(t *testing.T) {
		stamped := version
		t.Cleanup(func() { version = stamped })

		version = ""
		// Under `go test` the recorded main module version is the one for this
		// module, so the only safe claim is that it says something, and never
		// the empty string a bare fmt.Println would produce.
		assert.NotEmpty(t, binaryVersion())
	})
}
