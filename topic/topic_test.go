package topic

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The match cases are the worked examples in MQTT-5.0 §4.7.1.2, §4.7.1.3 and
// §4.7.2, so this asserts the spec's own answers rather than ours.
func TestMatchUsesSpecExamples(t *testing.T) {
	matches := []struct{ filter, name string }{
		// §4.7.1.2: "sport/tennis/player1/#" receives these.
		{"sport/tennis/player1/#", "sport/tennis/player1"},
		{"sport/tennis/player1/#", "sport/tennis/player1/ranking"},
		{"sport/tennis/player1/#", "sport/tennis/player1/score/wimbledon"},
		// "'sport/#' also matches the singular 'sport', since # includes the
		// parent level."
		{"sport/#", "sport"},
		// "'#' is valid and will receive every Application Message"
		{"#", "sport/tennis"},
		{"#", "a"},
		// §4.7.1.3
		{"sport/tennis/+", "sport/tennis/player1"},
		{"sport/tennis/+", "sport/tennis/player2"},
		{"sport/+", "sport/"},
		{"+/tennis/#", "a/tennis/b"},
		{"sport/+/player1", "sport/x/player1"},
		{"+/+", "/finance"},
		{"/+", "/finance"},
		// §4.7.2: an explicit $ prefix in the filter does match.
		{"$SYS/#", "$SYS/monitor/Clients"},
		{"$SYS/monitor/+", "$SYS/monitor/Clients"},
		// §4.8.2: the $share and ShareName parts play no part in matching.
		{"$share/consumer1/sport/tennis/+", "sport/tennis/player1"},
		{"$share/consumer1//finance", "/finance"},
	}
	for _, tc := range matches {
		assert.True(t, Match(tc.filter, tc.name), "%q should match %q", tc.filter, tc.name)
	}

	nonMatches := []struct{ filter, name string }{
		{"sport/tennis/+", "sport/tennis/player1/ranking"},
		{"sport/+", "sport"},
		{"+", "/finance"},
		// §4.7.2: "A subscription to '#' will not receive any messages
		// published to a topic beginning with a $" [MQTT-4.7.2-1].
		{"#", "$SYS/monitor/Clients"},
		{"+/monitor/Clients", "$SYS/monitor/Clients"},
		// §4.7.3: topics are case sensitive, and a leading '/' is significant.
		{"ACCOUNTS", "Accounts"},
		{"finance", "/finance"},
	}
	for _, tc := range nonMatches {
		assert.False(t, Match(tc.filter, tc.name), "%q should not match %q", tc.filter, tc.name)
	}
}

func TestValidateFilter(t *testing.T) {
	// §4.7.1.2 and §4.7.1.3 list these as valid.
	valid := []string{"#", "sport/tennis/#", "+", "+/tennis/#", "sport/+/player1", "/finance", "/", "sport/+"}
	for _, f := range valid {
		assert.NoError(t, ValidateFilter(f), "%q should be a valid Topic Filter", f)
	}

	// §4.7.1.2 and §4.7.1.3 list these as invalid.
	invalid := map[string]string{
		"# not at the end":       "sport/tennis/#/ranking",
		"# not alone in a level": "sport/tennis#",
		"+ not alone in a level": "sport+",
		"empty":                  "",
	}
	for name, f := range invalid {
		t.Run(name, func(t *testing.T) {
			assert.ErrorIs(t, ValidateFilter(f), ErrInvalid)
		})
	}
}

// A Shared Subscription Topic Filter is "$share/{ShareName}/{filter}", the
// ShareName must be at least one character and must not contain '/', '+' or
// '#' [MQTT-4.8.2-1, MQTT-4.8.2-2].
func TestSplitShared(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		cases := map[string]struct{ share, filter string }{
			"$share/consumer1/sport/tennis/+": {"consumer1", "sport/tennis/+"},
			"$share/consumer1//finance":       {"consumer1", "/finance"},
			"$share/g/#":                      {"g", "#"},
		}
		for in, want := range cases {
			share, filter, err := SplitShared(in)
			require.NoError(t, err, in)
			assert.Equal(t, want.share, share, in)
			assert.Equal(t, want.filter, filter, in)
			assert.True(t, IsShared(in))
		}
	})

	t.Run("not shared passes through unchanged", func(t *testing.T) {
		share, filter, err := SplitShared("sport/#")
		require.NoError(t, err)
		assert.Empty(t, share)
		assert.Equal(t, "sport/#", filter)
		assert.False(t, IsShared("sport/#"))
	})

	t.Run("malformed", func(t *testing.T) {
		for _, f := range []string{"$share/", "$share//finance", "$share/name", "$share/na+me/a", "$share/name/"} {
			_, _, err := SplitShared(f)
			assert.ErrorIs(t, err, ErrInvalid, "%q should be rejected", f)
		}
	})
}

// A shared filter's subject drops the $share/{ShareName} prefix: the
// ShareName selects the NATS queue group, not the subject (MQTT-5.0 §4.8.2).
func TestSharedFilterSubjectDropsShareName(t *testing.T) {
	subject, err := FilterToSubject("$share/workers/jobs/+/run")
	require.NoError(t, err)
	assert.Equal(t, "jobs.*.run", subject)

	plain, err := FilterToSubject("jobs/+/run")
	require.NoError(t, err)
	assert.Equal(t, plain, subject,
		"a shared subscription must map to the same subject as its unshared equivalent")
}
