package topic

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNameToSubjectMatchesNATSServer is a conformance check against the table
// in nats-server's own TestMQTTTopicAndSubjectConversion
// (server/mqtt_test.go, v2.14.6). Every pair below is copied from there. If
// this test fails, this broker and nats-server no longer agree on where an
// MQTT topic lands in the NATS subject space.
func TestNameToSubjectMatchesNATSServer(t *testing.T) {
	cases := []struct{ topic, subject string }{
		{"/", "/./"},
		{"//", "/././"},
		{"///", "/./././"},
		{"////", "/././././"},
		{"foo", "foo"},
		{"/foo", "/.foo"},
		{"//foo", "/./.foo"},
		{"///foo", "/././.foo"},
		{"///foo/", "/././.foo./"},
		{"///foo//", "/././.foo././"},
		{"///foo///", "/././.foo./././"},
		{"//.foo.//", "/././/foo//././"},
		{"foo/bar", "foo.bar"},
		{"/foo/bar", "/.foo.bar"},
		{"/foo/bar/", "/.foo.bar./"},
		{"foo/bar/baz", "foo.bar.baz"},
		{"/foo/bar/baz", "/.foo.bar.baz"},
		{"/foo/bar/baz/", "/.foo.bar.baz./"},
		{"bar/", "bar./"},
		{"bar//", "bar././"},
		{"bar///", "bar./././"},
		{"foo//bar", "foo./.bar"},
		{"foo///bar", "foo././.bar"},
		{"foo////bar", "foo./././.bar"},
		{".", "//"},
		{"..", "////"},
		{"...", "//////"},
		{"./", "//./"},
		{".//.", "//././/"},
		{"././.", "//.//.//"},
		{"././/.", "//.//././/"},
		{".foo", "//foo"},
		{"foo.", "foo//"},
		{".foo.", "//foo//"},
		{"foo../bar/", "foo////.bar./"},
		{"foo../bar/.", "foo////.bar.//"},
		{"/foo/", "/.foo./"},
		{"./foo/.", "//.foo.//"},
		{"foo.bar/baz", "foo//bar.baz"},
	}
	for _, tc := range cases {
		t.Run(tc.topic, func(t *testing.T) {
			got, err := NameToSubject(tc.topic)
			require.NoError(t, err)
			assert.Equal(t, tc.subject, got, "topic %q to NATS subject", tc.topic)

			assert.Equal(t, tc.topic, SubjectToName(got),
				"subject %q must convert back to the original topic", got)
		})
	}
}

// TestFilterToSubjectMatchesNATSServer copies the wildcard rows of
// nats-server's TestMQTTFilterConversion (server/mqtt_test.go, v2.14.6).
func TestFilterToSubjectMatchesNATSServer(t *testing.T) {
	cases := []struct{ filter, subject string }{
		{"+", "*"},
		{"/+", "/.*"},
		{"+/", "*./"},
		{"/+/", "/.*./"},
		{"foo/+", "foo.*"},
		{"foo/+/", "foo.*./"},
		{"foo/+/bar", "foo.*.bar"},
		{"foo/+/+", "foo.*.*"},
		{"foo/+/+/", "foo.*.*./"},
		{"foo/+/+/bar", "foo.*.*.bar"},
		{"foo//+", "foo./.*"},
		{"foo//+/", "foo./.*./"},
		{"foo//+//", "foo./.*././"},
		{"foo//+//bar", "foo./.*./.bar"},
		{"foo///+///bar", "foo././.*././.bar"},
		{"foo.bar///+///baz", "foo//bar././.*././.baz"},

		{"#", ">"},
		{"/#", "/.>"},
		{"/foo/#", "/.foo.>"},
		{"foo/#", "foo.>"},
		{"foo//#", "foo./.>"},
		{"foo///#", "foo././.>"},
		{"foo/bar/#", "foo.bar.>"},
		{"foo/bar.baz/#", "foo.bar//baz.>"},
	}
	for _, tc := range cases {
		t.Run(tc.filter, func(t *testing.T) {
			got, err := FilterToSubject(tc.filter)
			require.NoError(t, err)
			assert.Equal(t, tc.subject, got)
		})
	}
}

// nats-server passes a literal '*' or '>' in an MQTT topic straight through
// into the NATS subject, where it becomes a wildcard: its own test table
// records "foo/*/bar" becoming "foo.*.bar". In MQTT those are ordinary
// characters, so that subscription would receive topics it must not see. We
// reject them rather than mis-deliver — the one deliberate divergence from
// nats-server's mapping.
func TestNATSWildcardCharactersAreRejected(t *testing.T) {
	for _, s := range []string{"foo*/bar", "foo>/bar", "foo/*/bar", "foo/>", "*", ">"} {
		t.Run(s, func(t *testing.T) {
			_, nameErr := NameToSubject(s)
			assert.ErrorIs(t, nameErr, ErrInvalid, "as a Topic Name")

			_, filterErr := FilterToSubject(s)
			assert.ErrorIs(t, filterErr, ErrInvalid, "as a Topic Filter")
		})
	}
}

func TestNameToSubjectRejectsUnsupportedTopics(t *testing.T) {
	cases := map[string]string{
		"wildcard + in a Topic Name": "foo/+",
		"wildcard # in a Topic Name": "foo/#",
		"space":                      "foo bar",
		"DEL":                        "foo\x7fbar",
		"empty":                      "",
		"null character":             "foo\x00bar",
	}
	for name, topic := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := NameToSubject(topic)
			assert.ErrorIs(t, err, ErrInvalid)
		})
	}
}

// A '#' matches the parent level too, so "sport/#" must also match the topic
// "sport" (MQTT-5.0 §4.7.1.2). NATS's '>' needs at least one token, so the
// broker takes out a second subscription on the parent subject.
func TestParentSubject(t *testing.T) {
	cases := map[string]struct {
		parent string
		needed bool
	}{
		"foo.>":     {"foo", true},
		"foo.bar.>": {"foo.bar", true},
		"/.>":       {"/", true},
		">":         {"", false},
		"foo.bar":   {"", false},
		"foo.*":     {"", false},
	}
	for subject, want := range cases {
		t.Run(subject, func(t *testing.T) {
			parent, ok := ParentSubject(subject)
			assert.Equal(t, want.needed, ok)
			assert.Equal(t, want.parent, parent)
		})
	}
}

func TestPrefixRoundTrip(t *testing.T) {
	subject, err := NameToSubject("sensors/1/temp")
	require.NoError(t, err)

	prefixed := Prefix("mqtt", subject)
	assert.Equal(t, "mqtt.sensors.1.temp", prefixed)

	trimmed, ok := TrimPrefix("mqtt", prefixed)
	require.True(t, ok)
	assert.Equal(t, subject, trimmed)

	_, ok = TrimPrefix("mqtt", "other.subject")
	assert.False(t, ok)

	assert.Equal(t, subject, Prefix("", subject), "an empty prefix is a no-op")
}

// FuzzSubjectRoundTrip asserts the mapping is lossless for every topic it
// accepts. A topic that survives NameToSubject but comes back different would
// silently deliver messages under the wrong name.
func FuzzSubjectRoundTrip(f *testing.F) {
	for _, s := range []string{"foo", "/", "//", "a.b", "foo/bar", "foo//bar", ".", "./foo/.", "a/b/c/"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, name string) {
		subject, err := NameToSubject(name)
		if err != nil {
			return
		}
		assert.Equal(t, name, SubjectToName(subject),
			"topic %q converted to subject %q must convert back unchanged", name, subject)
	})
}
