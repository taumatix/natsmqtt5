package natsmqtt5_test

import (
	"os"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReleaseVersionsAgree keeps every version a user is told to install equal
// to the newest release in CHANGELOG.md: the image compose.yaml starts, and the
// go get, go install, docker run and compose download lines in the README.
//
// It exists because a comment asking for the bump did not work. compose.yaml
// pinned ghcr.io/taumatix/natsmqtt5:v0.4.2 from v0.4.3 through v0.8.0, so for
// five releases the quickstart the README hands out at each tag started an old
// broker, and nothing failed: compose.ci.yaml overrides the image in CI.
//
// A release PR moves the [Unreleased] entries under a new heading and bumps
// these lines in the same commit, so this holds on every commit of main.
func TestReleaseVersionsAgree(t *testing.T) {
	latest := newestReleasedVersion(t)

	compose := readRepoFile(t, "compose.yaml")
	pins := regexp.MustCompile(`ghcr\.io/taumatix/natsmqtt5:(v[0-9][^\s"']*)`).FindAllStringSubmatch(compose, -1)
	require.NotEmpty(t, pins, "compose.yaml pins no natsmqtt5 image")
	for _, p := range pins {
		assert.Equal(t, latest, p[1], "compose.yaml pins an image other than the newest release")
	}

	readme := readRepoFile(t, "README.md")
	installLines := []*regexp.Regexp{
		regexp.MustCompile(`github\.com/taumatix/natsmqtt5(?:/cmd/natsmqtt5)?@(v[0-9][^\s` + "`" + `]*)`),
		regexp.MustCompile(`ghcr\.io/taumatix/natsmqtt5:(v[0-9][^\s` + "`" + `]*)`),
		regexp.MustCompile(`raw\.githubusercontent\.com/taumatix/natsmqtt5/(v[0-9][^/]*)/compose\.yaml`),
	}
	found := 0
	for _, re := range installLines {
		for _, m := range re.FindAllStringSubmatch(readme, -1) {
			found++
			assert.Equal(t, latest, m[1], "README install line %q names another version", m[0])
		}
	}
	assert.GreaterOrEqual(t, found, 4, "README lost its install lines; this test would pass vacuously")
}

// newestReleasedVersion is the first "## [X.Y.Z]" heading in CHANGELOG.md, as
// a tag: v-prefixed. [Unreleased] is skipped by the pattern.
func newestReleasedVersion(t *testing.T) string {
	t.Helper()
	m := regexp.MustCompile(`(?m)^## \[([0-9]+\.[0-9]+\.[0-9]+)\]`).FindStringSubmatch(readRepoFile(t, "CHANGELOG.md"))
	require.NotNil(t, m, "CHANGELOG.md has no released version heading")
	return "v" + m[1]
}

func readRepoFile(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(name)
	require.NoError(t, err)
	return string(b)
}
