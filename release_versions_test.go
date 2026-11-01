package natsmqtt5_test

import (
	"os"
	"regexp"
	"strconv"
	"strings"
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
		regexp.MustCompile(`github\.com/taumatix/natsmqtt5(?:/v[0-9]+)?(?:/cmd/natsmqtt5)?@(v[0-9][^\s` + "`" + `]*)`),
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

// TestModulePathCarriesTheMajorVersion keeps go.mod's module path in step with
// the newest release: Go resolves "@v2.x.y" only against a path ending in
// "/v2", so a 2.0.0 heading in CHANGELOG.md with go.mod still on the v1 path
// would publish a tag nothing can fetch, and a README install line without the
// suffix would fetch the wrong major.
//
// The path may run ahead of the newest release (go.mod says /v2 while 1.1.0 is
// still the newest heading: the preparation for a major version), never behind.
func TestModulePathCarriesTheMajorVersion(t *testing.T) {
	released, err := strconv.Atoi(strings.TrimPrefix(strings.SplitN(newestReleasedVersion(t), ".", 2)[0], "v"))
	require.NoError(t, err)

	m := regexp.MustCompile(`(?m)^module github\.com/taumatix/natsmqtt5(?:/v([0-9]+))?$`).FindStringSubmatch(readRepoFile(t, "go.mod"))
	require.NotNil(t, m, "go.mod names another module")
	declared := 1
	if m[1] != "" {
		declared, err = strconv.Atoi(m[1])
		require.NoError(t, err)
		require.GreaterOrEqual(t, declared, 2, "a /v1 suffix is not a module path")
	}
	assert.GreaterOrEqual(t, declared, released,
		"CHANGELOG.md has a v%d release but go.mod's module path is /v%d", released, declared)

	if released >= 2 {
		suffix := "/v" + strconv.Itoa(released)
		install := regexp.MustCompile("github\\.com/taumatix/natsmqtt5[^\\s`]*@v[0-9][^\\s`]*")
		for _, line := range install.FindAllString(readRepoFile(t, "README.md"), -1) {
			assert.Contains(t, line, suffix, "README install line %q lacks the major version suffix", line)
		}
	}
}

func readRepoFile(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(name)
	require.NoError(t, err)
	return string(b)
}
