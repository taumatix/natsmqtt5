package natsmqtt5

import (
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5/packet"
)

// This file measures what OutboundTopicAliases saves, to inform whether it should default on and
// whether minAliasedTopic (8) is the right threshold (ROADMAP.md, "Outbound topic aliases"). It
// encodes real PUBLISH packets with the codec and runs the broker's own outAliases table; the
// numbers in the roadmap are from `go test -run TestMeasureOutboundAliasSavings -v`.

// topicMix is a stream of topic names as one subscriber on a broad wildcard would see them: many
// devices publishing telemetry (a long hierarchical name, a skewed few chatty ones), commands
// and status on mid-length names, and a few short names.
func topicMix(n int, seed int64) []string {
	r := rand.New(rand.NewSource(seed))
	zipf := rand.NewZipf(r, 1.2, 1, 399)
	out := make([]string, n)
	for i := range out {
		dev := zipf.Uint64()
		switch p := r.Intn(100); {
		case p < 70:
			out[i] = fmt.Sprintf("dt/site-%d/building-%d/device-%03d/telemetry", dev%7, dev%23, dev)
		case p < 85:
			out[i] = fmt.Sprintf("cmd/device-%03d/state", dev)
		case p < 93:
			out[i] = fmt.Sprintf("st/%d", dev%40) // short: 4-5 bytes
		default:
			out[i] = fmt.Sprintf("a/%d/b", dev%90) // 5-6 bytes
		}
	}
	return out
}

// savings returns the bytes a connection sends for the mix with aliases off and with them on for
// names of at least minLen bytes and a table of max entries, using the same decisions as
// conn.aliasPublish.
func savings(t *testing.T, topics []string, payload []byte, max uint16, minLen int) (plain, aliased int) {
	t.Helper()
	tab := newOutAliases(max)
	for _, topic := range topics {
		p := &packet.Publish{Topic: topic, Payload: payload}
		raw, err := packet.Encode(p)
		require.NoError(t, err)
		plain += len(raw)
		out := raw
		if len(topic) >= minLen && max > 0 {
			out = tab.encode(p, raw, 0)
		}
		aliased += len(out)
	}
	return plain, aliased
}

// The simulation and the broker agree: with minAliasedTopic, savings() sends what aliasPublish
// would, byte for byte.
func TestTheMeasurementUsesTheBrokersOwnDecisions(t *testing.T) {
	topics := topicMix(2000, 1)
	payload := []byte("21.5")
	c := &conn{outAlias: newOutAliases(32)}
	var viaConn int
	for _, topic := range topics {
		p := &packet.Publish{Topic: topic, Payload: payload}
		raw, err := packet.Encode(p)
		require.NoError(t, err)
		viaConn += len(c.aliasPublish(p, raw))
	}
	_, simulated := savings(t, topics, payload, 32, minAliasedTopic)
	assert.Equal(t, viaConn, simulated)
}

func TestMeasureOutboundAliasSavings(t *testing.T) {
	// The table in ROADMAP.md was measured over 100,000 PUBLISHes; that takes ten seconds under -race, so
	// a run takes a tenth of it, which holds the same skew; NATSMQTT5_SLOW_TESTS=1 runs the whole mix.
	n := 10000
	if os.Getenv("NATSMQTT5_SLOW_TESTS") != "" {
		n = 100000
	}
	topics := topicMix(n, 42)
	payloads := map[string][]byte{"4B": []byte("21.5"), "64B": make([]byte, 64), "512B": make([]byte, 512)}

	for _, name := range []string{"4B", "64B", "512B"} {
		for _, max := range []uint16{1, 8, 32, 128, 400} {
			plain, aliased := savings(t, topics, payloads[name], max, minAliasedTopic)
			t.Logf("payload %-4s table %-3d: %9d -> %9d bytes, saved %5.1f%%", name, max, plain, aliased, 100*float64(plain-aliased)/float64(plain))
			if name == "4B" && max == 128 {
				assert.Less(t, aliased, plain, "the mix saves bytes with a table that holds its hot topics")
			}
		}
	}
	for _, minLen := range []int{1, 4, 5, 6, 8, 12, 16, 32} {
		for _, max := range []uint16{8, 32} {
			plain, aliased := savings(t, topics, payloads["4B"], max, minLen)
			t.Logf("min name %-2d table %-2d: saved %5.1f%%", minLen, max, 100*float64(plain-aliased)/float64(plain))
		}
	}

	// Break-even for one name of L bytes sent n times: the first use costs 3 bytes (the Topic
	// Alias property), each later use saves L-3.
	for _, l := range []int{3, 4, 5, 8} {
		var plain, aliased int
		name := string(make([]byte, l))
		for n := 1; n <= 6; n++ {
			p, a := savings(t, repeat(name, n), nil, 8, 1)
			plain, aliased = p, a
			if aliased < plain {
				t.Logf("a %d-byte name starts to pay off at its %d-th send", l, n)
				break
			}
		}
	}
}

func repeat(s string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = s
	}
	return out
}

// Measured before the fix (ROADMAP.md): the first-sighting policy sends more bytes than no aliases at
// a table of 1 and in every mix that cycles through more names than the table holds. Whatever the
// policy, aliases must not cost more than a constant over none.
func TestAliasesNeverCostBytesWhateverTheClientMaximum(t *testing.T) {
	cycle := make([]string, 0, 3000)
	for i := 0; i < 3000; i++ { // a round robin over 40 names, the worst case for an LRU
		cycle = append(cycle, fmt.Sprintf("dt/site-1/building-2/device-%03d/telemetry", i%40))
	}
	mixes := map[string][]string{"skewed mix": topicMix(5000, 7), "round robin over 40 names": cycle}
	for name, topics := range mixes {
		for _, max := range []uint16{1, 2, 4, 8, 32} {
			plain, aliased := savings(t, topics, []byte("21.5"), max, minAliasedTopic)
			t.Logf("%s, table %d: %d -> %d bytes", name, max, plain, aliased)
			// A constant, not a share of the stream: the ledger lets a table pay for at most one alias
			// beyond what it has earned, plus the free ones.
			assert.LessOrEqual(t, aliased, plain+3*(int(max)+1), "%s with table %d", name, max)
		}
	}
}

// A full table admits a name on its second sighting. Before it, a table of 8 sent 671,872 bytes
// for the skewed mix (20,000 PUBLISHes, seed 42) and 150,003 for a round robin over 40 names;
// first-seen names evicted hot ones. These bounds sit between those figures and today's.
func TestAFullTableAdmitsANameOnItsSecondSighting(t *testing.T) {
	cycle := make([]string, 0, 3000)
	for i := 0; i < 3000; i++ {
		cycle = append(cycle, fmt.Sprintf("dt/site-1/building-2/device-%03d/telemetry", i%40))
	}
	_, skewed := savings(t, topicMix(20000, 42), []byte("21.5"), 8, minAliasedTopic)
	assert.Less(t, skewed, 600000, "skewed mix, table 8 (was 671,872, plain 832,649)")
	_, rr := savings(t, cycle, []byte("21.5"), 8, minAliasedTopic)
	assert.Less(t, rr, 145000, "round robin over 40 names, table 8 (was 150,003, plain 150,000)")
}

func TestASecondSightingNameTakesTheLeastRecentlyUsedAlias(t *testing.T) {
	tab := newOutAliases(1)
	a, b := "dt/aaaaaaaa/telemetry", "dt/bbbbbbbb/telemetry"
	send := func(topic string) (aliased bool) {
		p := &packet.Publish{Topic: topic, Payload: []byte("1")}
		raw, err := packet.Encode(p)
		require.NoError(t, err)
		return len(tab.encode(p, raw, 0)) != len(raw)
	}
	require.True(t, send(a), "a free alias is taken on first sight")
	require.True(t, send(a))
	require.False(t, send(b), "b is first seen with the table full: sent plain")
	require.True(t, send(a), "a keeps its alias")
	require.True(t, send(b), "b, seen again, remaps the alias")
}
