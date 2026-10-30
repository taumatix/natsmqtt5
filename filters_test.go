package natsmqtt5

import (
	"math/rand/v2"
	"reflect"
	"strings"
	"testing"
)

func TestReplayFilters(t *testing.T) {
	q := &offlineQueue{prefix: "p"}
	subs := func(fs ...string) []*subscription {
		var out []*subscription
		for _, f := range fs {
			out = append(out, &subscription{filter: f})
		}
		return out
	}
	shared := &subscription{filter: "$share/g/a/b", share: "g"}
	cases := []struct {
		name string
		in   []*subscription
		want []string
	}{
		{"none", nil, nil},
		{"shared only", []*subscription{shared}, nil},
		{"plain", subs("a/b"), []string{"p.$queue.a.b"}},
		{"hash adds parent", subs("a/#"), []string{"p.$queue.a.>", "p.$queue.a"}},
		{"covered", subs("a/#", "a/b"), []string{"p.$queue.a.>", "p.$queue.a"}},
		{"widened", subs("a/b", "a/+"), []string{"p.$queue.a.*"}},
		{"disjoint", subs("a/b", "c/d"), []string{"p.$queue.a.b", "p.$queue.c.d"}},
		{"crossing wildcards are joined", subs("a/+/c", "a/b/+"), []string{"p.$queue.a.*.*"}},
		{"crossing under a hash", subs("a/b/#", "a/+/c/d"), []string{"p.$queue.a.b", "p.$queue.a.*.>"}},
		{"a later broad filter swallows every narrower one", subs("a/b", "a/+/c", "a/#"), []string{"p.$queue.a.>", "p.$queue.a"}},
		{"a join that overlaps a third is folded again", subs("a/+/c", "a/b/+", "a/b/c/d/e", "a/+/+/d/+"), []string{"p.$queue.a.*.*", "p.$queue.a.*.*.d.*"}},
		{"all", subs("#"), []string{"p.$queue.>"}},
	}
	for _, c := range cases {
		if got := q.replayFilters(c.in); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}

// Whatever the subscriptions, the filters handed to JetStream overlap nothing
// and cover every filter the session holds.
func TestReplayFiltersNeverOverlapAndCoverEverySubscription(t *testing.T) {
	q := &offlineQueue{prefix: "p"}
	toks := []string{"a", "b", "+", "#"}
	var filters []string
	var gen func(prefix []string, depth int)
	gen = func(prefix []string, depth int) {
		if len(prefix) > 0 {
			filters = append(filters, strings.Join(prefix, "/"))
		}
		if depth == 0 || (len(prefix) > 0 && prefix[len(prefix)-1] == "#") {
			return
		}
		for _, tk := range toks {
			gen(append(append([]string{}, prefix...), tk), depth-1)
		}
	}
	gen(nil, 3)
	rng := rand.New(rand.NewPCG(1, 2))
	for n := 0; n < 3000; n++ {
		var subs []*subscription
		for k := 0; k < 2+rng.IntN(4); k++ {
			subs = append(subs, &subscription{filter: filters[rng.IntN(len(filters))]})
		}
		got := q.replayFilters(subs)
		for i := range got {
			for j := i + 1; j < len(got); j++ {
				if subjectsOverlap(got[i], got[j]) {
					t.Fatalf("%v: %q and %q overlap", filterNames(subs), got[i], got[j])
				}
			}
		}
		for _, s := range subs {
			subj, _ := q.codec.filterToSubject(s.filter)
			want := queuedSubject("p", subj)
			ok := false
			for _, g := range got {
				ok = ok || covers(g, want)
			}
			if !ok {
				t.Fatalf("%v: %q is not covered by %v", filterNames(subs), want, got)
			}
		}
	}
}

func filterNames(subs []*subscription) []string {
	var out []string
	for _, s := range subs {
		out = append(out, s.filter)
	}
	return out
}
