package natsmqtt5

import (
	"reflect"
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
		{"crossing wildcards read everything", subs("a/+/c", "a/b/+"), nil},
		{"all", subs("#"), []string{"p.$queue.>"}},
	}
	for _, c := range cases {
		if got := q.replayFilters(c.in); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}
