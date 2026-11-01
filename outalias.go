package natsmqtt5

import (
	"container/list"

	"github.com/taumatix/natsmqtt5/packet"
)

// minAliasedTopic is the shortest Topic Name worth an alias. Reusing an alias
// replaces the name with a 3-byte property and a 2-byte empty name, so a short
// name gains nothing and would only take a slot from a long one.
const minAliasedTopic = 8

// outAliases is the table of Topic Aliases the broker has set on one network
// connection, for the PUBLISH packets it sends. The client's Topic Alias
// Maximum bounds it; when it is full the least recently used alias is given to
// the new topic, which the client takes as a remapping [MQTT-3.3.2-8]. A table
// lives and dies with the connection [MQTT-3.3.2-7]. Only the write path uses
// it, under the connection's writeMu.
type outAliases struct {
	max     uint16
	byTopic map[string]*list.Element // front of order is the most recently used
	order   *list.List
	saved   int
	spent   int
	seen    map[string]struct{} // names sent plain once while the table was full
}

// maxSeenAliasNames bounds seen; when it fills it starts over, so a name must
// recur within about this many other new names to be admitted.
const maxSeenAliasNames = 32

// The table also keeps a ledger: saved is the bytes the aliases already set
// have taken off the wire (a name left out of a PUBLISH), spent the bytes it
// took to set them (the Topic Alias property on the PUBLISH that sets one). A
// client whose maximum is below the set of names it sees would otherwise have
// each name evict the next before it is reused, paying for every alias and
// saving nothing; with the ledger the table stops remapping once it has spent
// more than it saved, and goes on again when the aliases it kept have paid.
// Filling free aliases is not held to it, which bounds the cost of a
// connection that never repeats a name at 3 bytes per alias the client offered.

type outAlias struct {
	topic string
	alias uint16
}

func newOutAliases(max uint16) *outAliases {
	return &outAliases{max: max, byTopic: make(map[string]*list.Element), order: list.New()}
}

// hit reports the alias already set for topic and marks it used.
func (t *outAliases) hit(topic string) (uint16, bool) {
	el, ok := t.byTopic[topic]
	if !ok {
		return 0, false
	}
	t.order.MoveToFront(el)
	return el.Value.(*outAlias).alias, true
}

// next is the alias a new topic would take, without taking it.
func (t *outAliases) next() uint16 {
	if t.order.Len() < int(t.max) {
		return uint16(t.order.Len()) + 1 //nolint:gosec // G115: Len() < max, and max is a uint16
	}
	return t.order.Back().Value.(*outAlias).alias
}

// assign records that topic now has the alias next returned, replacing the
// least recently used topic when the table is full.
func (t *outAliases) assign(topic string) {
	a := &outAlias{topic: topic, alias: t.next()}
	if t.order.Len() >= int(t.max) {
		old := t.order.Back()
		delete(t.byTopic, old.Value.(*outAlias).topic)
		t.order.Remove(old)
	}
	t.byTopic[topic] = t.order.PushFront(a)
}

// withAlias is a copy of p carrying alias, with the Topic Name left out when the
// client already has it. p itself, which the session may keep for a resend, is
// not changed.
func withAlias(p *packet.Publish, alias uint16, omitTopic bool) *packet.Publish {
	out := *p
	var props packet.Properties
	if p.Properties != nil {
		props = *p.Properties
	}
	props.TopicAlias = &alias
	out.Properties = &props
	if omitTopic {
		out.Topic = ""
	}
	return &out
}

// aliasPublish returns the encoded form of p using the connection's Topic
// Alias table, or raw (its plain encoding) when aliasing does not apply or
// would take the packet over the client's Maximum Packet Size.
func (c *conn) aliasPublish(p *packet.Publish, raw []byte) []byte {
	if c.outAlias == nil || len(p.Topic) < minAliasedTopic {
		return raw
	}
	return c.outAlias.encode(p, raw, c.clientMaxPacketSize)
}

// encode is the aliased form of p, or raw when the table declines. A name
// already in the table goes as its alias alone. A new name takes a free alias;
// when the table is full it replaces the least recently used one only while
// the name has been sent before and the aliases so far have saved at least what
// setting them cost (see ledger),
// so a client whose maximum is smaller than the set of names it receives never
// pays more than it would without aliases.
func (t *outAliases) encode(p *packet.Publish, raw []byte, maxPacketSize uint32) []byte {
	if alias, ok := t.hit(p.Topic); ok {
		if out, err := packet.Encode(withAlias(p, alias, true)); err == nil {
			t.saved += len(raw) - len(out)
			return out
		}
		return raw
	}
	if t.order.Len() >= int(t.max) && t.saved < t.spent {
		return raw
	}
	if t.order.Len() >= int(t.max) {
		// A full table gives a name an alias only on its second sighting: a name
		// seen once would evict a hot one and, if it never returns, cost the
		// remapping for nothing.
		if _, again := t.seen[p.Topic]; !again {
			if t.seen == nil || len(t.seen) >= maxSeenAliasNames {
				t.seen = make(map[string]struct{})
			}
			t.seen[p.Topic] = struct{}{}
			return raw
		}
		delete(t.seen, p.Topic)
	}
	out, err := packet.Encode(withAlias(p, t.next(), false))
	if err != nil || (maxPacketSize > 0 && overLimit(len(out), maxPacketSize)) {
		return raw
	}
	t.spent += len(out) - len(raw)
	t.assign(p.Topic)
	return out
}
