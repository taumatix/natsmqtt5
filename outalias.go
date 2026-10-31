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
}

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
	t := c.outAlias
	if t == nil || len(p.Topic) < minAliasedTopic {
		return raw
	}
	if alias, ok := t.hit(p.Topic); ok {
		if out, err := packet.Encode(withAlias(p, alias, true)); err == nil {
			return out
		}
		return raw
	}
	out, err := packet.Encode(withAlias(p, t.next(), false))
	if err != nil || (c.clientMaxPacketSize > 0 && overLimit(len(out), c.clientMaxPacketSize)) {
		return raw
	}
	t.assign(p.Topic)
	return out
}
