package natsmqtt5

import (
	"crypto/sha256"
	"sync"
)

// publishIDs gives a retransmitted publish the message id its refused first attempt had
// (Options.DurablePublishDedupe), so the queue stream can drop the second copy by its Nats-Msg-Id.
//
// A packet identifier is reused after its acknowledgement, so it is not an id by itself. An entry is kept
// only for a publish that was refused or never acknowledged, is matched only by a PUBLISH marked DUP
// [MQTT-3.3.1-1] whose topic and payload are the same, and is forgotten when the publish is acknowledged or
// when anything else arrives under that identifier. The zero value is ready to use.
type publishIDs struct {
	mu      sync.Mutex
	pending map[uint16]pendingPublish
}

type pendingPublish struct {
	id  string
	sum [sha256.Size]byte
}

func publishSum(topicName string, payload []byte) [sha256.Size]byte {
	h := sha256.New()
	h.Write([]byte(topicName))
	h.Write([]byte{0})
	h.Write(payload)
	var sum [sha256.Size]byte
	h.Sum(sum[:0])
	return sum
}

// idFor returns the id of the first attempt when this is its retransmission, and otherwise records fresh as
// the id of this publish until done is called.
func (p *publishIDs) idFor(packetID uint16, dup bool, topicName string, payload []byte, fresh string) string {
	sum := publishSum(topicName, payload)
	p.mu.Lock()
	defer p.mu.Unlock()
	if e, ok := p.pending[packetID]; ok && dup && e.sum == sum {
		return e.id
	}
	if p.pending == nil {
		p.pending = make(map[uint16]pendingPublish)
	}
	p.pending[packetID] = pendingPublish{id: fresh, sum: sum}
	return fresh
}

// done forgets the id of an acknowledged publish.
func (p *publishIDs) done(packetID uint16) {
	p.mu.Lock()
	delete(p.pending, packetID)
	p.mu.Unlock()
}
