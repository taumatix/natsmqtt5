package natsmqtt5_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/taumatix/natsmqtt5/packet"
)

// Switching Options.EscapeNATSWildcards with data in JetStream (ROADMAP: "Escaping * and >").
//
// Turning it on needs no migration: a topic the off mapping could store contains none of the
// characters the on mapping escapes, and NameToSubjectEscaped gives such a topic the very
// subject NameToSubject does. These tests drive that over real sockets, two brokers one after
// another on one NATS server with JetStream, a retained message and an offline-queued one
// written by the first and read by the second.
func TestTurningEscapingOnKeepsTheRetainedAndQueuedMessagesTheBrokerAlreadyHeld(t *testing.T) {
	natsURL := startNATS(t)
	sessionConnect := func() *packet.Connect {
		cp := rawConnect("switch-sub", 300)
		cp.CleanStart = false
		cp.Properties = &packet.Properties{SessionExpiryInterval: packet.Uint32(300)}
		return cp
	}

	// Broker A, escaping off: a retained message with the awkward-but-storable characters
	// ('.', '/' at the edges, a leading '$app') and a message queued for an offline session.
	addrA, stopA := startStoppableBroker(t, natsURL, persistent)
	sub := dialRaw(t, addrA)
	sub.connect(sessionConnect())
	sub.subscribe("queued/#", packet.QoS1)
	sub.write(rawDisconnect)
	pub, _ := connectClient(t, addrA, connectOpts("switch-pub"))
	publishNamed(t, pub, "kept/a.b/c", true)
	publishNamed(t, pub, "queued/x.y/", false)
	stopA()

	// Broker B, escaping on, the same NATS server.
	addrB := startBroker(t, natsURL, persistent, withEscapes)
	late := dialRaw(t, addrB)
	late.connect(rawConnect("switch-late", 300))
	late.subscribe("kept/#", packet.QoS1)
	got := late.expectPublish()
	assert.Equal(t, "kept/a.b/c", got.Topic, "the retained message survives the switch")
	assert.True(t, got.Retain)

	back := dialRaw(t, addrB)
	back.connect(sessionConnect())
	queued := back.expectPublish()
	assert.Equal(t, "queued/x.y/", queued.Topic, "the queued message survives the switch")
}

// Turning it off is the direction with a hazard, and no rewrite can fix it: a topic that needed
// escaping has no subject under the plain mapping at all (that is why the plain broker refuses it
// with 0x90). The plain broker does not refuse what is already stored, though: it reads the
// escaped subject "ret.a/sb" back as the topic "ret/asb". This test records that, so the
// procedure the README gives (clear such retained messages before switching) is the one the
// behaviour calls for.
func TestTurningEscapingOffReadsAnEscapedRetainedMessageUnderAWrongName(t *testing.T) {
	natsURL := startNATS(t)
	addrA, stopA := startStoppableBroker(t, natsURL, persistent, withEscapes)
	pub, _ := connectClient(t, addrA, connectOpts("switch-off-pub"))
	publishNamed(t, pub, "ret/a*b", true)
	stopA()

	addrB := startBroker(t, natsURL, persistent)
	late := dialRaw(t, addrB)
	late.connect(rawConnect("switch-off-late", 300))
	late.subscribe("ret/#", packet.QoS1)
	got := late.expectPublish()
	assert.NotEqual(t, "ret/a*b", got.Topic, "the plain mapping cannot name the topic")
	t.Logf("the plain broker serves it as %q", got.Topic)
}
