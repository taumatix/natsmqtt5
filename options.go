// Package natsmqtt5 is an MQTT version 5.0 broker that uses an existing NATS
// server as its messaging core.
//
// The broker is a NATS client, not a fork of nats-server. Point it at a NATS
// URL you already run and it speaks MQTT v5 on a TCP port, translating topics
// to NATS subjects, fan-out to NATS subscriptions, shared subscriptions to
// NATS queue groups and retained messages to a JetStream stream. Two brokers
// against the same NATS server see each other's traffic, so an MQTT client on
// one reaches a subscriber on the other.
//
// The subject mapping is byte-for-byte the one nats-server uses for its own
// MQTT 3.1.1 support, so topics land where a NATS-native subscriber expects.
//
// Minimal use:
//
//	b, err := natsmqtt5.New(natsmqtt5.Options{NATSURL: "nats://localhost:4222"})
//	if err != nil {
//		return err
//	}
//	defer b.Close()
//	return b.Serve(ctx)
//
// Section numbers in the documentation refer to the OASIS MQTT v5.0 Standard
// of 07 March 2019.
package natsmqtt5

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/taumatix/natsmqtt5/v2/packet"
)

// Defaults applied to the zero value of the corresponding Options field.
const (
	DefaultListen        = ":1883"
	DefaultSubjectPrefix = "mqtt"
	DefaultStreamPrefix  = "MQTT5"
	// DefaultReceiveMaximum bounds how many QoS 1 and QoS 2 publications a
	// client may have in flight towards the broker (MQTT-5.0 §3.2.2.3.3).
	DefaultReceiveMaximum = 1024
	// DefaultTopicAliasMaximum is the highest Topic Alias the broker accepts
	// from a client (MQTT-5.0 §3.1.2.11.5).
	DefaultTopicAliasMaximum = 64
	// DefaultMaximumPacketSize caps an inbound packet at 64 MiB
	// (MQTT-5.0 §3.2.2.3.6).
	DefaultMaximumPacketSize = 64 << 20
	// DefaultConnectTimeout bounds how long a new connection may stay silent
	// before sending its CONNECT (MQTT-5.0 §3.1.4, step 1).
	DefaultConnectTimeout = 10 * time.Second
	// DefaultMaxSessionExpiry caps the Session Expiry Interval a client may
	// request. The broker returns the capped value in CONNACK, as
	// MQTT-5.0 §3.2.2.3.2 provides for.
	DefaultMaxSessionExpiry = time.Hour
	// DefaultSessionCheckpointInterval is how often a broker with
	// PersistentSessions writes where a connected client stands to the session
	// record, when something has changed.
	DefaultSessionCheckpointInterval = time.Second
)

// Options configures a Broker. The zero value is usable: it connects to
// nats.DefaultURL and listens on :1883.
//
// Fields are only ever added to this struct, never removed or repurposed, so
// a composite literal with field names keeps compiling across versions.
type Options struct {
	// NATSURL is the NATS server to use. Defaults to nats.DefaultURL. Ignored
	// when Conn is set.
	NATSURL string
	// NATSOptions are passed to nats.Connect for credentials, TLS and so on.
	// Ignored when Conn is set.
	NATSOptions []nats.Option
	// Conn is an already-established NATS connection to use instead of
	// dialling. The broker does not close a connection it did not open.
	Conn *nats.Conn

	// Listen is the TCP address to accept MQTT connections on. Defaults to
	// DefaultListen. Ignored when Listener is set.
	Listen string
	// Listener is an already-open listener to accept on instead of binding
	// Listen. The broker closes it on shutdown.
	Listener net.Listener
	// TLSConfig, when non-nil, wraps the listener in TLS.
	TLSConfig *tls.Config
	// WebSocketOriginPatterns lists the Origin hosts (path.Match patterns,
	// e.g. "app.example.com" or "*.example.com") a browser may connect from
	// through Broker.WebSocketHandler, besides the request's own host. A
	// request without an Origin header (any non-browser client) is always
	// accepted. Use "*" only if the broker's own authentication is what
	// protects it.
	WebSocketOriginPatterns []string

	// SubjectPrefix namespaces every NATS subject the broker publishes MQTT
	// traffic on, keeping it clear of NATS system subjects. Defaults to
	// DefaultSubjectPrefix. Set it to the same value on every broker that
	// should share traffic.
	SubjectPrefix string
	// StreamPrefix names the JetStream assets the broker creates. Defaults to
	// DefaultStreamPrefix. Brokers sharing a SubjectPrefix must share this.
	StreamPrefix string
	// DisableRetained turns off retained-message support, so no JetStream
	// assets are created and CONNACK advertises Retain Available = 0
	// (MQTT-5.0 §3.2.2.3.5). Use it against a NATS server without JetStream.
	DisableRetained bool
	// RetainedStorage selects file or memory storage for the retained-message
	// stream. Defaults to file storage.
	RetainedStorage jetstream.StorageType
	// RetainedReplicas is the JetStream replica count for the retained-message
	// stream. Defaults to 1.
	RetainedReplicas int
	// RetainedTombstoneTTL is how long the marker left in the retained stream
	// by clearing a retained message (an empty-payload retained PUBLISH) is
	// kept before the periodic sweep deletes it. Without the purge a workload
	// that retains and clears many distinct topics grows the stream without
	// bound. Defaults to DefaultRetainedTombstoneTTL; a negative value keeps
	// tombstones forever. A broker cut off from NATS for longer than this can
	// miss a clear and keep the cleared message until it restarts.
	RetainedTombstoneTTL time.Duration
	// RetainedVerifyInterval makes the periodic sweep check every entry of the retained
	// view against the stream at least this often (one lookup per entry), on top of the
	// count comparison it always makes and the full check after a NATS reconnect. It finds
	// a stale entry that a partition too short for the NATS client to notice left behind,
	// which neither of those can see [MQTT-3.3.1-6]. Zero, the default, turns it off. The
	// sweep runs every SessionSweepInterval, so the check is made at the first sweep after
	// the interval has passed.
	RetainedVerifyInterval time.Duration
	// SharedHoldLimit bounds how long a shared-subscription member may hold a
	// QoS 1 message without acknowledging it. Past it the message is handed
	// back to the group, so a member that is connected but never sends PUBACK
	// does not keep it from the others, and the group may deliver it twice:
	// the late PUBACK is still accepted and completes the exchange with that
	// client. Zero, the default, holds for as long as the session lives. The
	// check runs every third of the group's acknowledgement wait (10 s), so the
	// limit is met to within that.
	SharedHoldLimit time.Duration

	// PersistentSessions keeps session state in a JetStream key-value bucket
	// instead of in the broker process, so a client reconnecting with Clean
	// Start 0 resumes its subscriptions after a broker restart and on a broker
	// that has never seen it. Brokers sharing a StreamPrefix share the bucket
	// and arbitrate ownership between themselves, so exactly one of them serves
	// a given Client Identifier [MQTT-3.1.4-3].
	//
	// It needs JetStream, and it caps the Client Identifier at
	// MaxPersistentClientIDLen bytes.
	//
	// A resumed session carries its subscription set, and the offline queue
	// delivers what was published while it was away. Messages that were in
	// flight to the client when its broker went down are not stored, so they
	// are not resent. The Will Message is not part of the session record, because
	// it belongs to the network connection rather than to the session
	// (MQTT-5.0 §3.1.2.5); DurableWills stores it separately. ROADMAP.md has
	// the first.
	PersistentSessions bool
	// SessionStorage selects file or memory storage for the session bucket.
	// Defaults to file storage. Memory storage makes sessions survive a broker
	// restart but not a NATS restart.
	SessionStorage jetstream.StorageType
	// SessionReplicas is the JetStream replica count for the session bucket.
	// Defaults to 1.
	SessionReplicas int

	// RestrictDollarTopics follows MQTT-5.0 §4.7.2's "The Server SHOULD
	// prevent Clients from using such Topic Names to exchange messages with
	// other Clients" for every Topic Name and Topic Filter that starts with
	// "$": a PUBLISH or Will to one is refused with Topic Name invalid (0x90),
	// and a subscription to one with Topic Filter invalid (0x8F). A Shared
	// Subscription ($share/…) is allowed unless the Topic Filter inside it
	// starts with "$". On by default since v1.0.0 (MIGRATION.md); "$retained"
	// and "$queue", where the broker keeps its own data, are refused whatever
	// the setting. It does not apply to a subscription a persistent session
	// already stored.
	//
	// Because the default is on, false here cannot turn it off: set
	// PinRestrictDollarTopics to Ptr(false) to leave "$app/…" and the like
	// usable. Setting this to true is redundant and kept for compatibility.
	RestrictDollarTopics bool
	// PinRestrictDollarTopics fixes RestrictDollarTopics whatever the default
	// is, and beats RestrictDollarTopics when both are set: Ptr(false) keeps
	// "$app/…" usable (the behaviour before v1.0.0), Ptr(true) refuses it.
	// nil leaves the choice to RestrictDollarTopics and the default, which is on.
	PinRestrictDollarTopics *bool

	// DurableWills keeps the Will Message of every open connection in a
	// JetStream key-value bucket, so that the Will of a broker that is killed
	// outright is still published: a surviving broker notices the owner has
	// stopped answering its liveness subject, adopts the Will, waits out
	// whatever is left of the Will Delay Interval and publishes it
	// [MQTT-3.1.2-7], [MQTT-3.1.2-8], [MQTT-3.1.3-9]. Exactly one broker publishes a given Will,
	// by compare-and-swap on the record.
	//
	// On by default since v1.0.0 (MIGRATION.md) whenever JetStream is in use
	// (retained messages, PersistentSessions or OfflineQueue); a broker that
	// uses none of them has no JetStream and no durable Wills unless
	// PinDurableWills forces it. It changes where Wills live: the Will's payload
	// is written to the "<StreamPrefix>_wills" bucket in clear text, so that
	// bucket needs the access control of the session bucket. It needs JetStream
	// and works with or without PersistentSessions; without them a client that
	// reconnects to a different broker starts a new session, so a Will waiting
	// out its delay is published when it does, rather than cancelled.
	//
	// The delay is counted from the moment a surviving broker notices, which is
	// up to WillCheckInterval after the connection was lost.
	//
	// Because the default is on, false here cannot turn it off: set
	// PinDurableWills to Ptr(false) to keep Wills in memory. Setting this to
	// true also forces JetStream on, as before.
	DurableWills bool
	// PinDurableWills fixes DurableWills whatever the default is, and beats
	// DurableWills when both are set: Ptr(false) keeps Wills in memory (the
	// behaviour before v1.0.0), Ptr(true) stores them durably and starts
	// JetStream if nothing else did. nil leaves the choice to DurableWills and
	// the default, which is on whenever JetStream is in use.
	PinDurableWills *bool
	// WillCheckInterval is how often a broker looks for Wills whose owner has
	// gone, with DurableWills. It bounds how long after a broker's death its
	// clients' Wills wait to be noticed. Defaults to 5 seconds.
	//
	// It also sets the patience of a broker that loses NATS: one that cannot
	// have a write to the Will bucket accepted for twice this interval closes
	// its client connections, and a survivor waits about 5 intervals (plus the
	// liveness ping timeout, or at least two seconds below a one-second
	// interval) before adopting a silent broker's Wills, so a Will
	// is never published while its client's connection is open. A write needs a
	// majority of the bucket's servers, so a broker whose NATS server is cut off
	// from the rest of its cluster fences itself even though that server still
	// answers it. On a cluster, keep twice this interval above the time NATS
	// takes to elect a new leader for the bucket (it starts an election 4 to 9
	// seconds after the leader goes quiet): a shorter wait makes every broker
	// close its clients whenever the bucket's leader is lost.
	WillCheckInterval time.Duration

	// OfflineQueue requires the offline queue, so a broker that cannot set it
	// up fails to start.
	//
	// The queue keeps QoS 1 and QoS 2 messages for a session while its client
	// is disconnected, and delivers them when the session resumes, in publish
	// order and before anything published after the resume. MQTT-5.0 counts
	// those messages as session state that MUST outlive the connection
	// [MQTT-3.1.2-23], [MQTT-4.5.0-1], so the queue is on by default whenever
	// the broker uses JetStream (retained messages, the default, or
	// PersistentSessions); then a stream that cannot be created is logged at
	// warn level and the broker runs without it. Without the queue, messages
	// are dropped for an absent session, as core NATS keeps nothing.
	//
	// Each such message is published a second time, under
	// <SubjectPrefix>.$queue.<subject>, into a JetStream stream that keeps it
	// for OfflineQueueMaxAge, and the PUBACK or PUBREC waits for JetStream to
	// store it. It covers sessions held in this broker's memory and, with
	// PersistentSessions, sessions restored from the session store after a
	// restart or on another broker. Messages published straight onto NATS
	// rather than through a broker are not queued (ROADMAP.md).
	//
	// The queue also backs shared subscriptions: each gets a durable consumer
	// on the queue stream that only members with a connected client pull
	// from, so QoS 1 and 2 work goes to members that can take it and waits
	// when none can. Brokers sharing a SubjectPrefix must agree on whether the
	// queue is on, or a message can reach a group twice.
	OfflineQueue bool
	// DisableOfflineQueue turns the default offline queue off, for a
	// deployment that would rather not store every QoS 1 and 2 message and
	// accepts that a resumed session misses what was published while it was
	// away. Setting it together with OfflineQueue is an error.
	DisableOfflineQueue bool
	// CloseBehindClients disconnects a client that is 2048 messages behind, with DISCONNECT 0x97 (Quota
	// exceeded), when the broker has no offline queue to catch it up from (DisableOfflineQueue, or no
	// JetStream) and a QoS 1 or 2 message was about to be dropped. Without it the broker drops the message
	// and logs it, and the client is not told: the loss of a message the client was owed is invisible to
	// it. With it the loss is a lost connection, which the client sees, and its session is kept to resume
	// (SessionExpiryInterval permitting); messages dropped before the close, and those published while it
	// is away, are still lost, as without a queue they always are. It does not slow the publishers, and it
	// closes only the connection that fell behind, not the broker's NATS connection. QoS 0 loss never
	// closes a connection. It does nothing while the offline queue is on, where such a client catches up.
	CloseBehindClients bool
	// AwayBufferMessages is how many QoS 1 and 2 messages the broker holds in memory for a session
	// that is away, when it has no offline queue (DisableOfflineQueue, or no JetStream). They are
	// delivered, in publish order, when the client resumes the session. It is per session, counts
	// messages not bytes, and 0 means 1000; a negative value holds none, and the largest is 2048
	// (what a connection can wait to send). A session whose messages do not fit is ended instead of
	// kept with a gap: its client's next CONNECT with Clean Start 0 gets Session Present 0
	// [MQTT-3.2.2-3] and subscribes again, so a client told Session Present 1 has been sent
	// everything published while it was away [MQTT-3.1.2-23], [MQTT-4.5.0-1]. The messages are
	// lost with the process: a restart ends the sessions held in memory. It does nothing while the
	// offline queue is on.
	AwayBufferMessages int
	// DurablePublish makes the PUBACK of a QoS 1 PUBLISH and the PUBREC of a QoS 2
	// one mean that the message is safe, not only that the broker has it. It
	// implies OfflineQueue (the broker fails to start without the queue) and is
	// an error together with DisableOfflineQueue.
	//
	// Without it the broker already waits for JetStream to store the queue's
	// copy, but then hands the live copy to its NATS connection and
	// acknowledges at once; nats.go writes that from another goroutine, so a
	// broker killed in the window loses a live copy it has told the client
	// about. With it the broker also waits for the NATS server to confirm it
	// has processed the live publish (a flush: a PING answered by a PONG) before
	// it acknowledges. So an acknowledged message is (1) in the queue stream,
	// to the stream's replication and storage, for OfflineQueueMaxAge, and
	// (2) has reached the NATS server's routing. If either step fails or takes
	// longer than five seconds the client gets PUBACK / PUBREC 0x83
	// Implementation specific error and may send the message again.
	//
	// It does not make delivery to a subscriber exactly-once or guaranteed:
	// live delivery is core NATS, at most once per hop, and a session whose
	// client is away gets the queued copy on resume. It covers only QoS 1 and
	// 2 publishes from clients. A Will Message has no client to refuse to, so
	// its queue copy is tried up to three times instead (a try that timed out
	// may have stored its copy, so a Will can be queued twice). Costs a NATS round trip per
	// such publish on top of the queue's; see the README for measurements.
	DurablePublish bool
	// DurablePublishDedupe stops the retry of a refused DurablePublish from leaving a second copy in the
	// queue. A QoS 1 or 2 PUBLISH refused with 0x83 after its queue copy was stored (the flush failed, or
	// the queue answered too late) is sent again by the client, and without this the queue then holds the
	// message twice and a session that was away is offered it twice. With it, the broker gives each such
	// publish a Nats-Msg-Id and a retransmission of it (same packet identifier, DUP set, same topic and
	// payload) carries the same one, so JetStream drops the second copy inside the queue stream's
	// duplicate window (the stream's Duplicates, 2 minutes unless the operator changed it).
	//
	// A retry after the window has passed, one on a session that did not survive (Clean Start), or one that
	// differs in topic or payload is queued as before, and a live copy is still sent again, so a subscriber
	// that was connected can still see a retried message twice, as QoS 1 allows. A publish that was
	// acknowledged is forgotten, so a later message that reuses the packet identifier is never mistaken
	// for a duplicate. Requires DurablePublish; it is an error without it.
	DurablePublishDedupe bool
	// NATSIngest lets a plain NATS client publish through a broker. A message
	// published on a live subject straight onto NATS has no queue copy, so a
	// session that was away never gets it and a shared subscription gets it
	// only through its NATS queue group, which can pick a member whose client is
	// away. With this option the broker also listens on
	// <SubjectPrefix>.$ingest.<subject>, in a NATS queue group of every broker
	// that has it on, so one broker takes each message and handles it as an
	// MQTT PUBLISH on <subject>: the Mqtt5-* headers give its QoS, Retain flag
	// and properties (as on a live copy, see the README), the topic is checked
	// as a PUBLISH's is (and one starting with $ is always refused), a retained
	// message is stored, a QoS 1 or 2 message gets
	// its queue copy, and the live copy goes out with the broker's id and queue
	// sequence. A request is answered once that is done: an empty reply carrying
	// Mqtt5-Msg-Id and, when queued, Mqtt5-Queue-Seq; or one carrying
	// Mqtt5-Ingest-Error with the reason, after which nothing was published.
	//
	// The Authorizer is not asked: a NATS client has no MQTT identity, and NATS
	// permissions on the ingest subject decide who may use it. Off by default,
	// and then nothing listens on the subject.
	NATSIngest bool
	// QueuePlainPublishes queues a message a plain NATS client publishes straight
	// on a live subject, <SubjectPrefix>.<subject>, when its Mqtt5-Qos header is 1
	// or 2. On by default since v2.0.0 (MIGRATION.md). Such a message used to
	// reach the sessions subscribed to it and no queue: a session that was away
	// never got it, and a shared subscription got it only through its NATS queue
	// group, which can pick a member whose client is away. Now every broker with
	// an offline queue ignores it as a live copy, and one broker (a NATS queue
	// group on <SubjectPrefix>.>) takes it and does what NATSIngest does: checks
	// the topic and QoS as a PUBLISH's, stores a retained message, keeps the queue
	// copy and publishes the live copy again with Mqtt5-Msg-Id and
	// Mqtt5-Queue-Seq. A plain NATS subscriber therefore sees such a message
	// twice, the original and the broker's copy, which carries the Mqtt5-Msg-Id
	// header to tell them apart. A message with no Mqtt5-Qos header, or QoS 0, a
	// subject starting with $ and a message that already has an id are not
	// touched. Nothing answers the publisher: use NATSIngest to be told the
	// outcome. Brokers sharing a SubjectPrefix must agree on it.
	//
	// Because the default is on, false here cannot turn it off: set
	// PinQueuePlainPublishes to Ptr(false) for the v1 behaviour.
	QueuePlainPublishes bool
	// PinQueuePlainPublishes fixes QueuePlainPublishes whatever the default is,
	// and beats it when both are set. nil leaves the choice to the default, which
	// is on.
	PinQueuePlainPublishes *bool
	// ReconnectCatchUp makes a connected session get the QoS 1 and 2 messages
	// published while this broker's NATS connection was down. Live delivery is
	// core NATS: a broker that has lost its connection has no interest on the
	// server, so a message another broker publishes meanwhile reaches none of its
	// sessions, and without this option nothing sends it later (only a session
	// that was away is replayed).
	//
	// With it, when the connection drops the broker stops handing its sessions
	// live QoS 1 and 2 copies, and when it is back each connected session reads
	// the offline queue from OfflineQueueRewind before the drop: what it has
	// not been sent goes out first, in stream order, then live delivery
	// resumes. The ids the session keeps stop it sending anything twice.
	//
	// It needs the offline queue (an error together with DisableOfflineQueue)
	// and covers only messages with a queue copy: those published through a
	// broker. It covers a drop of this broker's own connection, which nats.go
	// reports, and a change in the cluster's servers the connection learns of
	// (a route that dropped while both brokers stayed connected); a route the
	// server has not noticed is dead, or nats.IgnoreDiscoveredServers, hides
	// that (ROADMAP.md). Costs one read of the
	// queue stream per connected session after each reconnect. Off by default.
	ReconnectCatchUp bool
	// OfflineQueueMaxAge is how long a queued message is kept. Defaults to 24
	// hours. A session away for longer loses what is older.
	OfflineQueueMaxAge time.Duration
	// OfflineQueueRewind is how far before a session's connection ended its
	// replay reaches for a queued message whose live copy never arrived (the
	// publisher stopped between storing the queue copy and publishing the live
	// one), and for one still on its way, when the session has no replay
	// cursor: one restored from a record written before the cursor existed, or
	// one whose cursor was dropped for its bounds (see the README). A session
	// with a cursor replays from it and finds such a message however long ago it
	// was stored. The session keeps the ids of what it delivered in that span so
	// that nothing is sent twice, so a longer rewind costs memory per session
	// and a read of that much of the queue stream on every resume. Defaults to
	// 10 seconds.
	OfflineQueueRewind time.Duration
	// OfflineQueueStorage selects file or memory storage for the queue
	// stream. Defaults to file storage.
	OfflineQueueStorage jetstream.StorageType
	// OfflineQueueReplicas is the JetStream replica count for the queue
	// stream. Defaults to 1.
	OfflineQueueReplicas int

	// Authenticator, when non-nil, decides whether a CONNECT is accepted. When
	// nil every connection is accepted, which is only appropriate on a trusted
	// network.
	Authenticator Authenticator
	// Authorizer, when non-nil, decides whether a session may publish to or
	// subscribe to a topic. When nil everything is permitted.
	Authorizer Authorizer
	// Auther, when non-nil, offers enhanced authentication: the AUTH-packet
	// exchange of MQTT-5.0 §4.12 for a CONNECT that names an Authentication
	// Method, and re-authentication on an established connection. When nil such a
	// CONNECT is refused with 0x8C (Bad authentication method).
	Auther Auther

	// ReceiveMaximum is advertised in CONNACK and bounds the client's in-flight
	// QoS 1 and QoS 2 publications. A client that has more QoS 2 PUBLISH packets
	// unacknowledged than this is sent DISCONNECT 0x93 and disconnected
	// (MQTT-5.0 §3.3.4). Defaults to DefaultReceiveMaximum.
	ReceiveMaximum uint16
	// MaximumQoS is the highest QoS the broker accepts in a PUBLISH and grants
	// on a SUBSCRIBE, advertised in CONNACK [MQTT-3.2.2-9]. Values above 2 are
	// clamped. Leave it nil for the default of 2; it is a pointer so that
	// Ptr(uint8(0)) can express a broker that only does QoS 0, which the zero
	// value would otherwise make unreachable.
	MaximumQoS *uint8
	// SharedQoS2 grants QoS 2 on a shared subscription ($share/...) when a
	// client asks for it. On by default since v2.0.0 (MIGRATION.md): a QoS 2
	// request is answered QoS 2 in the SUBACK, and the messages the member is sent
	// start the four-step exchange, where v1 answered QoS 1 [MQTT-3.8.4-7]. A
	// message a member holds is kept for it across a killed broker, a takeover
	// and a client away past the group's AckWait; use it with PersistentSessions
	// and a positive SessionCheckpointInterval, without which a killed broker's
	// held messages go back to the group (ROADMAP.md, "QoS 2 on shared
	// subscriptions", lists what is left).
	//
	// Because the default is on, false here cannot turn it off: set
	// PinSharedQoS2 to Ptr(false) to grant only QoS 1 on $share/ as v1 did.
	SharedQoS2 bool
	// PinSharedQoS2 fixes SharedQoS2 whatever the default is, and beats
	// SharedQoS2 when both are set: Ptr(false) keeps granting QoS 1 on a shared
	// subscription (the behaviour before v2.0.0), Ptr(true) grants QoS 2. nil
	// leaves the choice to the default, which is on.
	PinSharedQoS2 *bool
	// PublisherClockExpiry counts a live message's Message Expiry Interval from when the
	// publishing broker received it, not from when this broker did. The publisher stamps the
	// time (sub-second) on messages that carry an interval, so the wait spent in NATS between
	// brokers is no longer free and the remainder is not rounded up per hop. Off by default.
	//
	// It trusts the stamp, and so the clocks: a stamp in the future counts as no wait, but a
	// publisher with a clock running behind makes messages expire early, and anything able to
	// publish on the NATS subject can forge one. Enable it on every broker sharing the subjects,
	// on hosts with synchronised clocks, and where only brokers publish to them.
	PublisherClockExpiry bool
	// TopicAliasMaximum is advertised in CONNACK. Zero disables inbound topic
	// aliases; leave it unset for DefaultTopicAliasMaximum.
	TopicAliasMaximum *uint16
	// OutboundTopicAliases lets the broker send Topic Aliases to a client that
	// offered a Topic Alias Maximum in its CONNECT (MQTT-5.0 §3.3.2.3.4), up to
	// that maximum, least recently used first. It saves bandwidth on long topic
	// names that repeat. On by default since v1.0.0 (MIGRATION.md); a client
	// that offers no maximum is never sent one either way. A resend after a
	// reconnect carries the full name again, since the table belongs to the
	// network connection.
	//
	// Because the default is on, false here cannot turn it off: set
	// PinOutboundTopicAliases to Ptr(false) to send no alias.
	OutboundTopicAliases bool
	// PinOutboundTopicAliases fixes OutboundTopicAliases whatever the default
	// is, and beats OutboundTopicAliases when both are set: Ptr(false) sends
	// no alias (the behaviour before v1.0.0), Ptr(true) sends them. nil leaves
	// the choice to OutboundTopicAliases and the default, which is on.
	PinOutboundTopicAliases *bool
	// EscapeNATSWildcards lets a topic contain '*', '>', spaces and the other
	// whitespace characters, which MQTT allows but a NATS subject cannot carry,
	// so the broker refuses them by default. When set, each is stored in the
	// NATS subject as '/' and a letter ("/s" for '*', "/g" for '>', "/p" for a
	// space, "/t", "/n", "/r", "/f" and "/d" for tab, line feed, carriage
	// return, form feed and DEL). The encoder never writes '/' before a letter,
	// so the escape is unambiguous, and a topic that has none of those
	// characters maps to the same subject as before. The cost is
	// interoperability: a NATS-native client sees the escaped form, and
	// nats-server's own MQTT listener, which refuses these topics, does not
	// read it. Decide before you have data: turning it on or off later leaves
	// existing retained, queued and in-flight messages under the other mapping.
	EscapeNATSWildcards bool
	// MaximumPacketSize caps an inbound packet. Defaults to
	// DefaultMaximumPacketSize.
	MaximumPacketSize uint32
	// ServerKeepAlive, when non-zero, overrides the client's requested Keep
	// Alive and is returned in CONNACK [MQTT-3.1.2-21].
	ServerKeepAlive uint16
	// ConnectTimeout bounds the wait for a CONNECT on a new connection.
	// Defaults to DefaultConnectTimeout.
	ConnectTimeout time.Duration
	// MaxSessionExpiry caps a requested Session Expiry Interval. Defaults to
	// DefaultMaxSessionExpiry.
	MaxSessionExpiry time.Duration
	// SessionSweepInterval is how often the broker discards detached sessions
	// whose Session Expiry Interval has passed, tearing down their NATS
	// subscriptions, and retained messages whose Message Expiry Interval has
	// passed. A session therefore outlives its interval by up to this
	// long, never less [MQTT-3.1.2-23]; a client that returns after the interval
	// but before the sweep still gets Session Present 0. Defaults to
	// DefaultSessionSweepInterval.
	SessionSweepInterval time.Duration
	// SessionCheckpointInterval is the longest a broker with PersistentSessions
	// goes between writing where a connected client stands (the offline-queue
	// position its replay would start from) to the session record, while that
	// changes. It bounds what a broker killed outright costs its sessions: the
	// broker that claims the record next replays from the last position written,
	// so a client is sent again at most what it was sent in this long, instead
	// of everything the queue holds (OfflineQueueMaxAge). The cost is one
	// key-value write per interval for each connection whose position moved.
	// Defaults to DefaultSessionCheckpointInterval; a negative value turns the
	// checkpoint off, and a killed broker's sessions then replay the whole queue.
	SessionCheckpointInterval time.Duration

	// Logger receives broker events. Defaults to slog.Default().
	Logger *slog.Logger
}

// ErrInvalidOptions reports an Options value the broker cannot run with.
var ErrInvalidOptions = errors.New("natsmqtt5: invalid options")

// Ptr returns a pointer to v. The optional Options fields are pointers so that
// "not set" is distinguishable from a meaningful zero, and this saves callers
// declaring a variable to take the address of.
//
//	natsmqtt5.Options{MaximumQoS: natsmqtt5.Ptr(uint8(1))}
func Ptr[T any](v T) *T { return &v }

// resolved is Options with defaults filled in and invariants checked, so the
// rest of the broker never re-derives a default or re-validates a field.
type resolved struct {
	Options
	maxQoS            packet.QoS
	topicAliasMax     uint16
	connectTimeout    time.Duration
	maxSessionExpiry  time.Duration
	maximumPacketSize uint32
	receiveMaximum    uint16
	logger            *slog.Logger
	// defaultQueue is set when the offline queue is on only because it is
	// the default, so failing to create it is a warning, not a failure.
	defaultQueue bool
}

func (o Options) resolve() (*resolved, error) {
	r := &resolved{Options: o}

	if r.Listen == "" {
		r.Listen = DefaultListen
	}
	if r.SubjectPrefix == "" {
		r.SubjectPrefix = DefaultSubjectPrefix
	}
	if r.StreamPrefix == "" {
		r.StreamPrefix = DefaultStreamPrefix
	}
	if r.NATSURL == "" {
		r.NATSURL = nats.DefaultURL
	}
	if r.OfflineQueueMaxAge <= 0 {
		r.OfflineQueueMaxAge = 24 * time.Hour
	}
	if r.OfflineQueueRewind <= 0 {
		r.OfflineQueueRewind = 10 * time.Second
	}
	if r.OfflineQueueReplicas == 0 {
		r.OfflineQueueReplicas = 1
	}
	if r.RetainedReplicas == 0 {
		r.RetainedReplicas = 1
	}
	if r.SessionReplicas == 0 {
		r.SessionReplicas = 1
	}

	r.receiveMaximum = o.ReceiveMaximum
	if r.receiveMaximum == 0 {
		r.receiveMaximum = DefaultReceiveMaximum
	}
	r.maximumPacketSize = o.MaximumPacketSize
	if r.maximumPacketSize == 0 {
		r.maximumPacketSize = DefaultMaximumPacketSize
	}
	r.connectTimeout = o.ConnectTimeout
	if r.connectTimeout == 0 {
		r.connectTimeout = DefaultConnectTimeout
	}
	if r.SessionSweepInterval <= 0 {
		r.SessionSweepInterval = DefaultSessionSweepInterval
	}
	if r.RetainedTombstoneTTL == 0 {
		r.RetainedTombstoneTTL = DefaultRetainedTombstoneTTL
	}
	if r.SessionCheckpointInterval == 0 {
		r.SessionCheckpointInterval = DefaultSessionCheckpointInterval
	}
	r.maxSessionExpiry = o.MaxSessionExpiry
	if r.maxSessionExpiry == 0 {
		r.maxSessionExpiry = DefaultMaxSessionExpiry
	}

	r.maxQoS = packet.QoS2
	if o.MaximumQoS != nil && *o.MaximumQoS < 2 {
		r.maxQoS = packet.QoS(*o.MaximumQoS)
	}

	r.topicAliasMax = DefaultTopicAliasMaximum
	if o.TopicAliasMaximum != nil {
		r.topicAliasMax = *o.TopicAliasMaximum
	}

	r.logger = o.Logger
	if r.logger == nil {
		r.logger = slog.Default()
	}

	// v1.0.0 defaults (MIGRATION.md): on, unless pinned. The plain bools can
	// only say "on", so a pin is the one way to choose the old behaviour.
	r.RestrictDollarTopics = true
	r.OutboundTopicAliases = true
	// Durable Wills follow JetStream: on whenever something else already uses it.
	r.DurableWills = o.DurableWills || !o.DisableRetained || o.PersistentSessions || o.OfflineQueue
	if o.PinDurableWills != nil {
		r.DurableWills = *o.PinDurableWills
	}
	// v2.0.0 default (MIGRATION.md): QoS 2 on shared subscriptions, unless pinned.
	r.SharedQoS2 = true
	if o.PinSharedQoS2 != nil {
		r.SharedQoS2 = *o.PinSharedQoS2
	}
	// v2.0.0 defaults (MIGRATION.md): on, unless pinned.
	r.QueuePlainPublishes = true
	if o.PinQueuePlainPublishes != nil {
		r.QueuePlainPublishes = *o.PinQueuePlainPublishes
	}
	r.SharedQoS2 = true
	if o.PinSharedQoS2 != nil {
		r.SharedQoS2 = *o.PinSharedQoS2
	}
	if o.PinRestrictDollarTopics != nil {
		r.RestrictDollarTopics = *o.PinRestrictDollarTopics
	}
	if o.PinOutboundTopicAliases != nil {
		r.OutboundTopicAliases = *o.PinOutboundTopicAliases
	}

	// A subject prefix has to be a single well-formed NATS token, or every
	// subject derived from it is malformed.
	if strings.ContainsAny(r.SubjectPrefix, " \t\r\n*>") {
		return nil, fmt.Errorf("%w: SubjectPrefix %q contains a character that is not valid in a NATS subject",
			ErrInvalidOptions, r.SubjectPrefix)
	}
	if strings.ContainsAny(r.StreamPrefix, " \t\r\n.*>") {
		return nil, fmt.Errorf("%w: StreamPrefix %q contains a character that is not valid in a JetStream stream name",
			ErrInvalidOptions, r.StreamPrefix)
	}
	if o.OfflineQueue && o.DisableOfflineQueue {
		return nil, fmt.Errorf("%w: OfflineQueue and DisableOfflineQueue are both set", ErrInvalidOptions)
	}
	r.defaultQueue = !o.OfflineQueue && !o.DisableOfflineQueue && (!o.DisableRetained || o.PersistentSessions)
	if o.DurablePublishDedupe && !o.DurablePublish {
		return nil, fmt.Errorf("%w: DurablePublishDedupe needs DurablePublish", ErrInvalidOptions)
	}
	if o.DurablePublish {
		// The guarantee rests on the queue's stream, so the queue is required,
		// not merely defaulted: a broker that cannot set it up must not start
		// and acknowledge publishes it cannot keep.
		if o.DisableOfflineQueue {
			return nil, fmt.Errorf("%w: DurablePublish and DisableOfflineQueue are both set", ErrInvalidOptions)
		}
		r.OfflineQueue, r.defaultQueue = true, false
	}
	if o.ReconnectCatchUp && o.DisableOfflineQueue {
		return nil, fmt.Errorf("%w: ReconnectCatchUp and DisableOfflineQueue are both set", ErrInvalidOptions)
	}
	return r, nil
}

// AuthRequest describes a connection attempt for an Authenticator.
type AuthRequest struct {
	// ClientID is the Client Identifier from CONNECT, before the broker
	// assigns one for an empty value.
	ClientID string
	// Username is empty when the User Name Flag was not set.
	Username string
	// Password is nil when the Password Flag was not set. MQTT v5 permits a
	// password with no username (MQTT-5.0 §3.1.2.9).
	Password []byte
	// RemoteAddr is the client's network address.
	RemoteAddr net.Addr
	// TLS is the connection's TLS state, or nil for a plaintext connection.
	// Use it for client-certificate authentication.
	TLS *tls.ConnectionState
	// Properties are the CONNECT properties, including any User Properties.
	Properties *packet.Properties
}

// AuthResult is an Authenticator's verdict.
type AuthResult struct {
	// Identity, when non-empty, names the authenticated principal. It is
	// passed to the Authorizer and included in log records; it does not change
	// the Client Identifier.
	Identity string
	// AssignClientID, when non-empty, overrides the Client Identifier for the
	// session and is returned to the client as the Assigned Client Identifier
	// [MQTT-3.1.3-7].
	AssignClientID string
}

// Authenticator decides whether a CONNECT is accepted.
//
// Returning a nil error accepts the connection. Returning an error rejects it;
// wrap or return a *ConnectError to choose the CONNACK Reason Code, otherwise
// the broker answers 0x87 (Not authorized) and does not disclose the reason.
//
// # The Client Identifier is the session key
//
// A Session is identified by its Client Identifier alone (MQTT-5.0 §4.1), so
// every connection an implementation accepts under a given identifier inherits
// that session: its subscriptions, its unacknowledged messages and its QoS 2
// receive state. An implementation that authenticates a principal but lets it
// choose any ClientID therefore lets it read another principal's session.
//
// So an implementation must either refuse a CONNECT whose AuthRequest.ClientID
// is not one the authenticated principal owns, or ignore it and return an
// AuthResult.AssignClientID derived from the principal. Nothing downstream can
// make up for this: the Authorizer decides topics, not sessions.
//
// The broker re-runs the Authorizer over a resumed session's Topic Filters, so
// a permission narrowed between two connections takes effect on the second one
// rather than when the session expires. That bounds the damage; it does not
// replace binding the identifier, because the filters a principal is allowed
// are usually not what distinguishes it from the session's rightful owner.
type Authenticator interface {
	Authenticate(ctx context.Context, req *AuthRequest) (*AuthResult, error)
}

// AuthenticatorFunc adapts a function to the Authenticator interface.
type AuthenticatorFunc func(ctx context.Context, req *AuthRequest) (*AuthResult, error)

// Authenticate calls f.
func (f AuthenticatorFunc) Authenticate(ctx context.Context, req *AuthRequest) (*AuthResult, error) {
	return f(ctx, req)
}

// ConnectError carries the CONNACK Reason Code an Authenticator wants the
// broker to send. Any refusal code MQTT-5.0 Table 3-1 lists for a CONNACK is
// sent as given; common choices are BadUserNameOrPassword (0x86), NotAuthorized
// (0x87), Banned (0x8A) and ClientIdentifierNotValid (0x85). A code the table
// does not list, success codes included, is logged and sent as
// UnspecifiedError (0x80) [MQTT-3.2.2-8].
type ConnectError struct {
	Code   packet.ReasonCode
	Reason string
}

func (e *ConnectError) Error() string {
	if e.Reason != "" {
		return fmt.Sprintf("natsmqtt5: connection refused: %s (%s)", e.Code, e.Reason)
	}
	return fmt.Sprintf("natsmqtt5: connection refused: %s", e.Code)
}

// Action is an operation an Authorizer decides on.
type Action int

const (
	// ActionPublish is a client publishing to a Topic Name.
	ActionPublish Action = iota
	// ActionSubscribe is a client subscribing to a Topic Filter.
	ActionSubscribe
)

func (a Action) String() string {
	if a == ActionPublish {
		return "publish"
	}
	return "subscribe"
}

// AuthzRequest describes an operation for an Authorizer.
type AuthzRequest struct {
	Action Action
	// Resume distinguishes a filter being re-checked because a session is
	// being resumed from one the client has just asked for in a SUBSCRIBE. It
	// is only ever set with ActionSubscribe.
	//
	// The two are otherwise identical, and an implementation that audit-logs,
	// meters or rate-limits subscriptions needs to tell them apart: the client
	// performed no action, and the same filters come back on every reconnect.
	// It is also the cheap way out of the latency this check adds to a CONNACK
	// — an implementation may answer a resume from a cache it would not trust
	// for a fresh SUBSCRIBE.
	Resume bool
	// Will marks the Will Message of a CONNECT, asked about as an
	// ActionPublish before the connection is accepted. It is only ever set with
	// ActionPublish.
	//
	// The broker publishes a Will on the client's behalf after the client has
	// gone, so this is the only chance to refuse it: a denial refuses the
	// CONNECT with 0x87 (Not authorized), and the Will is not re-checked when it
	// fires. An implementation that audit-logs publishes can use the flag to
	// tell a Will — which may never be sent — from a PUBLISH the client made.
	Will bool
	// ClientID is the session's Client Identifier, after any assignment.
	ClientID string
	// Identity is the AuthResult.Identity from authentication, if any.
	Identity string
	// Username is the User Name from CONNECT, if any.
	Username string
	// Topic is the Topic Name for ActionPublish or the Topic Filter, shared
	// prefix included, for ActionSubscribe.
	Topic string
	// QoS is the requested QoS.
	QoS packet.QoS
	// Retain is set for a publish with the RETAIN flag.
	Retain bool
}

// Authorizer decides whether a session may publish to or subscribe to a topic.
// A nil error permits the operation.
//
// A denied publish is answered with 0x87 (Not authorized) in the PUBACK or
// PUBREC, or dropped silently at QoS 0, as MQTT-5.0 §3.3.4 requires. A denied
// SUBSCRIBE is answered with 0x87 in the SUBACK for that filter. A denied Will
// Message, asked about with AuthzRequest.Will set, refuses the CONNECT with
// 0x87 in the CONNACK.
//
// # It is also called while a session is being resumed
//
// A session is keyed by its Client Identifier alone, so the broker puts every
// filter of a resumed session back past the Authorizer before answering the
// CONNECT — once per live filter, with AuthzRequest.Resume set. That is what
// makes a permission narrowed between two connections take effect on the
// second one rather than when the session expires.
//
// Two consequences an implementation has to plan for:
//
// A slow Authorizer delays the CONNACK in proportion to how many filters the
// session holds. The ctx carries no deadline of the broker's making.
//
// A denial there is silent and it is final. There is no per-filter Reason Code
// in a CONNACK, so the filter is torn down, logged at warn level, and the
// client is told SessionPresent: true with no indication that anything is
// missing. With Options.PersistentSessions the reduced set is written straight
// back to the durable record. An error returned because the policy store could
// not be reached is therefore indistinguishable from a decision to deny, and
// costs the client its subscription permanently — an implementation that cannot
// decide should return ErrAuthorizerUnavailable, which refuses the CONNECT with
// 0x83 and leaves the session as it was, rather than any other error.
type Authorizer interface {
	Authorize(ctx context.Context, req *AuthzRequest) error
}

// ErrAuthorizerUnavailable is what an Authorizer returns (bare or wrapped, it is
// matched with errors.Is) when it could not reach its policy and so cannot say
// either way. The broker then does not treat it as a denial where a denial
// would be wrong:
//
//   - a SUBSCRIBE gets Reason Code 0x83 in its SUBACK, a PUBLISH 0x83 in its
//     PUBACK or PUBREC, and a CONNECT whose Will cannot be checked 0x83 in the
//     CONNACK, instead of 0x87 Not authorized, so the client knows to retry;
//   - [Broker.Reauthorize] leaves the subscriptions and the Will it could not
//     check as they are, and returns this error once the sweep is over;
//   - a CONNECT that resumes a session is refused with 0x83 before the session is
//     claimed or the connection holding it displaced, so the stored filters and
//     the live connection are untouched and the client can retry.
//
// The resume check runs before the takeover, over the filters the session holds
// in memory and in the store. A filter that appears in between is checked again
// at resume, and an unavailable answer there keeps it: the filter was authorised
// when it was subscribed and no CONNECT is left to refuse.
var ErrAuthorizerUnavailable = errors.New("natsmqtt5: the Authorizer could not decide")

// AuthorizerFunc adapts a function to the Authorizer interface.
type AuthorizerFunc func(ctx context.Context, req *AuthzRequest) error

// Authorize calls f.
func (f AuthorizerFunc) Authorize(ctx context.Context, req *AuthzRequest) error {
	return f(ctx, req)
}
