# Migrating

- [v1 → v2](#v1--v2)
- [v0.x → v1.0.0](#migrating-from-v0x-to-v100)

# v1 → v2

v2.0.0 changes the Go module path and flips the default of two options. No exported identifier
was removed or changed in meaning: every v1 `Options` field, flag and environment variable still
exists, and the two new `Pin...` fields are additive. A deployment that pins both options to the
v1 behaviour (below) and changes its import path behaves as v1.1.0 did, apart from the one extra
NATS subscription noted under the second change.

**v1.x stays supported.** The v1 line keeps its tags (`v1.1.0` and earlier stay fetchable at
`github.com/taumatix/natsmqtt5`) and a security fix or a regression in it is released as a v1
patch; new features land in v2 only.

## The import path gains `/v2`

Go resolves a `v2.x.y` tag only against a module path that ends in `/v2`, so the library and the
packages under it moved:

| v1 | v2 |
|---|---|
| `github.com/taumatix/natsmqtt5` | `github.com/taumatix/natsmqtt5/v2` |
| `github.com/taumatix/natsmqtt5/packet` | `github.com/taumatix/natsmqtt5/v2/packet` |
| `github.com/taumatix/natsmqtt5/topic` | `github.com/taumatix/natsmqtt5/v2/topic` |
| `github.com/taumatix/natsmqtt5/scram` | `github.com/taumatix/natsmqtt5/v2/scram` |
| `go install github.com/taumatix/natsmqtt5/cmd/natsmqtt5@v1.1.0` | `go install github.com/taumatix/natsmqtt5/v2/cmd/natsmqtt5@v2.0.0` |

The package name is still `natsmqtt5`, so no code beyond the import line changes. One command
rewrites a module (run it from its root, then `go mod tidy`):

    find . -name '*.go' -exec sed -i.bak -E 's#"github\.com/taumatix/natsmqtt5((/[a-z0-9]+)*)"#"github.com/taumatix/natsmqtt5/v2\1"#g' {} + && find . -name '*.go.bak' -delete

(`-i.bak` works on both GNU and BSD sed.)
The container image `ghcr.io/taumatix/natsmqtt5` is not a Go path and keeps its name; its `v2.x.y`
tags are the new defaults.

## QoS 2 on shared subscriptions on by default

**What changes.** A QoS 2 request on a `$share/<name>/<filter>` subscription is granted QoS 2 in the
SUBACK; v1 answered `Granted QoS 1`, which [MQTT-3.8.4-7] permits. The messages a member is sent
now start the four-step PUBLISH, PUBREC, PUBREL, PUBCOMP exchange when they were published at
QoS 2, and the member owns each message from the moment it is sent: it survives a killed broker,
a takeover and a client away past the group's AckWait (the duplicate windows that blocked the flip
are closed; what remains is the same as QoS 2 without a shared subscription, listed in the README
under "What it does not do"). With `PersistentSessions`, every broker also reads the session
records every AckWait/3 (10 s) to keep a dead broker's held messages from the group; that is a
scan of the session bucket per interval.

**Who is affected.** Deployments whose clients subscribe to `$share/` at QoS 2 (a client library
that requested QoS 2 and was quietly granted QoS 1 now sees the QoS 2 exchange and its packets),
and those with a client library that cannot handle QoS 2 deliveries. The guarantee across a
killed broker needs `PersistentSessions` and a positive `SessionCheckpointInterval`, as QoS 2
does anywhere else.

**Keep the v1 behaviour.** `Options.PinSharedQoS2: natsmqtt5.Ptr(false)`, or `-shared-qos2=false`
(`NATSMQTT5_SHARED_QOS2=false`). A pin beats the plain `SharedQoS2` bool, which can no longer say
"off".

## Plain NATS publishes with an `Mqtt5-Qos` of 1 or 2 are queued

**What changes.** A message a plain NATS client publishes straight on a live subject
(`<SubjectPrefix>.<subject>`) with an `Mqtt5-Qos` header of 1 or 2 used to reach the sessions
subscribed to it and nothing else: a session that was away never got it, and a shared group got it
only through its NATS queue group, which can pick a member whose client is away. v2 queues it. A
broker with an offline queue ignores such a message as a live copy, and one broker (a NATS queue
group on `<SubjectPrefix>.>`) handles it as `NATSIngest` handles a message: it checks the topic and
QoS as a PUBLISH's, stores a retained message, stores the queue copy and publishes the live copy
again with `Mqtt5-Msg-Id` and `Mqtt5-Queue-Seq`. MQTT sessions are delivered that second copy: an
away session is replayed it on resume, and a shared group's QoS 1 and 2 work reaches only
connected members. A message with no `Mqtt5-Qos` header, QoS 0, a subject starting with `$`, one
that already has an `Mqtt5-Msg-Id`, and every message from an MQTT client are not touched.

**Who is affected.**

- *Plain NATS subscribers* of those subjects see such a message **twice**: the original, which has
  no `Mqtt5-Msg-Id`, and the broker's copy, which has one. Filter on the header. This is the wire
  change, and it is why this waited for a major version.
- *Publishers* get no answer, as before; a message the broker refuses (invalid topic, QoS above
  `MaximumQoS`, retained messages disabled) is logged at WARN and dropped for MQTT sessions. Use
  `NATSIngest` to be told. Delivery to MQTT sessions waits for the broker's round trip, and a
  publisher's order against its own QoS 0 messages on the same subject is not kept (the QoS 1 and 2
  ones arrive later).
- *Every broker* with the option on has one more NATS subscription, on `<SubjectPrefix>.>` in a
  queue group, which sees each message under the prefix once per message in the cluster and
  discards what is not a candidate. A request to `<SubjectPrefix>.$ingest.<subject>` with
  `NATSIngest` off is no longer answered "no responders" but times out, and a JetStream publish to
  the queue stream when the stream is missing waits for its timeout instead of failing at once.
- *The broker liveness subject moved* from `<SubjectPrefix>.$broker.<id>.ping` to
  `<SubjectPrefix>$broker.<id>.ping` (for `mqtt`: `mqtt$broker.<id>.ping`), because a subscription
  on `<SubjectPrefix>.>` would otherwise turn a dead broker's immediate "no responders" into a
  timeout and slow every Will adoption and session takeover. A deployment with NATS permissions
  restricted to `<SubjectPrefix>.>` must also allow publish and subscribe on
  `<SubjectPrefix>$broker.>`.
- A fleet mixing v1 and v2 brokers on one `SubjectPrefix` delivers such a message twice to the
  sessions of a v1 broker, and **the two versions cannot see each other's liveness pings**, so
  each takes the other for dead (Wills published, sessions taken over). Do not run them side by
  side with `DurableWills` or `PersistentSessions`; stop the fleet and start it on v2.

**Keep the v1 behaviour.** `Options.PinQueuePlainPublishes: natsmqtt5.Ptr(false)`, or
`-queue-plain-publishes=false` (`NATSMQTT5_QUEUE_PLAIN_PUBLISHES=false`).

## Checklist before you upgrade

1. Change the import path with the command above and run `go mod tidy`; the build tells you what
   you missed.
2. Pin both options to the v1 behaviour (`PinSharedQoS2` and `PinQueuePlainPublishes` set to
   `Ptr(false)`, or the two flags), upgrade every broker to v2.0.0, and check nothing changed.
3. Unpin one option at a time. For `QueuePlainPublishes`, first make every plain NATS subscriber
   of the affected subjects ignore a message that has an `Mqtt5-Msg-Id` it does not want twice.

# Migrating from v0.x to v1.0.0

v1.0.0 flips the default of three options, all of which were deprecated in v0.14.0. The Go module
path is unchanged (`github.com/taumatix/natsmqtt5`, no `/v2`), and no exported identifier was
removed: the behaviour differs only for a deployment that left these options unset. A deployment
that pinned all three on v0.14.0 changes nothing.

| Option | v0.x default | v1.0.0 default | Keep the old behaviour with |
|---|---|---|---|
| Durable Wills | off | on whenever JetStream is in use | `Options.PinDurableWills: natsmqtt5.Ptr(false)`, `-durable-wills=false` |
| Restricting `$` topics | off | on | `Options.PinRestrictDollarTopics: natsmqtt5.Ptr(false)`, `-restrict-dollar-topics=false` |
| Outbound topic aliases | off | on | `Options.PinOutboundTopicAliases: natsmqtt5.Ptr(false)`, `-outbound-topic-aliases=false` |

A pin beats the plain option (`DurableWills`, `RestrictDollarTopics`, `OutboundTopicAliases`) when
both are set. Because the defaults are now on, the plain bools can no longer say "off": a Go
program that wrote `DurableWills: false` (or left it out) to mean off must use the pin. The binary
takes the same settings from `NATSMQTT5_DURABLE_WILLS`, `NATSMQTT5_RESTRICT_DOLLAR_TOPICS` and
`NATSMQTT5_OUTBOUND_TOPIC_ALIASES`; a variable or flag set either way pins, and one left out leaves
the v1.0.0 default. The INFO line v0.14.0 logged for unpinned options, and the WARN on the first `$`
topic use, are gone.

## Durable Wills on by default

**What changes.** Every connection's Will Message is also written to the `<StreamPrefix>_wills`
JetStream key-value bucket, so that the Will of a broker killed outright is published by a
surviving one. The Will's payload sits in that bucket in clear text.

**Who is affected.** Deployments that use JetStream and whose clients send Wills. They gain the bucket, one write per connection with a Will, the
liveness pings between brokers, and the need to give the bucket the same access control as the
session bucket. A broker that cannot reach NATS for about twice `WillCheckInterval` now closes its
clients (so that a survivor can publish their Wills exactly once) where before it kept serving them. Deployments with no Wills notice nothing beyond the bucket existing.

**Keep the old behaviour.** `Options.PinDurableWills: natsmqtt5.Ptr(false)`, or `-durable-wills=false`.

## Restricting `$` topics by default

**What changes.** A PUBLISH or Will on a topic starting with `$` is refused with Topic Name invalid
(0x90), and a subscription to one with Topic Filter invalid (0x8F). `$share/...` stays allowed
unless the filter inside it starts with `$`. `$retained` and `$queue` are refused today and
stay refused.

**Who is affected.** Applications that use `$app/...`, `$SYS/...` or any other `$` namespace as
ordinary topics. Their publishes and subscriptions fail after the upgrade. A subscription a
persistent session stored earlier is dropped on resume.

**Keep the old behaviour.** `Options.PinRestrictDollarTopics: natsmqtt5.Ptr(false)`, or
`-restrict-dollar-topics=false`.

## Outbound topic aliases on by default

**What changes.** A client that offers a Topic Alias Maximum in its CONNECT is sent Topic Aliases:
a topic of 8 bytes or more that repeats goes out as an alias with an empty name, up to the client's
maximum. Packets get smaller; a resend after a reconnect carries the full name.

**Who is affected.** Clients that advertise a Topic Alias Maximum but handle incoming aliases
badly (an MQTT 5 client is required to handle them, and one that does not is broken against any
other broker that sends them). Clients that offer no maximum see no change.

**Keep the old behaviour.** `Options.PinOutboundTopicAliases: natsmqtt5.Ptr(false)`, or
`-outbound-topic-aliases=false`.

## Checklist before you upgrade

1. On v0.14.0 or later, pin all three options to what you run today.
2. Upgrade to v1.0.0; nothing should change.
3. Unpin one option at a time when you want the new behaviour.

Without step 1, the pins above restore the old behaviour in one line each after the upgrade.
