# Migrating from v1.2 to v1.3.0

v1.3.0 turns two opt-in options on by default. The module path is unchanged and no exported
identifier was removed, but a deployment that left both unset sees different behaviour, so read this
before upgrading. Both were opt-in since v1.2.0.

| Option | v1.2 default | v1.3.0 default | Keep the old behaviour with |
|---|---|---|---|
| QoS 2 on shared subscriptions | off (QoS 1 granted) | on | `Options.PinSharedQoS2: natsmqtt5.Ptr(false)`, `-shared-qos2=false` |
| Queueing plain NATS publishes | off | on | `Options.PinQueuePlainPublishes: natsmqtt5.Ptr(false)`, `-queue-plain-publishes=false` |

A pin beats the plain option (`SharedQoS2`, `QueuePlainPublishes`) when both are set. Because the
defaults are now on, the plain bools can no longer say "off": a Go program that left one out to mean
off must use the pin. The binary reads `NATSMQTT5_SHARED_QOS2` and `NATSMQTT5_QUEUE_PLAIN_PUBLISHES`;
a variable or flag set either way pins.

**What changes with `SharedQoS2` on.** A QoS 2 request on a `$share/` filter is granted QoS 2 in the
SUBACK, not QoS 1, and members are sent QoS 2 through the four-step exchange. A shared QoS 1 message
held for a dead session stays held until `SharedHoldLimit` instead of going back to the group after
the AckWait. With `PersistentSessions` the brokers read the session records every AckWait/3.

**What changes with `QueuePlainPublishes` on.**

- A plain NATS subscriber sees a queued message twice: the original without `Mqtt5-Msg-Id`, then the
  broker's copy with one.
- The broker liveness subject moves from `<prefix>.$broker.<id>.ping` to `<prefix>$broker.<id>.ping`.
  NATS permissions must allow the new subject.
- **Rolling upgrades.** Brokers on the two sides of this change do not see each other's liveness
  pings. With `DurableWills` or `PersistentSessions`, a v1.2 broker and a v1.3 broker sharing a
  bucket take each other's sessions and Wills for dead. Either upgrade with
  `PinQueuePlainPublishes: Ptr(false)` on every broker first and unpin after the fleet is on v1.3.0,
  or stop the old brokers before starting the new ones.
- Brokers sharing a `SubjectPrefix` must agree on the setting.

## Checklist before you upgrade

1. On v1.2, pin both options to what you run today.
2. Upgrade to v1.3.0; nothing should change.
3. Unpin one option at a time when you want the new behaviour (the whole fleet at once for
   `QueuePlainPublishes`).

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
