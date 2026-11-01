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
