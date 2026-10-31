# Migrating to the next major version

**Draft for the next major version.** Nothing here has happened yet: v0.x keeps every default
described below, and this file says what will change so that you can decide now. It becomes the
real migration guide when the major version is released, and the deprecation notes in
`options.go`, the README and the CHANGELOG are deleted in the same release.

The next major version flips the default of three options. Each can be pinned today, in any
v0.14.0 or later release, with a `Pin…` option (or its flag), so a deployment can choose the old
or the new behaviour before the flip and need not change anything on the day.

| Option | Default today | Default in the next major | Pin it with |
|---|---|---|---|
| Durable Wills | off | on whenever JetStream is in use | `Options.PinDurableWills`, `-durable-wills[=false]` |
| Restricting `$` topics | off | on | `Options.PinRestrictDollarTopics`, `-restrict-dollar-topics[=false]` |
| Outbound topic aliases | off | on | `Options.PinOutboundTopicAliases`, `-outbound-topic-aliases[=false]` |

A pin beats the plain option (`DurableWills`, `RestrictDollarTopics`, `OutboundTopicAliases`) when
both are set. The binary takes the same settings from `NATSMQTT5_DURABLE_WILLS`,
`NATSMQTT5_RESTRICT_DOLLAR_TOPICS` and `NATSMQTT5_OUTBOUND_TOPIC_ALIASES`; a variable set to
`false` pins as well. While none of a deployment's flipping options is pinned or on, each process
logs one INFO line naming the unpinned ones; pinning them silences it.

## Durable Wills on by default

**What changes.** Every connection's Will Message is also written to the `<StreamPrefix>_wills`
JetStream key-value bucket, so that the Will of a broker killed outright is published by a
surviving one. The Will's payload sits in that bucket in clear text.

**Who is affected.** Deployments that use JetStream and whose clients send Wills. They gain the bucket, one write per connection with a Will, the
liveness pings between brokers, and the need to give the bucket the same access control as the
session bucket. Deployments with no Wills notice nothing beyond the bucket existing.

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

1. Pin all three options to what you run today, on the version you are on now (v0.14.0 or later).
2. Upgrade to the next major version; nothing should change.
3. Unpin one option at a time when you want the new behaviour.
