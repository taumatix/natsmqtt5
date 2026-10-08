# MQTT 5 conformance

natsmqtt5 aims to be a conforming MQTT 5.0 Server. This file records where it stands against the
specification's numbered conformance statements, and is what [ROADMAP.md](ROADMAP.md) is ordered
from.

- Specification: [MQTT Version 5.0, OASIS Standard, 7 March 2019](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html)
- Reviewed: 2026-10-07, against v0.9.0. The counts are the review's; findings fixed since are
  marked below and in the data file.
- Data: [`conformance/mqtt5-statements.tsv`](conformance/mqtt5-statements.tsv), one row per
  `[MQTT-x.y.z-n]` statement. The statement texts are in the specification; the file carries the
  ids, sections and findings.

## How it was done

Every statement id in the specification was extracted (252 distinct). The 45 that bind only a
Client were set aside. Each of the other 207 was looked up in the repository and classed:

| Class | Meaning | Count |
|---|---|---|
| CITED-TESTED | cited in a test | 40 |
| CITED-CODE | cited in the implementation only | 34 |
| ROADMAP | not cited, but the gap is on the roadmap | 19 |
| UNCITED | none of the above | 114 |

Each uncited or roadmap statement then got a verdict, from reading the code and, for the doubtful
ones, a probe test against an embedded NATS server:

| Verdict (column `verdict`) | Meaning | Uncited | Roadmap |
|---|---|---|---|
| a | met, though not cited | 96 | 2 |
| b | violated | 14 | 5 |
| c | not applicable (behind a feature the broker refuses or does not advertise) | 3 | 12 |
| u | unsure | 1 | 0 |

"Met, though not cited" is a reading of the code, not a test. Turning those into cited tests is
how this table's first column grows.

## What it found

The violated statements, grouped and ordered, are the first two tiers of the roadmap. The ones a
conforming client hits in ordinary use:

- **Fixed in v0.9.1.** A client could publish to `$retained/<topic>` and replace the retained
  message for `<topic>`, bypassing the Authorizer, and read the offline queue through `$queue/#`.
  [MQTT-3.3.1-8].
- **Fixed, unreleased.** QoS 1 and 2 messages published while a session was disconnected were
  dropped unless `OfflineQueue` was on, while the CONNACK said Session Present=1. The queue is now
  on by default whenever the broker uses JetStream. [MQTT-3.1.2-23], [MQTT-4.5.0-1].
- **Fixed, unreleased.** A disconnected shared-subscription member kept receiving, and dropping,
  its share. QoS 1 and 2 work now goes only to connected members, through a backlog in the
  offline queue. [MQTT-4.5.0-1], [MQTT-4.1.0-2].
- **Fixed, unreleased.** A shared member's unacknowledged QoS 1 message died with its session.
  It now stays unacknowledged in the backlog until the PUBACK and goes to another member when the
  session ends (the §4.8.2 SHOULD, which has no statement id). [MQTT-4.8.2-6] is held by the
  same tests: a PUBACK with a Reason Code of 0x80 or more settles the message and nothing is
  handed on.
- **Fixed, unreleased.** A shared subscription whose last member left kept its backlog for up to
  `MaxSessionExpiry`, and a client that subscribed to the name afterwards was given it. The
  backlog is now deleted with the subscription (the §4.8.2 sentence "ends, and any undelivered
  messages associated with it are deleted", which has no statement id). Proven by
  `shared_ends_test.go`.
- **Fixed, unreleased.** A client that fell behind lost QoS 1 and 2 messages. With the queue
  on, it now catches up from the stream. [MQTT-4.1.0-1].
- **Fixed, unreleased.** A connection that dropped while behind lost what was waiting for it, and
  the resume replay gave up after 30 seconds. It now starts at the lowest queue sequence the
  client had not been sent and has no deadline. [MQTT-3.1.2-23], [MQTT-4.5.0-1].
- **Fixed, unreleased.** A session restored from the session store replayed from its release
  time and lost what was waiting for a client that dropped while behind, and nothing was
  replayed for a session whose broker was killed. The record now holds the replay position.
  [MQTT-3.1.2-23], [MQTT-4.5.0-1].
- **Fixed, unreleased.** A session whose broker was killed outright was resumed from the start of
  the offline queue, which repeated up to a day of messages. A connected session now checkpoints
  its replay position (`Options.SessionCheckpointInterval`, one second by default), so the
  successor replays from it. Proved with a real SIGKILL of `cmd/natsmqtt5`
  (`TestAKilledBrokersClientIsNotSentWhatItAlreadyGotAgain`,
  `TestAKilledBrokersBehindClientGetsWhatItHadNotAcknowledged`). [MQTT-3.1.2-5], [MQTT-3.1.2-23],
  [MQTT-4.5.0-1], [MQTT-4.4.0-1].
- **Fixed, unreleased.** A detached session nobody resumed kept its NATS subscriptions for the life
  of the broker. A sweep now expires it once its Session Expiry Interval (capped by
  `MaxSessionExpiry`) has passed and never before. [MQTT-3.1.2-23]
  (`sessionsweep_test.go`). A takeover during a stored-record restore no longer drops the
  stored filters (`restore_takeover_test.go`).
- **Fixed, unreleased.** Message Expiry was forwarded and never enforced. A message that has
  waited its whole interval is now deleted, and one that has not goes out with the interval less
  the whole seconds it waited, on every delivery path: live, retained, offline replay, catch-up,
  shared backlog and resends. A QoS 2 PUBLISH already sent is not expired. [MQTT-3.3.2-5],
  [MQTT-3.3.2-6], [MQTT-4.3.3-7].
- **Fixed, unreleased.** With `PersistentSessions`, a restored session resent nothing it had in
  flight and forwarded a resent QoS 2 PUBLISH twice. The record now keeps each unacknowledged
  message's identifier, QoS state and queue sequence, and the QoS 2 identifiers received and not
  released. A message with no copy in the offline queue (retained, or the queue off) travels in the record
  when its PUBLISH is 16 KiB or less (`restore_nocopy_test.go`), and a larger one is kept in a bucket of
  its own that the record names (`oversize_inflight_test.go`). A broker killed
  outright leaves the in-flight state of its last checkpoint, and a QoS 2 PUBLISH the client
  sent is recorded before its PUBREC and cleared before its PUBCOMP, proved with a real SIGKILL
  (`killed_inflight_test.go`). [MQTT-4.4.0-1], [MQTT-4.3.3-10], [MQTT-4.3.3-12].
- **Fixed, unreleased.** A QoS 2 message sent to a client is recorded before its PUBLISH and before its
  PUBREL go out, so a broker killed at any step resends the PUBLISH or the PUBREL and never delivers the
  message again as a new one (`killed_qos2_outbound_test.go`, SIGKILL). [MQTT-4.3.3-6], [MQTT-4.4.0-1].
- **Fixed, unreleased.** A message whose live copy reaches the broker after the subscriber's connection
  ended is still sent on resume, however long the copy was delayed, on the same broker and on a broker
  that restores the session from its record (`late_live_copy_delay_test.go`). Not covered: a copy that
  arrives after another broker claimed the record, or after 8192 further queue sequences were delivered.
  [MQTT-4.4.0-1].
- **Fixed, unreleased.** A retained message could carry its publisher's Topic Alias to a subscriber
  on the same broker, an alias that means nothing on its connection. [MQTT-3.1.2-26],
  [MQTT-3.1.2-27], [MQTT-3.3.2-11].
- **Fixed, unreleased.** A SUBSCRIBE with No Local on a `$share/` filter was answered with 0x82
  inside the SUBACK, which is not a Subscribe Reason Code. It is now the Protocol Error the
  specification calls it: a DISCONNECT with 0x82 and the connection closed. [MQTT-3.8.3-4],
  [MQTT-3.9.3-2], [MQTT-4.13.1-1].
- **Fixed, unreleased.** A PUBLISH from a client carrying a Subscription Identifier was accepted,
  acknowledged and forwarded. It is a Protocol Error: DISCONNECT 0x82 and the connection closed,
  nothing acknowledged or forwarded. [MQTT-3.3.4-6], [MQTT-4.13.1-1].
- **Fixed, unreleased.** A Response Topic with a wildcard was forwarded to subscribers, in a
  PUBLISH and in a Will. It is a Protocol Error (data the protocol does not allow, in a packet that
  parses): DISCONNECT 0x82 and the connection closed, or CONNACK 0x82 for a Will. [MQTT-3.3.2-14],
  [MQTT-4.13.1-1].
- **Fixed, unreleased.** A CONNACK, PUBACK, PUBREC or DISCONNECT over the client's Maximum Packet
  Size was discarded whole. The Reason String and User Properties are now dropped first and the
  packet sent. [MQTT-3.2.2-19], [MQTT-3.2.2-20], [MQTT-3.4.2-2], [MQTT-3.5.2-2], [MQTT-3.14.2-3].
- **Fixed, unreleased.** A PUBLISH could be written after the broker's DISCONNECT, a race between
  the delivery goroutine and the read loop. [MQTT-3.14.4-1].
- **Added, unreleased.** The Will Message of a broker killed outright was never published. With
  `DurableWills` the Will is stored in JetStream and a surviving broker publishes it, once, after
  the Will Delay Interval. The repository had the ids of [MQTT-3.1.2-7] to [MQTT-3.1.2-10] one
  sentence out of place (the id follows the sentence it numbers); the data file now has -7 as
  "stored", -8 as "published after the connection closes, unless deleted or a new connection
  opens", -9 as the client's and -10 as "removed once published or on DISCONNECT 0x00".
  [MQTT-3.1.2-7], [MQTT-3.1.2-8], [MQTT-3.1.2-10], [MQTT-3.1.3-9].
- **Fixed, unreleased.** A Reason Code returned by an `Authenticator` was sent in the CONNACK
  whatever it was, so a success or a DISCONNECT-only code could reach the wire. A code Table 3-1
  does not list for a CONNACK is now logged and sent as 0x80. [MQTT-3.2.2-8].
- **Deviation from a SHOULD, by default.** MQTT 5.0 §4.7.2: "The Server SHOULD prevent Clients from
  using such Topic Names to exchange messages with other Clients" (no statement id). The broker
  refuses only the two levels its own data lives under (`$retained`, `$queue`) unless
  `RestrictDollarTopics` is set, because refusing every `$` topic would break applications using
  `$app/...` today. With the option, a PUBLISH or Will on a `$` Topic Name is refused with 0x90
  and a subscription to a `$` Topic Filter with 0x8F; `$share/...` is allowed unless the filter
  inside it starts with `$`. A subscription a persistent session stored before the option was
  turned on is resumed. Proven by `dollar_topics_test.go`.

Features the specification defines and the broker does not offer (enhanced AUTH, server-to-client
topic aliases, WebSocket, QoS 2 on shared subscriptions) are refused or left unadvertised the way
the specification allows, and are the fourth tier.

## Keeping it current

Re-run the review when the code or the specification has moved a lot. When a fix lands, cite the
statement it addresses in the test that proves it, and update its row here.
