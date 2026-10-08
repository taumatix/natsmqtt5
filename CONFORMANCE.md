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
- **Fixed, unreleased.** A client that fell behind lost QoS 1 and 2 messages. With the queue
  on, it now catches up from the stream. [MQTT-4.1.0-1].
- **Fixed, unreleased.** A connection that dropped while behind lost what was waiting for it, and
  the resume replay gave up after 30 seconds. It now starts at the lowest queue sequence the
  client had not been sent and has no deadline. [MQTT-3.1.2-23], [MQTT-4.5.0-1].
- **Fixed, unreleased.** A session restored from the session store replayed from its release
  time and lost what was waiting for a client that dropped while behind, and nothing was
  replayed for a session whose broker was killed. The record now holds the replay position.
  [MQTT-3.1.2-23], [MQTT-4.5.0-1].
- **Fixed, unreleased.** Message Expiry was forwarded and never enforced. A message that has
  waited its whole interval is now deleted, and one that has not goes out with the interval less
  the whole seconds it waited, on every delivery path: live, retained, offline replay, catch-up,
  shared backlog and resends. A QoS 2 PUBLISH already sent is not expired. [MQTT-3.3.2-5],
  [MQTT-3.3.2-6], [MQTT-4.3.3-7].
- With `PersistentSessions`, a restored session resends nothing it had in flight.
  [MQTT-4.4.0-1], [MQTT-4.3.3-10].
- A retained message can carry its publisher's Topic Alias to another client. [MQTT-3.3.2-11].

Features the specification defines and the broker does not offer (enhanced AUTH, server-to-client
topic aliases, WebSocket, QoS 2 on shared subscriptions) are refused or left unadvertised the way
the specification allows, and are the fourth tier.

## Keeping it current

Re-run the review when the code or the specification has moved a lot. When a fix lands, cite the
statement it addresses in the test that proves it, and update its row here.
