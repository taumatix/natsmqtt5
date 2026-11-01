# Changelog

What changed for someone depending on this broker. The format is
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[semantic versioning](https://semver.org/spec/v2.0.0.html) — the public Go API
is additive within a major version.

This file starts at v0.3.0. The three releases before it are summarised from
their merged pull requests, which is less than they deserve and all that can
honestly be reconstructed now.

## [Unreleased]

- **Changed:** outbound Topic Aliases, once a client's table is full, give an alias to a new name only on its second sighting (a bounded set of 32 names seen once), so a topic sent once no longer evicts a hot one. On the skewed mix at a table of 8 the bytes saved rise from 19% to 32%, a round robin over 40 names at a table of 8 goes from 0% to 6%, and no measured mix or table sends more bytes. Aliases are still per connection, never above the client's maximum, and a free alias is still taken on first sight. No new option.

- **Fixed:** outbound Topic Aliases (on by default since v1.0.0) no longer send more bytes than no aliases to a client whose Topic Alias Maximum is smaller than the set of topics it receives. Each new name used to take the least recently used alias, costing 3 bytes and saving nothing when no name returned before the next displaced it (up to 6% more bytes than aliases off; a maximum of 1 always lost). The connection's table now remaps an alias only while the aliases already set have saved at least what they cost. Aliases are still per connection, never above the client's maximum, and a table of 8 or more sends the same bytes as before. No new option.

- **Fixed:** a data race between a live copy of a queued message arriving and the session dropping its replay cursor (found under `-race` by a test of the cursor's id bound). The live copy now reads the cursor under the session's lock; a data race is undefined behaviour in Go, though the cursor's rules already made the outcome the same.

## [1.0.0] - 2026-11-01

**Breaking release.** The first major version: V1 scope is every MUST addressed, and the three defaults deprecated in v0.14.0 flip. The Go module path stays `github.com/taumatix/natsmqtt5` (no `/v2`). Read [MIGRATION.md](MIGRATION.md) before upgrading; pinning the three options to what you run on v0.14.0 makes the upgrade a no-op.

Install: `go get github.com/taumatix/natsmqtt5@v1.0.0`.

- **Changed (breaking):** `Options.DurableWills` is on by default whenever JetStream is in use (retained messages, `PersistentSessions` or `OfflineQueue`): every connection's Will is also written to the `<StreamPrefix>_wills` bucket in clear text, so that bucket needs the access control of the session bucket. Keep the old behaviour with `PinDurableWills: natsmqtt5.Ptr(false)` or `-durable-wills=false`.
- **Changed (breaking):** `Options.RestrictDollarTopics` is on by default: a PUBLISH or Will on any `$` topic is refused with 0x90 and a subscription to one with 0x8F (`$share/...` stays allowed). Keep the old behaviour with `PinRestrictDollarTopics: natsmqtt5.Ptr(false)` or `-restrict-dollar-topics=false`.
- **Changed (breaking):** `Options.OutboundTopicAliases` is on by default: a client that offers a Topic Alias Maximum is sent Topic Aliases. Keep the old behaviour with `PinOutboundTopicAliases: natsmqtt5.Ptr(false)` or `-outbound-topic-aliases=false`.
- **Changed:** because those defaults are on, the plain `DurableWills`, `RestrictDollarTopics` and `OutboundTopicAliases` can no longer say "off"; a `*bool` pin is the only way to turn one off, and it beats the plain bool. The three flags of the binary now default to `true` and pin only when given.
- **Removed:** the once-per-process INFO line naming unpinned options (it announced the flip, which has happened) and the once-per-broker WARN on the first `$` topic use.

- **Tests:** the 31 statements that bind only a Client and that no test cited are now tested from the Server's side, over a real socket with hand-written bytes: a Client that breaks one gets the CONNACK or DISCONNECT that [MQTT-4.13.1-1] and §3.1.4 require, and the connection closes. They cover the first packet not being a CONNECT, Will QoS and Will Retain with the Will Flag 0, a User Name or Password the flags do not announce (or announce and omit), a CONNECT payload out of order or without a Client Identifier, strings that are not UTF-8 in the Client Identifier, Will Topic, User Name, SUBSCRIBE and UNSUBSCRIBE, a Packet Identifier of 0, a Topic Alias above the maximum or with the maximum 0, a PUBLISH above the Maximum QoS or with RETAIN against Retain Available 0 or above the Maximum Packet Size, a packet other than AUTH before the CONNACK, an AUTH with the wrong Reason Code, without a method, or re-authenticating with another method, and a silent Client. The broker already refused every one: no product behaviour changed. `CONFORMANCE.md` now counts 240 TESTED and 8 CLIENT-ONLY (each with its reason), not 209 and 39.

- **Fixed:** a broker with no offline queue (no JetStream, or `DisableOfflineQueue`) dropped the QoS 1 and 2 messages published while a persistent session was away although its CONNACK said Session Present 1 [MQTT-3.1.2-23], [MQTT-4.5.0-1]. It now holds them in memory, per session, in order, and delivers them on resume ahead of anything newer; a session that overflows the bound is ended and its client is told Session Present 0 [MQTT-3.2.2-3], so a client told 1 has been sent everything. A restart still loses the memory, and says so with Session Present 0.
- **Added:** `Options.AwayBufferMessages` (`-away-buffer-messages`, default 1000 per session, negative holds none, at most 2048). With `CloseBehindClients`, a client whose connection was closed and whose backlog exceeds the bound now gets Session Present 0 on reconnect, not 1. Additive.

## [0.14.0] - 2026-11-01

Fixes to resume, retained delivery and offline-queue reads in clustered deployments (the clustered resume that got nothing, a retained message delivered twice on SUBSCRIBE, a queue read ended early by a lagging replica), delivered-id spill chunks, new opt-in options (all off by default), and a **deprecation notice**: the defaults of `DurableWills`, `RestrictDollarTopics` and `OutboundTopicAliases` are planned to flip to on in v1.0.0. v0.14.0 changes none of them; pin them now with the new `Pin*` options (see [MIGRATION.md](MIGRATION.md), a draft for the next major). No breaking change.

Install: `go get github.com/taumatix/natsmqtt5@v0.14.0`.

- **Deprecated:** relying on the implicit default of three options. `DurableWills`, `RestrictDollarTopics` and `OutboundTopicAliases` are off today and their default becomes on in the next major version; v0.x changes nothing. [MIGRATION.md](MIGRATION.md) is a draft of what each flip changes, who is affected and how to keep the old behaviour. Pin the choice now with the new options below. A broker with any of the three neither pinned nor on logs one INFO line per process naming the unpinned ones.
- **Added:** `Options.PinDurableWills`, `Options.PinRestrictDollarTopics` and `Options.PinOutboundTopicAliases` (`*bool`, nil by default). A non-nil value fixes the behaviour either way and beats the plain option; `Ptr(false)` is the one-line way to keep today's behaviour across the flip. Defaults are unchanged.
- **Added:** `-outbound-topic-aliases` for the binary (the option had no flag). A flag or environment variable given as `false` now pins its option, for `-durable-wills`, `-restrict-dollar-topics` and `-outbound-topic-aliases`; left out, it pins nothing. Environment variables are now applied through the flag set, so they count as given.

- **Fixed:** an offline-queue read (a resume, or the catch-up of `ReconnectCatchUp`) ended at once when its consumer's NATS server was a replica of the queue stream still catching up after a restart, because that replica reported nothing pending short of the stream's end. The session was sent none of what it was owed, and the cursor moved past it. The read now asks the stream's leader whether anything it matches lies between the consumer's position and the end, and keeps reading if so.
- **Fixed:** with `DurableWills`, a broker could close its clients while NATS elected a new leader for the Will bucket at the default `WillCheckInterval`. The fence counted its twice-the-interval from the last successful check, up to half an interval before the server failed, which left an election that takes up to 9 s only 7.5 s. It now counts from the failure, so the survivors' wait for a silent owner is about 5 intervals instead of 4.5 and a Will of a broker that really died is published 2.5 s later at the default.

- **Tests:** `Broker.Close` is shown to return with NATS cut and a catch-up read held (a `Close` hang reported by #216 was looked for and not reproduced; see ROADMAP), and `retryAfterDrop` is covered end to end: a catch-up read cut by a reconnect is started again and the client gets every message once. No API change.

- **Fixed:** a SUBSCRIBE read the retained messages it owed after writing its SUBACK, so a publisher that stored a retained message in between had it delivered twice, the second time with RETAIN set [MQTT-3.3.1-6], [MQTT-3.3.1-12]. The set is now read before the SUBACK.
- **Fixed:** an adoption of a Will Message whose update got no answer from the store (an election, a timeout) but was applied left a record owned by the adopter and published by nobody; the adopter now looks again and publishes it. A failed adoption is logged.
- **Tests:** end-to-end tests where only unit tests stood: `NATSIngest` with the queue stream gone and with `DurablePublish`'s flush unconfirmed, the delivered-id chunks when the payload bucket is gone or a chunk is purged, and a `ReconnectCatchUp` read held by a slow client across a second NATS drop. No API change.
- **Added:** `Options.RetainedVerifyInterval` (`-retained-verify-interval`, off by default): the sweep checks every entry of the retained view against the stream at least that often, one lookup per entry. It finds a stale entry that a NATS partition shorter than the client's ping interval left behind, which triggers no reconnect check and leaves the counts equal [MQTT-3.3.1-6]. Additive: zero keeps the previous behaviour.
- **Added:** `Options.DurablePublishDedupe` (`-durable-publish-dedupe`, off by default, needs `DurablePublish`): the retry of a QoS 1 or 2 publish refused with 0x83 after its queue copy was stored no longer leaves a second copy in the queue. The broker keeps the message id of a refused publish per session and a retransmission of it (same packet identifier, DUP set, same topic and payload) carries the same `Nats-Msg-Id`, so JetStream drops it inside the stream's duplicate window. An acknowledged publish is forgotten, so a later message reusing the packet identifier is never dropped. Additive.
- **Added:** `Options.PublisherClockExpiry` (`-publisher-clock-expiry`, off by default) counts a live message's Message Expiry Interval from when the publishing broker received it, not from when the receiving broker did, so time spent in NATS between brokers is no longer free [MQTT-3.3.2-5], [MQTT-3.3.2-6]. The publisher stamps a `Mqtt5-Published-At` header on a PUBLISH that carries an interval; a stamp later than the receiver's clock counts as no wait. It trusts the clocks and anyone able to publish on the subjects: a publisher running behind, or a forged stamp, makes a message expire early. Additive: off, nothing is stamped or read.
- **Added:** `Options.CloseBehindClients` (`-close-behind-clients`, off by default): with no offline queue, a client that is 2048 messages behind and would lose a QoS 1 or 2 message is disconnected with 0x97 (Quota exceeded) instead of the message being dropped and only logged. Its session is kept to resume. QoS 0 loss and a broker with the queue on are unaffected. Additive.
- **Tests:** a cluster test SIGKILLs the server that leads the Will bucket (`TestClusterAKilledWillBucketLeaderClosesNoClientAtTheDefaultCheckInterval`) and holds that, at the default `WillCheckInterval`, neither broker closes a client nor is a Will published while NATS elects another leader (3 runs of 3 passed), so the default does not need to rise. `TestSpillAcknowledgementCost` measures what an acknowledgement rewrites of a spilled in-flight window: one chunk (132 KB) beside an 824 KB record. No API change.
- **Changed:** a checkpoint of a full in-flight window (65535 spilled entries) costs about 75 ms of CPU, not 250 ms: `spillRecordReusing` sizes the record from the entry sizes (`recordSizer`, held equal to `encodeRecord` by test) instead of encoding it at each step of the search, and builds each chunk from the entry encodings it already has. The record and chunk bytes, and so the chunk keys, are unchanged.
- **Added:** `Options.NATSIngest` (`-nats-ingest`, `NATSMQTT5_NATS_INGEST`), off by default. A plain NATS client publishing on `<prefix>.$ingest.<subject>` has its message handled by one broker as an MQTT PUBLISH on `<subject>`: topic and QoS checked, retained stored, a QoS 1 or 2 message queued, the live copy published with the broker's id and queue sequence, and a request answered (`Mqtt5-Msg-Id`, `Mqtt5-Queue-Seq`, or `Mqtt5-Ingest-Error`). Before, a message published straight onto NATS was never queued: a session that was away missed it, and a shared group could hand it to a member whose client was away, where it was dropped. Publishing on the live subject is unchanged. The `Authorizer` is not asked; NATS permissions on the ingest subject decide who may use it. Additive.

- **Added:** `Options.ReconnectCatchUp` (`-reconnect-catch-up`, `NATSMQTT5_RECONNECT_CATCH_UP`), off by default. A broker whose NATS connection dropped missed every live copy published until it reconnected, and its connected sessions never got them (only a session that was away is replayed). With the option, the drop holds the sessions' live QoS 1 and 2 copies back and the reconnect has each connected session read the offline queue from just before the drop: what it was not sent goes out first, in order, once, and only what was stored after the subscription was made. Covers messages published through a broker; a copy lost between two servers of a cluster while both brokers stay connected is not seen (`ROADMAP.md`). Additive: with the option off nothing changes. An error together with `DisableOfflineQueue`.

- **Fixed:** a session record that could not carry every delivered id above the replay cursor (more than 4096) was written without the cursor, so a broker that restored it replayed from the time rewind and lost a message older than the rewind whose live copy never came [MQTT-4.4.0-1]. The ids past 4096 (up to 65536 more) are now kept in chunks in the payload bucket beside the record, written only when they change, deleted with the session, and read back on resume; the record keeps its cursor. Additive: a record with 4096 ids or fewer is written as before, one with chunks adds a `DeliveredSpill` key that an older broker ignores, and an old record restores unchanged. A chunk that cannot be written or read drops the cursor, as before.
- **Tests:** two windows the roadmap listed against QoS 2 on shared subscriptions (`Options.SharedQoS2`, still off by default) are driven over real sockets: a member's broker killed the moment its client has the PUBLISH, and killed after reading the PUBREC and before recording the PUBREL. In both the client, back on another broker within the group's AckWait, completes the exchange on its original Packet Identifier and the group's other member is never given the message [MQTT-4.8.2-4], [MQTT-4.8.2-5]. A client away longer than the AckWait after its broker was killed still has its message given to another member as well; `ROADMAP.md` records it. No API change.
- **Fixed:** a delivery noted while a scan of the replay cursor was running started no scan, and the running one ended without scanning again when it stopped at the connection's lowest unsent message, found the cursor changed, or could not read the queue. If no other delivery followed, the cursor stayed behind until the next one, and a resume read the queue from there; nothing was lost or sent twice, since a replay reads from the cursor. A delivery noted during a scan now makes it run once more. Also in the tests: `TestALiveCopyBelowARestartedCursorsBaseDropsIt` waited for a dropped cursor that the delivery of the same message could restart before the test looked (macOS CI). No API change.
- **Fixed:** on a clustered offline queue, a resume could send nothing it was owed and log "replaying the offline queue failed; what it held is lost for this session" with `nats: no responders available for request`. nats.go makes a new consumer for every fetch of a replay, the cluster can place it on another NATS server, and a pull that reached the broker's server before the consumer's interest had was answered 503. The read now fetches again, as it does after an empty fetch, instead of failing. Before, the session got nothing until its next resume (CI's cluster job: about one resume in a few hundred). The broker also logs each resume's queue read at debug.

## [0.13.0] - 2026-10-31

**[urgent]** v0.12.0 and v0.12.1 crash with a nil pointer dereference at the first session sweep of a broker started with `DisableRetained` (`-no-retained`); upgrade if you use that option. Everything else below is additive: new API (`Options.Auther`, package `scram`, `Options.SharedQoS2`, WebSocket and TLS flags) and fixes, no breaking change.

Install: `go get github.com/taumatix/natsmqtt5@v0.13.0`.
- **Fixed:** a connected session whose replay cursor was dropped (more than 1024 holes, a record written before the cursor, a SUBSCRIBE that could not read the queue's position) gets it back without a resume: after 1024 deliveries a restart scan reads the session's subjects from one `OfflineQueueRewind` ago and starts the cursor there, with the messages it was owed and never sent as holes. Before, it fell back to the time rewind until its next resume, so a message whose live copy never came was lost once older than the rewind [MQTT-4.4.0-1]. The restart never promises less than the rewind: it is refused while the connection holds a message below where it read, and a live copy below that drops the cursor again. A restarted cursor is kept in memory, not in the session record. A scan of the cursor now throws its result away when a subscription was made while it read, and scans again when deliveries piled up meanwhile. No API change.
- **Fixed:** a replay of the offline queue that got an empty fetch took it for the end of the stream. nats.go makes an ordered consumer afresh for every fetch, and on a loaded server a fetch can come back empty although matching messages are stored, so a resume could skip the message a session was owed and move its replay cursor past it [MQTT-4.4.0-1]. The read now ends only when the consumer reports nothing pending, and fails after 20 empty fetches in a row with messages still pending.
- **Fixed:** a move of the replay cursor, or a new hole, did not count as activity for the session checkpoint. If nothing else happened afterwards, the stored record never got the hole, and a broker killed then left a record that did not name a message the session was owed [MQTT-4.4.0-1].
- **Tests:** the expiry tests in `spec_integration_test.go` wait for the session sweep to expire the session instead of sleeping 1.5 s past a one-second Session Expiry Interval.
- **Changed:** the session store sweep deletes a payload in the in-flight payload bucket that belongs to a session attached to the sweeping broker and that neither the session's record nor the broker's memory names, once it is older than the grace period. Before, such payloads stayed until the bucket's TTL. No API change.
- **Changed:** a resume reads each hole of its replay cursor (a message it was owed and never sent) by its sequence, and the queue stream from the cursor, instead of reading the stream from the lowest hole on: a hole kept for hours no longer costs every message stored above it on each resume. Measured on a three-way replicated stream (`TestClusterReplayReadCost`): with a hole below 2000 completed messages, the owed message above them arrives in 77 ms instead of 112 ms. No API change.
- **Fixed:** a broker started with `DisableRetained` (`-no-retained`) crashed with a nil pointer dereference at its first session sweep, one `SessionSweepInterval` (a minute by default) after it started. Introduced in v0.12.0.
- **Fixed:** after its NATS connection reconnects, a broker checks its retained view against the stream at once, not at the next session sweep (up to `SessionSweepInterval`, a minute by default, later), so a retained message another broker cleared while this one was cut off stops being sent to new subscribers within moments [MQTT-3.3.1-6]. The broker listens for the reconnect without replacing a reconnect handler set on `Options.Conn`. No API change.
- **Fixed:** when a broker whose default offline queue came up late gives a shared subscription the group's backlog, it skips the live copy of every message the backlog holds, judged by the group consumer's acknowledgement floor rather than the queue's last sequence. When members on another broker had already made the consumer, a message stored after that and before the backlog was given, whose live copy arrived afterwards, reached the group twice [MQTT-4.3.3-2]. No API change.
- **Fixed:** a shared subscription made while the default offline queue was off (it failed at start-up and came up later through the retry) is now given its membership entry and its group's backlog when the queue comes up, as a SUBSCRIBE made then would be. Before, it stayed a plain queue-group member until its session subscribed again, so a message the group sent it while it was away was lost [MQTT-4.5.0-1], and the last-member rule did not count it (MQTT-5.0 §4.8.2). The live copy of a message queued before the backlog was given is still delivered live; `ROADMAP.md` records the duplicate that leaves when another broker's members already had the backlog.
- **Fixed:** the fence probe added for the cut-off broker waits at least one second for its write to be acknowledged, instead of one `WillCheckInterval`. Below a second, a write slowed by a loaded server counted as unanswered and could make a healthy broker close its clients; a survivor now waits correspondingly longer (about two seconds more) before adopting a silent broker's Wills. Intervals of a second or more, the default 5 s included, are unchanged.
- **Tests:** `TestClusterAConnectionWithManyUnacknowledgedMessagesStillEndsWhenItsNATSServerFreezes` now stores its in-flight payloads one by one (no offline queue, PUBLISHes over the 16 KiB the record keeps inline, checkpoint off), so it tells the shared deadline from one per payload: the connection ends 5 s after its NATS server freezes, and 60 s with the deadline of before #170. It used to pass on both, since with the queue nothing was stored.
- **Fixed:** with `DurableWills`, a broker whose NATS server was cut off from the rest of its cluster never closed its clients, so a surviving broker published a Will while its client was still connected [MQTT-3.1.2-8]. The broker proved it could reach the cluster by reading the Will bucket, and every replica answers a read, the cut-off one included; it now writes a probe key (`fence-probe`) that only a majority can accept. The same read also failed now and then on brokers of the majority (a read can be routed to the cut-off replica), which made a survivor close its own clients, the Will's watchers among them. The probe costs one replicated write per broker every half `WillCheckInterval`; on a cluster keep twice that interval above a NATS leader election (4 to 9 s), or every broker closes its clients when the Will bucket's leader is lost. The test helper that saw a client's socket close mistook its read deadline for the close, so the cluster test passed without the fence; it now clears it.
- **Changed:** resuming a session whose subscriptions are all shared no longer reads the offline queue stream (a shared message comes from its group's backlog, never from the replay), so such a session costs nothing in proportion to the other sessions' traffic. No API change.
- **Changed:** spilled in-flight entries are cut into chunks at fixed identifier boundaries (1024 identifiers, or the payload size if that is smaller), so acknowledging the oldest message rewrites the one chunk it was in at a checkpoint instead of all of them. The stored layout is still an ordered list of chunk keys: a broker of an earlier version reads it as before. No API change.
- **Fixed:** with `DurablePublish` on, a Will whose queue copy the stream did not store (no answer within five seconds) is tried up to three times before it is published live, so a session that was away is not left without it [MQTT-3.1.2-8]. A try that timed out may have stored its copy, so a Will can be queued twice, which QoS 1 allows. No API change.
- **Changed:** CI runs `golangci-lint` (errcheck, gosec); its findings are fixed. `main` now logs a failing `Broker.Close`, and the broker logs a failing NATS drain at debug, instead of dropping them. No API change.
- **Fixed:** a Will that was being handed over for publication could be missed by `Broker.Reauthorize` (or a takeover) running at that moment, so a Will the new identity may not publish was published anyway. The Will now moves from the session to the pending slot in one step, so it is always found and can be discarded. **Tests:** the slowest tests run in parallel (the suite takes about six minutes, not ten), and the flakes of the durable-publish cut and the cluster Will test are made deterministic.
- **Fixed:** a queued QoS 1 or 2 message whose live copy never reached a session (its publisher stopped between storing the queue copy and publishing the live one), or reached it after the session was released and more than 8192 sequences below what it had since been sent, was lost once it was older than `OfflineQueueRewind`. Each session now keeps a replay cursor, moved by reading the queue stream without delivering, and a resume replays from it: such a message is sent however long ago it was stored, on the same broker or one that restores the session, and nothing the client already received is sent again [MQTT-4.4.0-1], [MQTT-4.3.3-2]. The session record gains `ReplayCursor`, `ReplayHoles` and a subscription's `SinceSeq`, all omitted when empty: a record without them is replayed from the time rewind as before, and a broker that predates them ignores them. A SUBSCRIBE with the offline queue on costs one more JetStream request, waited for at most 250 ms and never refused for it. `OfflineQueueRewind` is now only the start for a session without a cursor.
- **Changed:** with `SharedQoS2` on, a shared member's QoS 2 message is acknowledged to its group once the record naming the PUBREL is written (at PUBREC) and not at PUBCOMP, so ending the session, expiring it or killing the broker after the PUBREC no longer lets the group give the message to another member. A broker restoring a record that names the message acknowledges it.
- **Fixed:** when another broker claimed a persistent session, the broker that lost it handed the shared-subscription messages the session held back to their groups, so a group's other member was sent a message the session's client was about to be resent. It no longer does: the claiming broker holds them from the stored record. **Added:** `Options.SharedQoS2` and `-shared-qos2`, off by default, grant QoS 2 on `$share/` filters (the SUBACK still grants QoS 1 without them); the held message survives a killed broker and a takeover. The guarantee is not complete yet (ROADMAP.md), so leave it off in production.
- **Fixed:** after its NATS connection reconnects, a broker checks every entry of its retained view against the stream at the next sweep, so a stale retained message that a count could not reveal (one the stream cleared, or replaced through another broker, while the broker was cut off) is dropped or replaced instead of being served until the consumer catches up. One lookup per retained topic per reconnect.
- **Changed:** the shared-subscription membership sweep also removes the entry of a released session whose record no longer lists the group (a failed restore that kept the record), not only of one with no record. A record a broker is serving is never judged by this. **Fixed:** a shared-subscription puller whose connection had ended while its fetch was running could re-create the group and write the member entry of a session that is away; it no longer does.
- **Tests:** the duplicate a `DurablePublish` retry leaves in the queue stream after a refused flush is driven over a socket, and the decision not to dedupe it with a client-chosen `Nats-Msg-Id` is recorded in `ROADMAP.md`.
- **Tests:** a broker whose default queue came up late after a failed start now has its shared hold-back and membership refresh driven by tests, and the decision to keep Session Present 1 while the queue is off is recorded in `ROADMAP.md`; a shared subscription made before the queue came up is recorded there as not covered by either.
- **Documented:** that without the offline queue a client that falls behind its publishers loses QoS 1 and 2 past 2048 waiting messages, why holding the publishers back through the NATS subscription is not offered, and what to run instead.
- **Fixed:** a broker without `EscapeNATSWildcards` read a subject stored under escaping (retained, queued) back as a mangled topic (`ret/a*b` as `ret/asb`). `topic.SubjectToName` now decodes the escape sequences too; the plain mapping never writes them, so every subject it produces decodes as before. Switching the option off no longer renames what was stored; it cannot be reached by a client that must publish or subscribe by that name.
- **Added:** package `scram`, a SCRAM-SHA-256 (RFC 5802, RFC 7677) `Auther` over a caller-supplied credential lookup (`scram.Auther`, `scram.Credential`, `scram.NewCredential`); it matches the RFC 7677 example byte for byte, answers an unknown user as it does a wrong password, and has no channel binding.
- **Fixed:** a connection holding many in-flight payloads no longer waits one 5 s deadline for each when its NATS server stops answering: storing, reaping and spilling them share one deadline, so the connection's end (and a resume waiting on it) is bounded.
- **Added:** enhanced authentication. `Options.Auther` (`Auther`, `AuthExchange`, `AuthBegin`, `AuthStep`, `ErrBadAuthMethod`) drives the MQTT-5.0 §4.12 AUTH exchange for a CONNECT that names an Authentication Method, and a client's re-authentication (AUTH 0x19) on an open connection; a rejected CONNECT is a CONNACK error, a failed or stalled re-authentication a DISCONNECT. A re-authentication that proves a different identity re-runs the `Authorizer` over the session's subscriptions and Will for it, as `Broker.Reauthorize` does, dropping those no longer allowed; if that check cannot complete the connection is disconnected with 0x87. With no `Auther` a CONNECT with a method is still refused with 0x8C. `Authenticator` is not asked for such a CONNECT.
- **Added:** an unrestricted broker logs a warning, once, the first time a client publishes, subscribes or sets a Will on a topic starting with `$` (other than the reserved `$retained` and `$queue`, which stay refused), saying a future major version may refuse them by default and that `Options.RestrictDollarTopics` refuses them now. Behaviour is otherwise unchanged.
- **Added:** the `natsmqtt5` command serves WebSocket: `-ws-listen` (with `-ws-path`, default `/mqtt`), and `-tls-cert` / `-tls-key` make `-listen` (mqtts) and the WebSocket listener (wss) serve TLS. Without them nothing changes.
- **Fixed:** the WebSocket handler's read limit was never applied (`websocket.NetConn` lifts it), so a client's single huge message was streamed to the packet reader. A message is now limited to `MaximumPacketSize` plus 64 KiB and a larger one is closed with status 1009 up front.
- **Fixed:** a resume reads the offline queue through the subjects its subscriptions match, and when those overlapped without one covering the other (`a/+/c` with `a/b/+`) it read the whole rewind window, and when a broader filter came after narrower ones (`a/b`, `a/+/c`, `a/#`) the filter set still overlapped, which JetStream refuses, so the replay failed and the messages queued while the client was away were not delivered. Overlapping filters are now joined into subjects that cover them and overlap nothing.
- **Changed:** with `DurableWills`, the Will index each broker keeps holds only a reference per record (owner, revision, whether it has ended and when it is due), not the decoded Will, and a record is read from the bucket when a broker is to adopt it. A broker's memory no longer grows with the size of its clients' Wills: 174 B per record whatever the payload, against about 330 B plus the Will (a 64 KiB Will cost 64 KiB in every broker). No API change.
- **Added:** with `DurableWills`, a broker deletes, unpublished, a Will record that ended and was due more than 24 hours ago. The `_wills` bucket has no time-to-live (an open connection's record can be years old), so a record a fault left behind was indexed by every broker for ever; age is counted from the due time, so no Will Delay Interval can outlast it. A Will whose broker died is adopted within seconds and never reaches that age unless every broker was down for a day. Not configurable.
- **Documented:** what `DurablePublish` and the Will bucket each guarantee, that a published Will is not held for the flush and why, and that the Will bucket needs the NATS permissions of the session bucket (README, "Durable Will Messages").
- **Changed:** a shared-subscription message handed back by `Options.SharedHoldLimit` is no longer left in the slow
  member's in-flight set: a reconnect of that session does not resend it (the group already has it), and its Packet
  Identifier is owed as a withdrawn one, so a late PUBACK is ignored and returns the send-quota slot. With the limit
  unset nothing changes.
- **Changed:** with persistent sessions, a surviving broker removes the membership entry of a shared-subscription member
  whose session record is gone (a session killed with its broker whose record the store's sweep has reclaimed), and ends
  the group if that was its last member, instead of waiting for the entry to lapse. Entries are written with the
  consumer name and Client Identifier after the instance; older entries are still read and only lapse.
- **Changed:** a connected shared-subscription member pulls its backlog in batches sized to the free room under its
  client's Receive Maximum (at most 64), not one message per round trip to JetStream; it never holds more than its client
  could be sent, and gives back the messages it holds when the connection ends.
- **Tests:** a broker killed with SIGKILL while a shared member holds a QoS 1 message is pinned: restored before
  `AckWait` the restored session holds the message again and the group never has it; restored after, the client sees it
  twice (at-least-once), not thrice (`NATSMQTT5_SLOW_TESTS=1`).
- **Changed:** QoS 2 messages that wait together for a persistent session share the session-record write that precedes their PUBLISH, and PUBRELs queued during a write share the next one; a burst of 32 delivers in under a third of the time. The guarantees of [MQTT-4.3.3-6] are unchanged.

- **Changed:** a session whose unacknowledged messages spill into the payload bucket no longer rewrites a chunk that did not change at each checkpoint, and the sweep deletes the payloads a released session's record does not name.
- **Fixed:** with `DurablePublish`, a PUBACK or PUBREC could acknowledge a message whose live copy was lost. If the NATS connection dropped just after the broker handed the live publish to a connected NATS client, the bytes went into the dead socket, the client had nothing buffered to resend, and its flush was answered after the reconnect. The broker now refuses with 0x83 when the connection reconnected between the live publish and the confirmation, so the client sends the message again (the queue stream may then hold a duplicate, as for any 0x83). A publish made while NATS is already reconnecting is unchanged: it waits in the client's buffer. Seen as a flake of `TestDurablePublishSurvivesNATSBeingCutAfterTheQueueCopyIsStored` under `-race` in CI on 2026-10-23.
- **Changed:** dependencies: nats.go v1.54.0 to v1.55.0, nats-server (tests) v2.15.0 to v2.15.1, and patch bumps of golang.org/x/{crypto,net,sys,text} and klauspost/compress. No API change.
- **Fixed:** a broker cut off from NATS for longer than `RetainedTombstoneTTL` kept serving a retained message another broker had cleared, because the tombstone was purged before it could read it. The sweep now notices that its retained view holds more entries than the stream has messages and drops those the stream no longer holds. The tombstone purge also treats the server's "sequence not found" (error 10043) as already purged. Found with the three-server cluster harness.
- **Fixed:** a broker cut off from its cluster no longer announces a Will a surviving broker has already adopted. When the
  removal of a Will's record failed for a reason other than losing the race (the store unreachable, as it is from the
  minority side of a partition), the broker published the Will at once; it now retries the removal until the store answers
  and publishes only if it wins, so the adopter's publication is the only one. A broker that has not fenced itself, with the
  store failing alone, still publishes after about twice the survivors' adoption wait. Found with the new three-server
  cluster harness (`natscluster_test.go`, `TestCluster...`, gated behind `NATSMQTT5_CLUSTER_TESTS`).
- **Added:** `Options.EscapeNATSWildcards` (off by default). A topic may then contain `*`, `>`, a space, tab, line feed,
  carriage return, form feed or DEL: each is stored in the NATS subject as `/s`, `/g`, `/p`, `/t`, `/n`, `/r`, `/f`, `/d`,
  and a literal `*` level is no longer a NATS wildcard. Topics without those characters map to the same subject as
  before. `-escape-nats-wildcards` sets it. New `topic.*Escaped` functions carry the mapping.
- **Added:** `Options.OutboundTopicAliases` (off by default). The broker sends Topic Aliases to a client that offered a
  Topic Alias Maximum: a topic of 8 bytes or more that repeats is sent as an alias with an empty name, up to the
  client's maximum, the least recently used alias reassigned when the table is full. The table lives with the network
  connection, so a resend after a reconnect carries the full name; a first use that would take a packet over the
  client's Maximum Packet Size is sent plain.
- **Added:** MQTT over WebSocket. `Broker.WebSocketHandler()` returns an `http.Handler` that upgrades a request
  (subprotocol `mqtt`, binary frames) and hands the connection to the same state machine as a TCP one;
  `Options.WebSocketOriginPatterns` allows browser origins other than the request's host. A new dependency,
  `github.com/coder/websocket`. MQTT-6.0.0-1, -2 and -4 move from not-applicable to tested.
- **Fixed:** a SUBSCRIBE that replaced an existing subscription could still make a message being delivered at that instant
  vanish [MQTT-3.8.4-4], a narrower window left by the v0.11.1 fix. The delivery handler found the old subscription current,
  then found it retired, and dropped the message instead of passing it to the replacement. It now resolves the
  subscription again when the one it found is no longer live. Seen once in CI (`TestSubscribe_MQTT_3_8_4_4`, QoS 0, message
  60 of 300) and reproduced in a unit test against the real `installSubscription`; not measured for QoS 1 and 2.

## [0.12.1] - 2026-10-13

- **Fixed:** a shutdown (Serve's context cancelled, or `Close`) that began just after a client had read its CONNACK could
  close the connection without the DISCONNECT 0x8B it is owed, because the connection was only marked connected after the
  CONNACK write returned and the shutdown path skipped the DISCONNECT in that gap. It now waits for the CONNACK write
  first. Found by one CI failure of `TestShutdownAfterCancelledServeSendsDisconnect` on v0.12.0's merge commit; the
  early check that opened the window came in with #120 and is in v0.12.0, not in v0.11.3.

## [0.12.0] - 2026-10-13

- **Added:** a default offline queue that could not be created at start-up is retried in the background (5 s, doubling
  to a minute) instead of staying off until the broker restarts, and logged at info when it comes up. New
  `Broker.OfflineQueueActive()` reports whether the broker is keeping messages for disconnected sessions. Nothing changes
  for a queue that started, or one turned off with `DisableOfflineQueue`; an explicit `OfflineQueue` still fails start-up.
- **Added:** `Options.SharedHoldLimit` (default 0, unlimited). A shared-subscription member that is connected but
  never acknowledges a message otherwise keeps it for as long as the connection lives; with the limit set, the
  message goes back to the group once it has been held that long (checked every third of the ack wait), and a
  late PUBACK from the slow member is still accepted.
- **Fixed:** the refresh that keeps a shared subscription's membership entry alive could write it back just after
  the session unsubscribed, leaving a stale entry (and so the group and its backlog) until it lapsed. The refresh
  now checks the subscription again after writing and leaves the group if it is gone.
- **Added:** a resumed session that lost filters to the `Authorizer` is now told which. The CONNACK carries one
  `natsmqtt5-dropped-filter` User Property per dropped filter (`natsmqtt5.DroppedFilterProperty`), on both the
  in-memory and the stored-record resume. It is this broker's own, not MQTT v5, and is left out when the CONNACK
  would exceed the client's Maximum Packet Size. A client that ignores it behaves as before.
- **Added:** `Options.RetainedTombstoneTTL` (default 24 h, negative disables). Clearing a retained message leaves a
  zero-length marker in the stream; the periodic sweep now deletes markers older than the TTL, so a workload that
  retains and clears many distinct topics no longer grows the stream without bound. A broker cut off from NATS for
  longer than the TTL can miss a clear and keep the cleared message until it restarts.
- **Fixed:** a filter that appears after the resume check (a SUBSCRIBE from the connection being displaced, still
  decoding buffered packets) is no longer dropped when the `Authorizer` answers `ErrAuthorizerUnavailable` for it at
  resume. It was authorised when it was subscribed and no CONNECT is left to refuse, so it stays, as `Reauthorize`
  leaves one on an outage. A plain denial still drops it. Only a window one SUBSCRIBE wide, during an outage.
- **Fixed:** a retained message whose Message Expiry Interval has passed is now removed by the broker's periodic
  sweep (the one that expires detached sessions, every `SessionSweepInterval`), not only when a subscription happens to
  match it. Before, one that no subscription asked for after it expired stayed in the retained stream and in every
  broker's memory until a publish to its topic replaced it. It was never delivered, so this is storage, not conformance
  [MQTT-3.3.2-5].
- **CI:** a `Vulnerability scan` workflow runs `govulncheck` on pushes to `main`, on pull requests that change `go.mod`
  or `go.sum`, and weekly, on the newest Go. No library change. First run on 2026-10-10: nothing called in this module's
  dependencies; six standard-library findings, all fixed in Go 1.27.2.
- **Added:** `ErrAuthorizerUnavailable`. An `Authorizer` that cannot reach its policy returns it (bare or wrapped)
  and the broker no longer treats that as a denial: SUBSCRIBE gets `0x83` in the SUBACK, PUBLISH `0x83` in the
  PUBACK/PUBREC, and a CONNECT whose Will cannot be checked is refused with `0x83` without displacing the live
  connection, instead of `0x87 Not authorized`. `Broker.Reauthorize` leaves the subscriptions and Will it could not
  check and returns an error wrapping the sentinel. Additive: an Authorizer that never returns it behaves as before.
- **Added:** `ErrAuthorizerUnavailable` is honoured when a session resumes. Before, a policy service that failed
  during a reconnect storm cost each resuming client its stored filters and unacknowledged messages (the Authorizer's
  error read as a denial, the reduced set was written back, and the client was told `SessionPresent: true`). Now the
  resume is checked before the session is claimed or the connection holding it displaced, and a CONNECT whose filters
  could not be checked is refused with `0x83`; the record and the live connection are untouched and the client retries.
  The Authorizer is also asked once per filter on a resume, not twice. Any other error is still a denial.
- **Fixed:** a shutdown that raced a connection still in its handshake read the client's Maximum Packet Size while
  the handshake was writing it (found by `-race` in `TestMQTT_3_1_2_25_ANoConnackFitsBelowFiveBytes`, 1 run in
  about 100). The server DISCONNECT is now skipped before the CONNACK without touching handshake state. The race
  came in with the unreleased fix above and was never in a release.
- **Fixed:** with `RestrictDollarTopics` on, a persistent session that stored a `$app/...` subscription before the
  option was turned on kept receiving it after resume. The restricted broker now drops a stored `$` filter when it
  resumes the session (as it does a filter the Authorizer denies) and writes the reduced set back. Off by default
  and no record format change.
- **Fixed:** cancelling the context passed to `Serve` closed every client's socket at once, so a client got a bare TCP
  close instead of the DISCONNECT with reason 0x8B that `Close` sends; a client could also miss it if shutdown began
  in the instant after it received its CONNACK. Both now send the DISCONNECT. The spec allows either, so this
  is a courtesy, but a client library can only tell "the server is going away" from "the network failed" by it.
- **Tests.** The Will index's memory is measured (`TestWillIndexHeapPerRecord`): about 330 bytes per record plus the
  Will's payload, 16 MiB at 50,000 records. No library change.
- **Tests.** A NATS server restarting under two brokers (`will_watch_restart_test.go`): the Will index each
  broker keeps comes back, and a Will stored afterwards is adopted when its broker is killed. No library change.
- **Performance.** With `DurableWills`, a broker's Will check no longer lists the bucket and fetches every
  other broker's record each `WillCheckInterval`. It reads an in-memory index kept by a bucket watcher and
  pings each owner once; until the watcher has synced, or while it is down, it reads the bucket as before.
  Over ten checks of 100 records, 2,100 direct gets became none per record
  (`TestWillCheckDoesNotReadTheBucketEveryTick`). No option or record format change.
- **Tests.** `DurablePublish` with the flush failing is driven over a socket (`durable_flush_fail_test.go`): a
  NATS connection cut after the queue copy is stored still ends in a success PUBACK and one delivery to a
  subscriber on another broker, and NATS staying down ends in 0x83. No library change.
- **Performance.** A resume reads only the offline-queue subjects its own subscriptions match, not the whole
  rewind window: 50 sessions resuming over 100,000 queued messages took 7.6 s to the slowest, now 15 ms; one
  session over a million, 2.4 s, now 3 ms. A session whose subscriptions are all shared still reads the whole
  window. `TestReplayReadCost` (skipped unless `NATSMQTT5_REPLAY_COST` is set) measures it.
- **Tests.** Eleven more tests wait for the broker to log the detach after a `drop()` instead of sleeping a fixed
  100 to 300 ms (`restore_inflight_test.go`, `oversize_inflight_test.go`); no library change.
- **Fixed.** A queued message whose live copy never reached the broker that had the session (or reached it
  only after another broker claimed the session) was not delivered on resume when it was more than 2 s older
  than the release. A replay now reaches back `OfflineQueueRewind` (new option, 10 s by default) and the
  session record carries the delivered ids for that span (`DeliveredSince`, an added field; a record without
  it behaves as before). Retained messages sent on subscribe now carry a message id, so the wider rewind does
  not send them twice. A bound, not a guarantee: a copy delayed past the rewind is still not heard (ROADMAP).
  The record keeps in-flight entries ahead of delivered ids when both do not fit one value.

- **Fixed.** Unacknowledged messages that did not fit in a session record (about 15 under a 4 KiB NATS
  `max_payload`, thousands at the default) were cut, newest first, and not resent after the session moved.
  They are now kept in the `<StreamPrefix>_inflight` bucket in chunks the record names (`Spill`, an added
  field; a broker that predates it restores only the entries in the record, as before) and resent in order.

- **Fixed.** A payload in the `<StreamPrefix>_inflight` bucket that no session record could need (stashed
  by a broker that lost its session to a Clean Start elsewhere a moment earlier, or a key from before keys
  named a client) waited for the bucket's TTL. The store sweep now deletes keys whose owner has no record
  once they are ten minutes old, and old-format keys no record names. It does nothing if it could not read
  every record.

- **Fixed.** A session restored by another broker (or after a restart) wrote every oversize in-flight
  payload to the `<StreamPrefix>_inflight` bucket again at its first checkpoint, though the bucket held it.
  The session record now carries when the payload was written (`BlobAt`, an added field; a record without
  it is rewritten once, as before). A write time ahead of the restoring broker's clock counts as stale.
- **Fixed.** A payload in the `<StreamPrefix>_inflight` bucket whose delete failed (a NATS timeout at the
  wrong moment) was left until the bucket's TTL. The next checkpoint now tries the delete again.
- **Fixed.** With a NATS `max_payload` under about 18 KiB, an unacknowledged message of a few KiB was left
  out of the session record and not resent after the session moved to another broker. The inline share of
  the record now shrinks with `max_payload` (a quarter of the value limit, at most 16 KiB), and a larger
  message goes to the payload bucket. Together the inline messages are limited to a quarter of the value, so many small ones no longer push
  the newest out of the record either. At the default 1 MiB only a client holding more than about 230 KiB of
  unacknowledged messages sees a difference (the excess goes to the bucket rather than being cut).
- **Fixed.** The payloads a session kept in the `<StreamPrefix>_inflight` bucket outlived the session
  when it was discarded rather than completed: a reconnect with Clean Start, a session with Session Expiry
  Interval 0 ending with its connection, and an expired record swept. Each now deletes the session's
  payloads with its record, instead of leaving up to 1 MiB values until the bucket's TTL. Payloads
  written by versions before keys named their client are still left to the TTL.

## [0.11.3] - 2026-10-09

A patch release: two fixes; no API change and the Go floor is unchanged. One behaviour to know before upgrading: a client that sends more QoS 2 PUBLISH packets unacknowledged than the broker's Receive Maximum is now disconnected with 0x93 (it was served before).

- **Fixed.** The payload of an unacknowledged message too large for the session record stayed in the
  `<StreamPrefix>_inflight` bucket until its TTL, twice the maximum Session Expiry Interval, after the
  client had acknowledged it, so a prompt client left up to that long of dead values behind (up to 1 MiB
  each). The payload is now deleted when the PUBACK or PUBCOMP completes the message, or the session
  drops it. Its key now names the client as well as the digest, so one session finishing does not take
  the payload another was sent under the same Packet Identifier. Keys written by earlier versions are
  still read, and are left to the TTL.

- **Fixed.** A client with more QoS 2 PUBLISH packets unacknowledged than the Receive Maximum the
  CONNACK stated (`Options.ReceiveMaximum`, 1024 by default; 65535 when the CONNACK had to leave it
  out to fit a small Maximum Packet Size) was served without limit. It is now sent DISCONNECT 0x93
  Receive Maximum exceeded and the connection is closed (MQTT-5.0 §3.3.4, §4.13.1
  [MQTT-4.13.1-1]). A PUBCOMP, or a PUBREC of 0x80 or more, gives the slot back; a resent
  PUBLISH and QoS 0 are not counted, and QoS 1 is answered before the next packet is read, so a
  client that respects the limit is never disconnected.

## [0.11.2] - 2026-10-09

A patch release: two fixes, tests and documentation; no API change and the Go floor is unchanged.

- **Fixed.** An unacknowledged message with no queue copy (a retained message, or the queue off) whose
  PUBLISH was over seven eighths of the NATS server's `max_payload` was logged and not resent after
  the broker serving the session stopped, though the payload bucket could hold it: the bucket was held
  to the limit of a session record. It now holds a PUBLISH up to `max_payload` less 512 bytes, which
  is what a retained message can be.
- **Fixed.** A SUBSCRIBE to a shared subscription could be refused with 0x83 when it raced the last
  other member's UNSUBSCRIBE: the leaver was deleting the group's JetStream consumer as the joiner
  updated it, and the joiner saw "consumer does not exist" or a failed store. The joiner now tries
  again until the deletion is over and creates the consumer afresh.
- **Changed (documentation and tests).** `conformance/mqtt5-statements.tsv` is rewritten for the
  state of main: 252 statements, 199 TESTED, 13 NOT-APPLICABLE, 40 CLIENT-ONLY, none VIOLATED or
  UNSURE. A row says TESTED only for tests that exist and cite its id, and the counts in
  `CONFORMANCE.md` are generated from the table; both are checked by tests
  (`conformance_table_test.go`). `CONFORMANCE.md` now lists what is not implemented and why that is
  allowed, and gives the findings as history by release. `ROADMAP.md` was audited against the code:
  it states that no known MUST violation is open, and adds entries for a client over its Receive
  Maximum (no 0x93), a Close after a cancelled Serve that can skip the 0x8B DISCONNECT, and topics
  with whitespace.
- **Tests.** The reserved bits of the CONNACK flags are zero [MQTT-3.2.2-1], and a CONNACK carries
  no User Properties [MQTT-3.2.2-20], are now driven over a socket.

## [0.11.1] - 2026-10-09

A patch release: every entry is a fix, there is no API change, and the Go floor is unchanged. One
behaviour to know before upgrading: with `DurableWills`, the Will of a broker that really died is
published up to about 4.5 `WillCheckInterval` later than before (see the fourth entry).

- **Fixed.** A client whose Maximum Packet Size was under 24 bytes (the success CONNACK
  stating the broker's limits) got no CONNACK at all and waited for one. The CONNACK now leaves out what
  is optional until it fits, in order: the availability flags that state their default, Topic Alias
  Maximum (then 0 for that connection), Maximum Packet Size, Receive Maximum. A client that sends no
  Client Identifier and cannot fit the one the broker assigns is refused with 0x85 in a CONNACK that
  does fit. Under 5 bytes nothing fits, and the CONNACK is discarded as before. CONNACKs that already
  fit are unchanged. [MQTT-3.1.2-24], [MQTT-3.2.2-16], [MQTT-3.3.2-12].

- **Fixed.** With `PersistentSessions`, a session restored after a broker restart forgot the
  identifiers of messages withdrawn on resume (a filter denied): the client's acknowledgement of one was
  answered with `0x82 Protocol Error` and the identifier could be handed to a new message first. The
  withdrawn set now travels in the session record, with its age, so the two-resumptions rule counts the
  connections made before the restart. It is written with the withdrawal itself, so a broker killed
  outright leaves it too. The record version stays 1; a broker that predates the field ignores it.
  [MQTT-4.4.0-1], [MQTT-2.2.1-4].

- **Fixed.** A resent PUBLISH took a send-quota slot, and an acknowledgement of the original
  that landed before the copy was written returned it, so for the length of that exchange the client had
  one message more in flight than its Receive Maximum allows. The slot now stays with the copy until the
  client's acknowledgement of it. An acknowledgement that arrives after the copy is written cannot be told
  from the copy's own and is treated as it always was. [MQTT-3.3.4-9].

- **Fixed.** With `DurableWills`, a broker that was alive but cut off from NATS had its
  clients' Wills published by a survivor while they were still connected. The broker now checks every
  half `WillCheckInterval` that it can reach NATS, and closes its client sockets after twice that interval
  without; a survivor adopts a silent broker's Wills only once the silence has lasted long enough for
  that to have happened. The Will of a broker that really died is therefore published up to about 4.5
  `WillCheckInterval` later than before. A reconnect that resumes the session while a survivor is adopting
  the old Will now cancels the adopted record too, rather than losing the race and leaving it to be
  published. [MQTT-3.1.2-8], [MQTT-3.1.3-9].

- **Fixed.** A client that reconnected with its session while its old connection was still
  ending could have the new connection's Will published or cancelled by the old one, and the old
  connection's Will went out although the client was live again. The Will now belongs to the connection
  that set it, a takeover with the session kept cancels the displaced connection's Will (and its stored
  record with `DurableWills`), and a replaced Will no longer leaves a record in the Will bucket for a
  surviving broker to publish. A clean-start takeover still publishes the old Will, because the session
  ends. [MQTT-3.1.2-8], [MQTT-3.1.2-10], [MQTT-3.1.3-9].

- **Fixed.** A subscriber that dropped and resumed could lose a message when its live copy
  reached the broker after the connection had ended and a later message had already raised the replay
  position. The resume now replays by time when the message at that position was stored after the rewind
  point, so the earlier message is found. A live copy delayed past that point, or one that arrives after
  the connection has gone and the session waits for the next, is now noted on the session and in its stored
  record (`AwayLateSeq`, additive), so the next replay on this broker or another starts at or below it
  [MQTT-4.4.0-1]. A copy arriving after another broker claimed the session is still lost (ROADMAP).
- **Fixed.** With `PersistentSessions`, an unacknowledged message with no copy in the offline
  queue (a retained message, or any with the queue off) and a PUBLISH over 16 KiB was not resent by the
  broker that restored the session. The PUBLISH is now written to a bucket of its own
  (`<StreamPrefix>_inflight`, created on first use) and the session record names it, so the record stays within `max_payload`
  and the message is resent with its original Packet Identifier and DUP 1 [MQTT-4.4.0-1]. Proved with a
  real SIGKILL and a 100 KiB retained message at QoS 1 and QoS 2 (`oversize_inflight_test.go`). The new
  bucket is created on first use; a broker that predates it ignores the record's new field.
- **Fixed.** A QoS 2 message delivered to a client in the last second before its broker
  was killed outright was delivered again as a new message by the broker that took the session over.
  The session record is now written before the PUBLISH goes out and before the PUBREL does, so a kill
  at any step of the exchange resends the PUBLISH (DUP 1) or the PUBREL with the original Packet
  Identifier and never publishes the message a second time [MQTT-4.3.3-6], [MQTT-4.4.0-1]. The cost
  is two key-value writes per QoS 2 message sent to a persistent session: a lockstep delivery takes
  about 0.77 ms instead of 0.16 ms (`BenchmarkQoS2Delivery`); `-session-checkpoint-interval -1`
  turns every checkpoint off. Proved with a real SIGKILL at each step
  (`killed_qos2_outbound_test.go`).

### Fixed

- **Fixed.** Two ways a client could lose what it was owed. A QoS 1 or 2 live copy that
  reached a connection as it was ending was dropped half the time, and a later message then moved the
  resume replay's start past it, so the client never got it. It is now noted as owed and replayed
  [MQTT-4.4.0-1]. And with `PersistentSessions`, a late-delivered old write of the session record was
  read as another broker taking the session, disconnecting the client that had just arrived there
  and dropping its subscriptions [MQTT-3.1.4-3]; only a write newer than this broker's own claim
  counts now. Several timing-sensitive tests no longer depend on a fixed wait.
- **Fixed.** `Broker.Close` cleared its session store field while a late NATS callback could still
  read it to persist a copy, a data race under `-race`. Close now leaves the field set; a write that
  arrives after the store is closed fails and is logged.
- **Tests.** The restore of a session record is now driven over the wire for a queue copy
  that aged out between the release and the resume (dropped, the session carries on) and for a message
  still unacknowledged when its filter was unsubscribed (still resent). [MQTT-4.4.0-1].

- **Fixed.** With `PersistentSessions`, an unacknowledged message with no offline-queue
  copy (a retained message sent on subscribing, or any with `DisableOfflineQueue`) was left out of the
  session record and not resent by a restored session. The record now carries its encoded PUBLISH
  (up to 16 KiB), and the session is checkpointed with the queue off too. [MQTT-4.4.0-1].
- A session whose broker was killed outright (`kill -9`) was resumed from the start of the offline
  queue, up to a day of repeats. A connected session now writes its replay position to its record
  once a second while it moves, and the broker that takes the session over replays from it, so a
  kill costs the last second. New option `Options.SessionCheckpointInterval`, flag
  `-session-checkpoint-interval`, env `NATSMQTT5_SESSION_CHECKPOINT_INTERVAL` (negative turns it
  off). [MQTT-3.1.2-5], [MQTT-3.1.2-23], [MQTT-4.5.0-1], [MQTT-4.4.0-1]
  (`killed_broker_test.go`, with a real SIGKILL of `cmd/natsmqtt5`).
- A session whose broker was killed outright now also keeps its in-flight state. The checkpoint
  writes the unacknowledged messages and the QoS 2 identifiers received and not released, so the
  broker that takes the session over resends the PUBLISH and PUBREL packets with their original
  identifiers and DUP, and acknowledges a resent QoS 2 PUBLISH without forwarding it twice. The
  record is written before the PUBREC and before the PUBCOMP of a QoS 2 PUBLISH the client sends,
  so neither promise is older than the record. [MQTT-4.4.0-1], [MQTT-4.3.3-10], [MQTT-4.3.3-12]
  (`killed_inflight_test.go`, with a real SIGKILL of `cmd/natsmqtt5`).
- Subscribing again to a filter the session already holds no longer loses the messages published
  while the subscription is replaced [MQTT-3.8.4-4]. A repeated SUBSCRIBE on a non-shared filter
  keeps its NATS subscriptions and swaps only the options; a shared one hands the messages the old
  member had been sent to the new. Before, 1 to 7 of 300 messages went missing across 12 repeats.

## [0.11.0] - 2026-10-08

**Behaviour changes for existing deployments, none of them behind an option.** A broker upgraded
from v0.10.0 with no configuration change now: enforces Message Expiry Interval on every delivery
path (a message past its interval is no longer delivered); delivers a shared subscription's QoS 1
message to another member when the first one's session ends, and deletes the shared backlog when the
last member leaves; sweeps detached sessions whose Session Expiry Interval has passed, so a session
a client never resumes is now gone; sends an acknowledgement over the client's Maximum Packet Size
without its Reason String instead of dropping it; and disconnects with 0x82 Protocol Error a client
that sets No Local on a shared subscription, puts a Subscription Identifier in a PUBLISH, or sends a
wildcard Response Topic. Clients that were lenient about these will notice. Three new options are
opt-in and change nothing until set: `DurableWills`, `DurablePublish`, `RestrictDollarTopics`. The Go
API is additive: new `Options` fields and `DefaultSessionSweepInterval`, nothing removed or changed.

### Changed by default

- **Message Expiry Interval is enforced.** A message whose interval has passed is no longer
  delivered, and one still live goes out with the interval less the time it waited in the broker,
  on every path: live, retained (the wait counts from storage, and an expired retained message is
  removed from the stream), offline-queue replay, catch-up, shared backlog and resends. Time is
  counted in whole seconds, rounded down. A resent QoS 1 PUBLISH that expired in flight is
  deleted; a QoS 2 PUBLISH already sent is resent regardless [MQTT-3.3.2-5], [MQTT-3.3.2-6],
  [MQTT-4.3.3-7].

- **A shared subscription ends when its last session does.** The backlog consumer behind
  `$share/g/f` is now deleted, with the messages in it, when the last session subscribed to it
  leaves by UNSUBSCRIBE, by a Session Expiry Interval of 0, by expiry, by a Clean Start takeover or
  by losing the right to resume it, so a client that subscribes to the name later starts with
  nothing (MQTT 5.0 §4.8.2). The brokers keep a key-value entry per member session in a new bucket
  `<StreamPrefix>_share_members`, which a broker rewrites for the sessions it holds and which lapses
  for the sessions of a broker that was killed. It needs the offline queue, like the backlog.

- **A shared member's unacknowledged QoS 1 message goes to another member.** The broker keeps the
  message unacknowledged in JetStream until the member's PUBACK, holding off redelivery while the
  member is connected, and hands it back to the group when the member's session ends: expiry,
  a Session Expiry Interval 0 disconnect, a Clean Start takeover, or a restored session that may
  not resume the subscription. A member that PUBACKs is never redelivered to. [MQTT-4.8.2-6]
  stays as it was; this is the MQTT 5.0 §4.8.2 SHOULD.

- **Detached sessions that are never resumed are now expired.** A session whose client disconnected
  with a non-zero Session Expiry Interval kept its NATS subscriptions until the client returned or the
  broker stopped, so a workload that churns through Client Identifiers grew the broker's subscription
  set without bound. A sweep now drops each such session once its interval (capped by
  `MaxSessionExpiry`) has passed, and unsubscribes it from NATS. `Options.SessionSweepInterval`
  (default `DefaultSessionSweepInterval`, one minute) sets how often it runs. A session lives at
  least its interval and at most one sweep longer; sessions with an interval of 0 and connected
  sessions are not touched. [MQTT-3.1.2-23].

- **An acknowledgement over the client's Maximum Packet Size is sent without its Reason String.**
  A CONNACK, PUBACK, PUBREC or DISCONNECT that would exceed the limit was discarded whole, so a
  refused CONNECT, or a refused QoS 1 or 2 PUBLISH, got no answer at all and the client's exchange
  never finished. The Reason String and User Properties are dropped first and the rest is sent;
  only a packet that is still too large is discarded [MQTT-3.2.2-19], [MQTT-3.2.2-20],
  [MQTT-3.4.2-2], [MQTT-3.5.2-2], [MQTT-3.14.2-3]. A CONNECT's Maximum Packet Size now also binds
  the CONNACK that refuses it for an unsupported Authentication Method.

- **No Local on a shared subscription is a Protocol Error.** A SUBSCRIBE that set No Local on a
  `$share/` filter used to get 0x82 inside the SUBACK, which is not a valid SUBACK Reason Code.
  The broker now sends a DISCONNECT with 0x82 and closes the connection, and subscribes none of
  the packet's filters [MQTT-3.8.3-4], [MQTT-3.9.3-2], [MQTT-4.13.1-1].

- **A client PUBLISH with a Subscription Identifier is a Protocol Error.** The broker used to
  accept, acknowledge and forward it. It now sends a DISCONNECT with 0x82 and closes the
  connection, acknowledging and forwarding nothing [MQTT-3.3.4-6], [MQTT-4.13.1-1].

- **A Response Topic with a wildcard is a Protocol Error.** The broker forwarded it unchecked. A
  PUBLISH carrying one now gets a DISCONNECT with 0x82 and the connection is closed, and a CONNECT
  whose Will carries one is refused with CONNACK 0x82 [MQTT-3.3.2-14], [MQTT-4.13.1-1].

- **Nothing follows a server DISCONNECT.** A PUBLISH queued for a client could still be written
  after the broker's DISCONNECT, because the delivery goroutine and the read loop write
  independently. Once the DISCONNECT is written, or decided against, every later write on that
  connection is refused [MQTT-3.14.4-1].

- **An Authenticator's Reason Code is checked.** A `ConnectError` code was sent in the CONNACK as
  given, so a success code, or one only DISCONNECT or SUBACK may carry, could reach the wire. A
  code MQTT-5.0 Table 3-1 does not list for a CONNACK is logged and sent as 0x80 (Unspecified
  error) [MQTT-3.2.2-8]. Codes the table lists are unchanged.

### Added (opt in, off by default)

- **The Will Message of a broker that was killed is now published.** `Options.DurableWills`
  (`-durable-wills`, `NATSMQTT5_DURABLE_WILLS`) keeps the Will of every open connection in a
  JetStream key-value bucket, `<StreamPrefix>_wills`. A surviving broker that finds the owner no
  longer answering its liveness subject adopts the Will, waits out what is left of the Will Delay
  Interval and publishes it; a client reconnecting inside the delay cancels it; exactly one broker
  publishes, by a compare-and-swap on the record. `Options.WillCheckInterval` (`-will-check-interval`,
  default 5s) bounds how long a dead broker's Wills go unnoticed. **It is off by default**: it
  stores the Will's payload in clear text in that bucket, and it needs JetStream. Behaviour
  without the option is unchanged. [MQTT-3.1.2-7], [MQTT-3.1.2-8], [MQTT-3.1.2-10], [MQTT-3.1.3-9].

- **A PUBACK or PUBREC that means the message is safe, opt in.** `Options.DurablePublish`
  (`-durable-publish`, `NATSMQTT5_DURABLE_PUBLISH`) makes the broker acknowledge a QoS 1 or 2
  PUBLISH only once JetStream has stored the queue's copy and the NATS server has confirmed the live
  publish (a flush), and refuse it with 0x83 otherwise. It requires the offline queue and costs one
  more NATS round trip per publish; the README has the measured cost of the queue and of this.

- **Keep clients off `$` topics, opt in.** `Options.RestrictDollarTopics`
  (`-restrict-dollar-topics`, `NATSMQTT5_RESTRICT_DOLLAR_TOPICS`) makes the broker follow MQTT 5.0
  §4.7.2's "SHOULD prevent Clients from using such Topic Names" for every `$` topic: a PUBLISH or
  Will to one is refused with Topic Name invalid (0x90), a subscription to one with Topic Filter
  invalid (0x8F), and a `$share/...` subscription is allowed unless the filter inside it starts
  with `$`. Off by default, so `$app/...` keeps working; `$retained` and `$queue` stay refused
  either way.

### Fixed

- **A takeover in the middle of a stored-session restore no longer loses the session's
  subscriptions.** With `PersistentSessions`, a second CONNECT on a Client Identifier that landed
  while the first was still restoring the session from its record (the Authorizer call on a resume
  has no bound) was answered Session Present 1 and given a session with none of the stored filters,
  and the empty set was written back to the record. The second connection now finishes the restore.
  [MQTT-3.1.2-23], [MQTT-3.2.2-3].

- **A session narrowed repeatedly no longer runs out of Packet Identifiers.** The identifier of a
  message taken back on resume (its filter was denied) stayed spent until the client acknowledged
  it, and a client that never does left it spent for the life of the session. It is now forgotten
  on the second resumption after the withdrawal, when the client has had two connections to send
  the acknowledgement [MQTT-2.2.1-4].

- **A withdrawn identifier only accepts the acknowledgement it is owed.** A PUBCOMP for a withdrawn
  QoS 1 message, or a PUBACK for a QoS 2 one, used up the record and was ignored; it is now a
  Protocol Error, and the record stays for the right acknowledgement.

- **A late acknowledgement no longer disconnects the client.** An acknowledgement that landed
  between a resend reading the in-flight entry and writing it left the broker resending a finished
  exchange, and the client's acknowledgement of that copy was answered with 0x82 Protocol Error. The
  connection now remembers which identifiers it resent and ignores the unmatched acknowledgement it
  is owed for them [MQTT-4.4.0-1].

- **A resume no longer skips a message the dropped connection was sending.** The resumed
  connection took its list of unacknowledged messages while the old connection's delivery
  goroutine could still send and track one more, so that message was missing from the resend and
  from the replay (the replay believed it delivered) and came only on the next resumption. The new
  connection now waits, bounded, for the old one's delivery goroutine before it resends
  [MQTT-4.4.0-1]. Reproduced on one scheduler thread at about one resume in twenty.

- **A retained message no longer carries its publisher's Topic Alias.** A subscriber on the same
  broker could be sent the alias the publisher used, which means nothing on its connection and is
  not allowed when the subscriber advertised no Topic Alias Maximum [MQTT-3.1.2-26],
  [MQTT-3.1.2-27], [MQTT-3.3.2-11]. It showed up in about one run in four, depending on whether the
  broker's own copy or the one read back from the stream was served.

- **A connection that drops while behind no longer loses what was waiting for it.** The messages
  queued for a client, and the stretch of the offline queue a catch-up had still to send, used to
  be discarded when its connection ended, and the resume replay started only 2 seconds before
  the disconnect, so a client minutes behind lost those minutes [MQTT-3.1.2-23], [MQTT-4.5.0-1].
  The replay now starts at the lowest queue sequence the client had not been sent. It also no
  longer gives up after 30 seconds: a slow client with a long backlog gets all of it, at its own
  pace. A session restored from the session store still replays from the time it was released.

- **A restored session replays from what it was owed, and a killed broker's sessions are replayed.**
  With `PersistentSessions`, the session record now holds the lowest queue sequence the client had
  not been sent and the ids it had been sent above it, so a session resumed after a restart or on
  another broker gets what was waiting for it, not only what was published after the release
  [MQTT-3.1.2-23], [MQTT-4.5.0-1]. Records written by earlier versions still load and replay as
  before. A session left attached by a broker that was killed outright is replayed from the start
  of the queue (`OfflineQueueMaxAge`) when it is resumed, which loses nothing and can repeat
  messages the dead connection delivered.

- **A session restored from the session store resends what it had in flight.** With
  `PersistentSessions`, a session resumed after a restart or on another broker used to come back
  with nothing unacknowledged, so those messages were lost, and a QoS 2 PUBLISH the client resent
  was forwarded a second time. The record now keeps each unacknowledged message's Packet
  Identifier, QoS state and offline-queue sequence, and the QoS 2 identifiers received and not
  released. The restored session resends the PUBLISH with its original identifier and DUP 1 (or
  the PUBREL, past the PUBREC), drops one that expired, and acknowledges a resent QoS 2 PUBLISH
  without forwarding it again [MQTT-4.4.0-1], [MQTT-4.3.3-10], [MQTT-3.1.2-5]. Payloads are read
  back from the offline queue, so a message with no copy there is not resent; a record too large for
  one key-value value is cut, newest entries first, and logged. Records written by earlier
  versions still load.

## [0.10.0] - 2026-10-08

**Behaviour change for existing deployments.** The offline queue is now on by default whenever
the broker uses JetStream, so a broker upgraded from v0.9.x with no configuration change starts
creating the `MQTT5_queue` stream and storing every QoS 1 and 2 message for up to 24 hours. Size
JetStream storage for that, set `OfflineQueueMaxAge` to change the window, or set
`DisableOfflineQueue` (`-no-offline-queue`) to keep the old behaviour. No Go API was removed.

### Changed

- **The offline queue is on by default whenever the broker uses JetStream**, which it does with
  retained messages (the default) or `PersistentSessions`. MQTT 5 requires it: QoS 1 and 2
  messages pending for a session are session state that outlives a disconnect
  [MQTT-3.1.2-23], [MQTT-4.5.0-1], and until now a resumed session was told Session Present 1
  and given none of what was published while it was away. What changes for an existing
  deployment that set nothing:
  - A new stream, `MQTT5_queue` (file storage, 24 hours by default), holds every QoS 1 and 2
    message published through a broker. Size its JetStream storage for a day of that traffic,
    or set `OfflineQueueMaxAge`.
  - A QoS 1 or 2 PUBACK or PUBREC now waits for JetStream to store the copy, one extra round
    trip per publish.
  - If the stream cannot be created, the broker logs a warning and runs as before rather than
    failing to start.
  - A broker with `DisableRetained` and without `PersistentSessions` is unchanged: it does not
    use JetStream and has no queue.

- **A shared subscription's QoS 1 and 2 messages go only to members whose client is connected**,
  on any broker, and wait for the first member back when none is. Before, NATS chose among all
  members, and a member whose client was away dropped its share: a probe lost 27 of 40 messages
  [MQTT-4.5.0-1], [MQTT-4.1.0-2]. Each shared subscription now has a durable consumer on the
  offline-queue stream (`MQTT5_share_<hash>`), removed by JetStream once no member has pulled for
  longer than `MaxSessionExpiry`. It needs the offline queue; without it, shared subscriptions
  behave as before. Brokers sharing a `SubjectPrefix` must agree on whether the queue is on.

- **A connected client that falls behind no longer loses QoS 1 and 2 messages.** Past 2048
  messages waiting for one connection the broker dropped them [MQTT-4.1.0-1], [MQTT-4.5.0-1]. With
  the offline queue on, the connection now reads the stream from the first message it could not
  hold, at the client's pace, in order, and returns to live delivery once it has caught up. A
  message's live copy carries its stream position in a new `Mqtt5-Queue-Seq` header. QoS 0, and
  everything when the queue is off, is still dropped at that point.

### Added

- `Options.DisableOfflineQueue` and `-no-offline-queue` (`NATSMQTT5_NO_OFFLINE_QUEUE`) turn the
  queue off. `OfflineQueue` and `-offline-queue` now mean the queue is required: a broker that
  cannot create its stream does not start. Setting both is `ErrInvalidOptions`.

## [0.9.1] - 2026-10-07

A security fix. No API change and no configuration change.

### Security

- **A client could replace any retained message, and read the offline queue, through the
  broker's internal subjects.** The retained store and the offline queue keep their messages
  under `<prefix>.$retained.>` and `<prefix>.$queue.>`, which is inside the subject space MQTT
  topics map onto. A PUBLISH to `$retained/news` replaced the retained message for `news`, with
  the `Authorizer` asked only about `$retained/news`, so a permission on `news` itself did not
  apply. A SUBSCRIBE to `$queue/#` (with `OfflineQueue` on) received a copy of every QoS 1 and
  QoS 2 message on the broker, and to `$retained/#` every retained-message write. Topic Names and
  Filters whose first level is `$retained` or `$queue` are now refused: PUBLISH with `0x90`, SUBSCRIBE
  with `0x8F`, a Will with a `0x90` CONNACK. Other `$` topics are unaffected. Found by a
  conformance review against the MQTT 5 specification. The retained-message forgery affects
  every release with retained messages; reading the queue affects v0.7.0 to v0.9.0 with
  `OfflineQueue` on (reproduced on v0.9.0: a `$queue/#` subscriber received another client's
  `private/payroll` message).

  **Check your retained messages** if untrusted clients could publish: any retained message may
  have been replaced. A session that had subscribed to one of these filters before the upgrade
  fails to restore from the session store; reconnecting with Clean Start clears it.

## [0.9.0] - 2026-10-07

### Changed

- **The minimum Go version is now 1.26** (was 1.25). If you build on Go 1.25, stay on v0.8.0.
  Go 1.25 has been out of upstream security support since 2026-08-19, and the NATS releases below
  declare `go 1.26.0`, so the floor could not stay without holding them back.
  ([#11](https://github.com/taumatix/natsmqtt5/issues/11))
- `github.com/nats-io/nats.go` v1.53.1 → v1.54.0, the NATS client the broker is built on.
- `github.com/nats-io/nats-server/v2` v2.14.5 → v2.15.0, used only by the tests.
- The Docker image builds with `golang:1.26-alpine`, and CI tests Go 1.26 and the current stable
  release.

### Security

- `golang.org/x/crypto` v0.55.0 → v0.57.0, past [GO-2026-6354] and [GO-2026-6355], two DoS
  advisories in `x/crypto/ssh`. `govulncheck` never found them on a call path from this module,
  which does not speak SSH, but they could not be patched from a Go 1.25 floor.

### Fixed

- **`compose.yaml` started v0.4.2, whatever release you downloaded it from.** Its two
  `image:` lines were not bumped from v0.4.3 to v0.8.0, so the compose quickstart the README
  hands out at each tag ran the v0.4.2 broker. At this tag it starts v0.9.0. A new test,
  `TestReleaseVersionsAgree`, fails when `compose.yaml` or a README install line names a version
  other than the newest one in this file.

[GO-2026-6354]: https://pkg.go.dev/vuln/GO-2026-6354
[GO-2026-6355]: https://pkg.go.dev/vuln/GO-2026-6355

## [0.8.0] - 2026-10-05

### Added

- **The offline queue covers sessions restored from the session store.** With
  `PersistentSessions` and `OfflineQueue`, a client that comes back after a broker restart, or to
  another broker, now gets what was published while it was away, as one returning to the broker
  that held it in memory already did. The stored record carries when the session was released
  (`AwayAt`), and the claim that restores it clears that again, so a later claim cannot replay an
  old absence. A restored replay starts at the release, not just before it. A broker killed
  without releasing its sessions records no release, and nothing is replayed for them.

## [0.7.0] - 2026-10-05

### Added

- **An offline queue (`Options.OfflineQueue`, `-offline-queue`).** QoS 1 and 2 messages
  published while a session is disconnected used to be dropped for it. With the queue on, they
  are kept in a JetStream stream (`MQTT5_queue`, for `OfflineQueueMaxAge`, 24 hours by default)
  and delivered when the session resumes. Delivery is in publish order, before live traffic, and
  exactly once across the drop and the resume. This covers sessions held in the memory of the
  broker the client returns to. Sessions restored from `PersistentSessions` come next (see
  ROADMAP.md).
  The queued copy is stored, and confirmed by JetStream, before the message is published live
  and acknowledged. That adds a JetStream round trip to each QoS 1 and 2 publish, and refuses
  the publish if JetStream cannot store it. Storing it afterwards could lose a message published
  just before a client resumed. CI caught that on the first version, and it is reproduced in
  the tests by delaying the store.
- `Options.OfflineQueueMaxAge`, `OfflineQueueStorage` and `OfflineQueueReplicas`.

### Changed

- **Every message the broker publishes carries a `Mqtt5-Msg-Id` header**, so its live delivery
  and its queued copy are known to be the same message. NATS-native subscribers see one more
  header.

## [0.6.0] - 2026-10-05

### Added

- **`Broker.Reauthorize` reaches the broker holding the session, from any broker.** With several
  brokers on one NATS cluster, a deployment had to call it on the one serving the client, which it
  had no easy way to know. Brokers sharing a `SubjectPrefix` now listen on
  `_NATSMQTT5.reauthorize.<prefix>`. A broker that does not hold the session asks there, and the
  one that does runs the sweep and answers. When none does, `Reauthorize` returns nil after
  `ReauthorizeForwardWait` (one second) or when its context ends.
- `ReauthorizeForwardWait`.

### Security

- Anything able to publish on `_NATSMQTT5.reauthorize.<prefix>` can make a broker re-run its
  `Authorizer` over a session. That can only narrow the session, but an `Authorizer` outage turns
  it into removed subscriptions. If untrusted clients share your NATS, restrict `_NATSMQTT5.>` to
  the brokers' NATS user.

## [0.5.0] - 2026-10-04

### Added

- **`Broker.Reauthorize(ctx, clientID)`: revoke a permission from a client that is still
  connected.** The `Authorizer` was asked at SUBSCRIBE, at publish, at CONNECT for the Will, and
  at resume, never while a client simply stayed connected. A client that kept its connection for
  a week kept receiving on a filter its principal lost on day one. `Reauthorize` re-runs the
  `Authorizer` over the session's subscriptions and Will:
  - a denied subscription stops delivering at once, including messages already queued for the
    connection;
  - its unacknowledged messages are withdrawn, so a resume does not resend them, and their
    acknowledgements still return the send-quota slot;
  - a denied Will is discarded, even one waiting out its Will Delay Interval.

  The connection stays up. Call it from whatever tells you about revocations.

### Fixed

- A message queued for a subscription that was then removed (by an UNSUBSCRIBE, or by
  `Reauthorize`) is no longer delivered after the removal, including one waiting for room in the
  client's Receive Maximum. MQTT-5.0 §3.10.4 allows either behaviour after an UNSUBSCRIBE.
- A delayed Will cancelled by a resumption stays cancelled even if the resumed connection ends
  before the delay does. Before, the original timer published it when it woke.
- `Broker.Close` no longer clears the NATS connection it drains, which raced with `Serve`'s
  start-up and with delayed Wills firing on shutdown. `Broker.NATS()` after `Close` now returns
  the drained connection rather than `nil`.

## [0.4.3] - 2026-10-04

A fix. No API change and no configuration change.

### Fixed

- **A takeover could get the new connection disconnected for acknowledging a message.** When a
  connection is taken over, an acknowledgement it had already buffered is still applied: the
  client did receive that message, and the session belongs to the Client Identifier
  (MQTT-5.0 §4.1). But by then the new connection may have resent the same message, and its
  client acknowledges that copy too. The broker answered that second acknowledgement with
  `0x82 Protocol Error` and closed the connection. It now ignores it, and does not reuse the
  Packet Identifier before it arrives.
- **The same race cost the new connection a send-quota slot.** The slot its resend took was
  returned to the old connection's quota instead, so the new connection could send one fewer
  message at a time from then on. With a Receive Maximum of 1, delivery stopped. The slot now
  comes back to the connection that spent it.

## [0.4.2] - 2026-10-04

A security fix. No API change and no configuration change.

### Security

- **A connection being taken over could remove its successor's subscriptions.** A connection
  displaced by a second CONNECT on its Client Identifier goes on decoding what its socket had
  already buffered. An UNSUBSCRIBE the client sent before the takeover was handled against the
  session the new connection now owned: it tore down the new connection's filter, and that client
  was never told. Reproduced end to end over a real NATS server before the fix, with a Keep Alive
  of 0. With a non-zero Keep Alive the race is narrower but not closed. UNSUBSCRIBE now checks
  that its connection still owns the session in the same locked step that removes the filter, and
  a displaced connection's UNSUBSCRIBE is refused. Its client never received an UNSUBACK, so it
  cannot have assumed the unsubscribe happened.
- Acknowledgements from a displaced connection are still applied, deliberately. They come from the
  same client, about messages it received, and dropping them makes the successor resend a message
  the client already acknowledged or refused.

## [0.4.1] - 2026-09-28

A security fix. No API change and no configuration change.

### Security

- **A connection being taken over could install a subscription into the
  session it had just lost.** An `Authorizer` call is not interrupted by a
  takeover, so a SUBSCRIBE decided on the old connection's principal could
  finish after a second CONNECT had claimed the Client Identifier — under a
  principal that may not read that filter — and after that connection's resume
  check had already run. The filter was installed anyway, and its messages were
  delivered to the new principal. Reproduced end to end over a real NATS server
  before the fix. A subscription is now installed only while the connection that
  asked for it still owns the session, checked under the lock the takeover
  takes; otherwise it is torn down and answered `0x80` on a socket nothing
  reads. The same check guards filters restored from a stored session record.
- Its NATS subscription no longer delivers until it is installed. The NATS side
  is bound first, and the handler delivers to whichever connection holds the
  session at the time, so a subscription refused at install could briefly carry
  messages to the wrong connection.

## [0.4.0] - 2026-09-27

A security fix with one behaviour change to check before upgrading: an
`Authorizer` is now asked about every Will Message, and one that denies a Will's
topic refuses that client's CONNECT. The Go API change is one new field, so
existing code compiles unchanged.

### Security

- **A Will Message now goes past the `Authorizer`** ([#24]). Before this, the Will was
  checked for QoS, RETAIN and topic syntax and then published with no
  authorization at all, so a principal denied `ActionPublish` on a topic could
  publish — and retain — to it by setting it as the Will and dropping its
  connection, at a moment of its choosing via the Will Delay Interval. The Will
  is now asked about at CONNECT as an `ActionPublish` under the connecting
  principal, and a denial refuses the connection with `0x87 Not authorized`.
  The check runs before the session is taken over, so a refused CONNECT does not
  displace a live connection on the same Client Identifier.

  **Check your `Authorizer` before upgrading** if your clients set Wills: one
  that denies publishing to a Will's topic now refuses those clients' CONNECTs
  where it used to let them in. A deployment without an `Authorizer` is
  unaffected, and so is a CONNECT without a Will.

### Added

- **`AuthzRequest.Will`** ([#24]), set only on that check and only with `ActionPublish`,
  so an implementation that audit-logs publishes can tell a Will — which may
  never be sent — from a PUBLISH the client made. The zero value is every other
  call site, so an existing `Authorizer` compiles unchanged.

## [0.3.1] - 2026-09-26

Documentation only. No code changed, so upgrading from 0.3.0 changes nothing at
runtime — this release exists because the instructions inside the `v0.3.0` tag say
something that is no longer true, and a tag cannot be edited.

### Fixed

- **The container image is public**, so `docker run ghcr.io/taumatix/natsmqtt5` and
  the `compose.yaml` quickstart work with no credential ([#7]). The README and
  `compose.yaml` carried a warning that they would fail with `denied`; that warning
  was correct until today and is now removed. Verified anonymously: every published
  tag resolves for `linux/amd64` and `linux/arm64`, with a nonexistent tag returning
  404 in the same run to prove the check discriminates.

  `v0.1.0` has no image and never did — the release workflow was added after that
  tag — so the oldest pullable version is `v0.1.1`.

### Added

- CI pulls the **published** image anonymously and drives the documented quickstart
  against it. The existing compose smoke test overrides `image:` with a locally
  built `natsmqtt5:ci`, which is right for testing the working tree and means it
  never checked that the thing users are told to run is reachable. That gap is why
  the image stayed unpullable for fourteen days with CI green throughout.

## [0.3.0] - 2026-09-26

### Added

- **The Authorizer is consulted while a session is resumed** ([#19]). A session
  is keyed by its Client Identifier alone, so a permission narrowed between two
  connections of the same client used not to take effect until the session
  expired — and the common case is a client reconnecting to the broker it just
  left. Every live Topic Filter of a resumed session now goes back past the
  `Authorizer` before the CONNACK, and a filter the connection may no longer
  hold is unsubscribed from NATS.

  Two things to plan for if you implement `Authorizer`:

  - It is now called once per live filter during a resumed handshake, so a slow
    implementation delays the CONNACK in proportion to the session's size. The
    `ctx` carries no deadline of the broker's making.
  - **A denial there is silent and final.** A CONNACK has no per-filter Reason
    Code, so the filter is torn down, logged at warn level, and the client is
    told `SessionPresent: true` with nothing to say a subscription is missing.
    With `Options.PersistentSessions` the reduced set is written straight to the
    durable record. An error returned because your policy store was unreachable
    is therefore indistinguishable from a decision to deny, and costs the client
    its subscription permanently — refuse the connection from the
    `Authenticator` instead if you cannot decide.

- **`AuthzRequest.Resume`** ([#19]), set only on a re-check of this kind and
  only with `ActionSubscribe`. The client performed no action and the same
  filters return on every reconnect, which matters if you audit-log, meter or
  rate-limit subscriptions — and it is the cheap way out of the added CONNACK
  latency, since you may answer a resume from a cache you would not trust for a
  fresh SUBSCRIBE. The zero value is the behaviour of v0.2.0 at both existing
  call sites, so an existing `Authorizer` compiles and behaves unchanged.

- **QoS 1 and QoS 2 retransmission on resume** ([#13]). What the previous
  connection left unacknowledged is resent when the session is taken over: the
  PUBLISH packets with DUP set and their original Packet Identifiers, and the
  PUBRELs for exchanges already past their PUBREC. Before this, a message
  accepted at QoS 1 or 2 was lost if the connection dropped between delivery and
  its acknowledgement.

### Fixed

- **A packet larger than the client's Maximum Packet Size stranded a send-quota
  slot for the life of the session** ([#13]). It was discarded without being
  completed; [MQTT-3.1.2-25] says the server must behave as if it had finished
  sending. A client with a small Receive Maximum eventually stopped receiving
  anything.
- **Send quota was a bare count while the in-flight set outlives the connection
  that filled it** ([#13]), so a client reconnecting with Receive Maximum 1
  could be handed several concurrent publications after acknowledging what it
  had received before.
- **A withdrawn Packet Identifier could be reallocated to a new message**
  ([#19]). When a resumed session loses a filter, the messages it had earned are
  taken back — but the client was sent those messages and may still owe an
  acknowledgement. `nextID` looked only at the in-flight set, so the identifier
  was free to be reused, and the late acknowledgement would complete whichever
  new message then held it, which was silently never resent. PUBACK and PUBCOMP
  now ignore a withdrawn identifier rather than answering 0x82 Protocol Error,
  which would disconnect a conforming client for the broker's own withdrawal.
- A message already past its PUBREC is **not** withdrawn: the client owns it
  [MQTT-4.3.3-8] and what is outstanding is a PUBREL carrying no payload, so
  withholding it would strand a Packet Identifier to prevent a disclosure that
  already happened.

### Changed

- `Options.Authorizer`'s documentation said a denied subscription is answered
  with 0x87 in the SUBACK. That is no longer true for a whole class of denials:
  on a resume there is no SUBACK and no packet of any kind.
- The README's `Authenticator` example and the runnable `Example_authentication`
  showed an implementation that verifies a principal and accepts any Client
  Identifier — which is exactly how one principal inherits another's session.
  Both now bind the identifier to the principal.

### Documentation

- [`UPSTREAM.md`](UPSTREAM.md) declares what this broker was built against — the
  MQTT v5 specification, the `nats-server` release whose subject mapping is
  copied, and the Paho client the end-to-end tests drive it with — each with the
  date it was last checked ([#12]).

### Known limitation — resolved in 0.3.1

The published container image was not public when 0.3.0 shipped, so `docker run
ghcr.io/taumatix/natsmqtt5:v0.3.0` failed with `denied`. The image existed and every
release pushed it; the package's visibility was settable only by a human in the web
UI, which happened later the same day. Tracked in [#7]. `go get`, `go install` and
building from source were never affected. **The tag still carries this warning in
its own README, because a tag cannot be edited — that is what 0.3.1 is for.**

## [0.2.0] - 2026-09-13

### Added

- **Sessions that outlive the broker process** ([#4]) — `Options.PersistentSessions`
  keeps session state in a JetStream key-value store, so a client's
  subscriptions and QoS state survive a restart and can be resumed against a
  different broker in front of the same NATS server.

### Fixed

- A Clean Start deleting its own freshly written session record ([#5]).
- The reported version came from a hardcoded string rather than the build
  information stamped into the binary ([#3]).

## [0.1.1] - 2026-09-12

### Fixed

- A SUBACK was written before the NATS subscription was established, so it
  promised interest that had not been registered yet, and the response was not
  flushed ([#2]).

## [0.1.0] - 2026-09-12

First release: an MQTT v5 broker that speaks to clients over TCP and carries
their messages on an existing NATS server, using the same topic-to-subject
mapping `nats-server` uses for its own MQTT support.

[#2]: https://github.com/taumatix/natsmqtt5/pull/2
[#3]: https://github.com/taumatix/natsmqtt5/pull/3
[#4]: https://github.com/taumatix/natsmqtt5/pull/4
[#5]: https://github.com/taumatix/natsmqtt5/pull/5
[#7]: https://github.com/taumatix/natsmqtt5/issues/7
[#12]: https://github.com/taumatix/natsmqtt5/pull/12
[#13]: https://github.com/taumatix/natsmqtt5/pull/13
[#19]: https://github.com/taumatix/natsmqtt5/pull/19
[#24]: https://github.com/taumatix/natsmqtt5/pull/24
[Unreleased]: https://github.com/taumatix/natsmqtt5/compare/v0.13.0...HEAD
[0.13.0]: https://github.com/taumatix/natsmqtt5/compare/v0.12.1...v0.13.0
[0.12.1]: https://github.com/taumatix/natsmqtt5/compare/v0.12.0...v0.12.1
[0.12.0]: https://github.com/taumatix/natsmqtt5/compare/v0.11.3...v0.12.0
[0.11.3]: https://github.com/taumatix/natsmqtt5/compare/v0.11.2...v0.11.3
[0.11.2]: https://github.com/taumatix/natsmqtt5/compare/v0.11.1...v0.11.2
[0.11.1]: https://github.com/taumatix/natsmqtt5/compare/v0.11.0...v0.11.1
[0.11.0]: https://github.com/taumatix/natsmqtt5/compare/v0.10.0...v0.11.0
[0.10.0]: https://github.com/taumatix/natsmqtt5/compare/v0.9.1...v0.10.0
[0.9.1]: https://github.com/taumatix/natsmqtt5/compare/v0.9.0...v0.9.1
[0.9.0]: https://github.com/taumatix/natsmqtt5/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/taumatix/natsmqtt5/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/taumatix/natsmqtt5/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/taumatix/natsmqtt5/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/taumatix/natsmqtt5/compare/v0.4.3...v0.5.0
[0.4.3]: https://github.com/taumatix/natsmqtt5/compare/v0.4.2...v0.4.3
[0.4.2]: https://github.com/taumatix/natsmqtt5/compare/v0.4.1...v0.4.2
[0.4.1]: https://github.com/taumatix/natsmqtt5/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/taumatix/natsmqtt5/compare/v0.3.1...v0.4.0
[0.3.1]: https://github.com/taumatix/natsmqtt5/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/taumatix/natsmqtt5/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/taumatix/natsmqtt5/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/taumatix/natsmqtt5/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/taumatix/natsmqtt5/releases/tag/v0.1.0
