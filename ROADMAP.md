# Roadmap

natsmqtt5's goal is MQTT 5 coverage, so this list is ordered by conformance: violated MUST
statements a conforming client hits in normal use, then MUSTs on rare or optional paths, then
SHOULDs, then features the spec defines that the broker does not offer. Operational work that is
not about conformance comes last. The order comes from the statement-by-statement review in
`CONFORMANCE.md`; re-run it when the code or the spec has moved a lot. Each entry says what breaks
today and cites the statements it touches.

**No known MUST violation is open.** The table in `conformance/mqtt5-statements.tsv` has no VIOLATED
or UNSURE row (a test enforces it). Tiers 1 and 2 are windows inside MUSTs the table counts as met:
each needs a particular failure (a broker killed at one instant, a payload near `max_payload`) and
names it. Entries are checked against the code each time the review is re-run (last: 2026-10-09; the Receive Maximum entry, which §4.13.1-1 made a MUST, was done in v0.11.3).

# Tier 1: MUST statements a conforming client hits in normal use

## The replay's time rewind is a bound, not a guarantee

**Today:** a replay reaches back `OfflineQueueRewind` (10 s by default) before the moment a session was released,
on this broker or the one that claims it, and the record carries the delivered ids for that span
(`DeliveredSince`), so a live copy that reached the old broker late, or never, is replayed instead of lost, and
nothing already delivered is sent twice (`never_published_copy_test.go`). Still open: a copy delayed past the
rewind, which is the entry below; and two costs of the bound. Each resume
reads that span of the queue stream (now only the subjects its subscriptions match, below). And the
record holds at most half a value's worth of ids (`trimDelivered`), so a session that delivered more than that
in the span can see a QoS 2 message again (logged as a warning). [MQTT-4.4.0-1], [MQTT-4.3.3-2].

**Measured** (2026-10-10, `TestReplayReadCost`, one embedded NATS server, file storage): an unfiltered resume read
the whole window at about 2.4 us a message (a million messages: 2.4 s), and 50 sessions resuming at once over 100,000
messages took 7.6 s to the slowest, 200 sessions 31.6 s, since each pays the whole read. The replay now reads only the
queue subjects the session's non-shared subscriptions match (`replayFilters`): 50 sessions over 100,000 messages
15 ms to the slowest, one session over a million 3 ms. Still not measured over a clustered, replicated stream.

**Shape:** a session that subscribes during its own replay no longer gets the window's older messages for the new
filter (they were queued before it subscribed; MQTT owes it only what arrives after). Left: a session whose
subscriptions are all shared, or whose filters overlap without one covering the other, still reads the whole window;
the per-session cursor below replaces the span.

## A live copy for a session that has since delivered 8192 more sequences is not noted

**Today:** `lateSeqSafeLocked` refuses to note a copy more than `deliveredSeqWindow` (8192) sequences below
the highest delivered, because the delivered ids that far back are forgotten and a replay from there could
send a QoS 2 message twice [MQTT-4.3.3-2]. The copy is then lost. It needs a copy delayed past 8192 other
deliveries to the same session, so it is rare, but it is the exact case the spec's "MUST resend" covers.

**Shape:** a per-session cursor in the record: the highest sequence below which every matching queued message
has been delivered or acknowledged, advanced only by a stream read, so an unheard copy cannot be skipped over.
A replay starts at the cursor; the delivered ids are only needed above it. That makes the window above moot
too, and it is the redesign this entry and the one before it share. Size L; the entry above is the bounded
version that shipped first.

# Tier 2: MUST statements on rare or optional paths

## Self-fencing is proven for a silent link, not for an isolated NATS server

**Today:** a broker that cannot reach NATS for twice `WillCheckInterval` closes
its clients (`willStore.selfLoop`). `nats_partition_test.go` proves it with a
proxy that stops forwarding between the broker and a single server. Two paths
are reasoned about and not tested: a broker whose server is alive but cut off
from its cluster (only the Will-bucket read in `reachable` notices), and a
fenced broker's own disconnect path, which waits out the Will-store timeouts
and then publishes its own copy of the Will once NATS returns, relying on the
record's compare-and-swap to keep it to one publication.

**Shape:** a test on a three-server cluster that isolates the server one broker
uses, and one that heals a long partition and counts the Will publications.

## The Will index's memory is not measured, and a restart fences every client

**Today:** the Will index (`watchLoop`) holds every record of the bucket in each broker, so its memory grows with
the cluster's connections that have a Will; it has not been measured at a few thousand. A NATS server restart under
two brokers is driven (`will_watch_restart_test.go`): the index recovers and a Will stored afterwards is adopted.
That restart took longer than twice `WillCheckInterval` in the test, so both brokers closed all their clients,
as designed; a real restart of a replicated server may or may not.

**Shape:** measure the index at 5,000 and 50,000 records and, if it matters, hold only the owner and revision there
and read the record when one is to be adopted. Separately, decide whether a restart that finishes inside
`WillCheckInterval` times two should cost no connection at all, which it does not for a broker that sees the
reconnect late.

## Durable Wills on by default

**Today:** `DurableWills` is off, because it stores Will payloads in clear text
in a bucket and needs JetStream. **Shape:** make it default whenever JetStream
is in use, once the partition entry above is done and a Will payload's
visibility has an answer (an Authorizer check on the bucket, or a documented
requirement). A default change needs a CHANGELOG entry that says so.

## DurablePublish does not cover live delivery to subscribers on other brokers

`Options.DurablePublish` makes the PUBACK wait for the queue stream's copy and for a NATS flush of
the live copy. The live copy itself is still core NATS: a subscriber connected to another broker
whose NATS connection drops at the wrong moment misses it, and nothing re-sends it to a session
that was connected (only to one that was away). Closing that needs a stream capturing the live
subjects, which overlaps the retained stream's subjects (JetStream refuses overlap), so it needs
either a subject layout change or per-subscription JetStream consumers. A decision with a
compatibility cost.

## DurablePublish: the retry after a refused flush duplicates the queue copy

The flush failure is driven over a socket now (`durable_flush_fail_test.go`, 2026-10-10): a NATS connection cut
after the queue stream stored the message is a success (the client library buffers the live publish across the
reconnect and the flush completes after it, so the subscriber gets the message once), and NATS staying
unreachable past the 5 s flush timeout is a 0x83 PUBACK. In the second case the queue already holds the copy, so
the client's retry stores a second, with a new message id, and a session that was away sees it twice. QoS 1
allows that; a QoS 2 PUBREC refusal cannot be told apart by the client from "never received", so it also does.
Not tested: that duplicate, and whether a retry could carry the first attempt's id (a `Nats-Msg-Id` on the
queue publish would let JetStream drop it within its duplicate window).
Deriving that id from the MQTT packet id is unsafe: a packet id is reused after its PUBACK, so a later, legitimate
message could be dropped as a duplicate within the window. It would need an id the client controls per message.

## Will Messages are not covered by DurablePublish

A Will published by `publishWillFor` goes through the queue's keep but is not flushed, and a Will
has no client to refuse to. Whether a published Will should wait for the flush is undecided.

# Tier 3: SHOULD statements

## A held shared message: the killed-broker window and the missing cap

A shared member's QoS 1 message now stays unacknowledged in JetStream until its PUBACK
(`held.go`), kept alive by an `InProgress` every third of `sharedAckWait`, and goes back to the
group when the session ends. Two edges remain, neither covered by a test:

- A broker that is killed stops sending the keepalive, so after `sharedAckWait` (30 s) the
  message returns to the group even though the session record may be restored elsewhere with the
  message in flight. The restored session then settles an acknowledgement JetStream no longer
  expects, and the client can see the message twice (the restored copy with Dup=1, a fresh one
  from the other member). That is at-least-once, which QoS 1 allows, but nothing pins it.
- A connected member that never PUBACKs holds the message for as long as the connection lives,
  because the keepalive does not look at progress. A hold cap (a deadline after which the message
  is handed back and the packet identifier withdrawn) would bound it.

## A shared subscription's membership: the edges left

A shared subscription now ends when its last session does (`sharedmembers.go`: a key-value
entry per member session, the consumer deleted by whoever leaves last). What remains:

- A session that is dropped because its persistent record could not be restored (a failed
  restore) does not leave the group, and a session killed with its broker leaves only when its
  entry lapses, `MaxSessionExpiry` plus a minute. The consumer's inactivity threshold is the
  backstop for both, so a group can keep collecting for that long. Making a surviving broker
  sweep entries whose session record is gone would close it.
- The refresh that keeps a live session's entry can write it back just after the session left
  (it read the subscription before the UNSUBSCRIBE and wrote after), leaving a stale entry until it
  lapses, and the group lives that long. A compare-and-set on the instance would close it.
- The race of a last leaver and a joiner is healed by the joiner's puller creating the consumer
  again, on its first failed pull (up to `sharedPullWait` later). A message published in that window
  is not in the new consumer, which starts at the next message.

## Close after a cancelled Serve context can skip the 0x8B DISCONNECT

**Today:** `Broker.Close` following a cancelled `Serve` context can close a connected client without
the `0x8B Server shutting down` DISCONNECT (a race between `conn.serve` and `Broker.drainConns`,
`broker.go`). The client sees a TCP close, which the spec allows; the courtesy is sometimes missing.
**Shape:** make `drainConns` send the DISCONNECT under the connection's write lock whatever state
the serve loop is in, and test it without a sleep.

## Restricting `$` topics by default

`Options.RestrictDollarTopics` (opt in) refuses every `$` topic but
`$share/...`, which is the spec's advice (MQTT-5.0 §4.7.2). Defaulting it on breaks anyone using
`$app/...`, so it waits for the next major version: consider flipping the default there, with a
`MIGRATION.md` and a release that logs a warning the first time a client uses a `$` topic. What
remains of the entry today:

- A subscription a persistent session stored before the option was turned on is resumed, so a
  `$app/...` filter keeps receiving. Dropping it at resume, as a denied filter is, would finish
  the restriction.
- The broker publishes nothing of its own on `$SYS`; the option leaves that namespace unused
  rather than serving it.

## Expired retained messages nobody subscribes to stay in the stream

**Today:** Message Expiry is enforced for retained messages when a subscription looks for them: an
expired one is not sent, and is removed from the in-memory view and deleted from the retained
stream [MQTT-3.3.2-5]. One that no subscription matches after it expired is never looked at and
stays in the stream (and in every broker's map) until a publish to its topic replaces it or a
restart reads it again. It is never delivered, so no statement is violated; it is storage.

**Shape:** a periodic sweep over the retained map, or a per-message TTL on the stream
(`AllowMsgTTL`, nats-server 2.11 and later, with the interval as the `Nats-TTL` header), which would
also need a floor for the servers that do not have it.

## A message's wait is counted from this broker's receipt, and in whole seconds

**Today:** the interval is whole seconds on the wire and the time waited is counted in whole seconds,
rounded down (README, "What it does not do"). A live message's wait starts when this broker's NATS
subscription receives it, not when the publishing broker did, so time spent in NATS between brokers
is not subtracted; a message stored in the queue starts at the JetStream timestamp. An interval of 0
therefore survives a fraction of a second rather than none.

**Shape:** a live copy could carry its publisher's receive time in a header
(`Mqtt5-Published-At`), at the cost of trusting two brokers' clocks to agree. Worth doing only if
someone runs brokers apart enough for the difference to matter.

# Tier 4: features MQTT 5 defines that the broker does not offer

## Enhanced authentication (AUTH packets)

**Today:** a CONNECT with an Authentication Method is refused with
`0x8C Bad authentication method`. That is the conforming answer, but it rules
out SCRAM and Kerberos.

**Shape:** an `Auther` interface driving the AUTH exchange in MQTT-5.0 §4.12,
plus re-authentication on an established connection.

## Outbound topic aliases

**Today:** the broker accepts topic aliases from clients but never sends them,
which is permitted and costs bandwidth on long topic names.

**Shape:** a per-connection alias table bounded by the client's Topic Alias
Maximum, with a least-recently-used policy.

## QoS 2 on shared subscriptions

**Today:** a QoS 2 request on a `$share/` filter is granted QoS 1, reported
honestly in the SUBACK, which [MQTT-3.8.4-7] permits.

**Why it is hard:** a shared subscription is a work queue, and QoS 2 is a
four-step conversation with one specific client. Once a QoS 2 message has been
offered to a group member it must stop being the group's message and become
that member's, durably, so a broker that has never seen the exchange can still
tell "delivered" from "possibly lost". JetStream can record "unacked on
consumer G" but has no concept of "unacked by member B".

**Shape:** a separate arbitration stream with create-only compare-and-set for
ownership, a private per-member copy of the message, and cleanup for every way
the exchange can end — unsubscribe, disconnect, session expiry, takeover.

The shared backlog changed this: a member takes a message off the group's consumer
and the message becomes that session's in-flight state, exactly the handover
described above, and QoS 1 is held in JetStream until its PUBACK (`held.go`). What
QoS 2 still needs is that state surviving a broker restart and a takeover, which
the session record carries for QoS 1 and 2 on a non-shared subscription but not
yet for a message a shared member holds.

## WebSocket transport

**Today:** TCP and TLS only. MQTT over WebSocket is what browser clients and
many hosted deployments need.

**Shape:** an `http.Handler` that upgrades and hands the connection to the same
`conn` state machine, so it is a transport change and nothing more.

## Escaping `*` and `>` in topics

**Today:** a topic containing `*`, `>` or whitespace (MQTT 5 §4.7.3 allows the space character) is refused with
`0x90 Topic Name invalid`, because the `nats-server` subject mapping does not
escape them and letting them through turns an MQTT subscription into a NATS
wildcard.

**Trade-off:** an escaping scheme fixes those topics but breaks subject-level
interoperability with `nats-server` for exactly the topics it escapes. It
should therefore be opt-in — `Options.EscapeNATSWildcards` — and off by
default, so the interop promise holds unless a deployment explicitly trades it
away.

# Operations and robustness (not conformance)

## An Authorizer has no way to say "I could not decide"

**Today:** `Authorize` returns an `error`, and `mayResume` reads any non-nil
error as a denial. On a SUBSCRIBE that conflation is harmless — the client gets
0x87 and retries. On a resume it is not: the filter is torn down, its
unacknowledged messages are withdrawn, and with `PersistentSessions` the reduced
set is written straight back to the durable record. One 500 from a policy
service during a reconnect storm therefore unsubscribes a fleet, permanently,
while every client is told `SessionPresent: true`.

The `Authorizer` doc now says this plainly and tells an implementation to fail
the connection from the `Authenticator` instead. That is guidance, not a
mechanism.

**Shape:** an exported `ErrAuthorizerUnavailable` sentinel. `mayResume` answers
`errors.Is` on it by refusing the CONNECT with `0x83 Implementation specific
error`, leaving the session and the record untouched, so the client retries
rather than silently losing its subscriptions. Fail-closed and non-destructive,
which neither of today's two outcomes is.

`Broker.Reauthorize` (v0.5.0) is a fourth: called during an outage, it removes every
subscription it re-checks from a live connection. Since v0.6.0 it can also be triggered over NATS, on
`_NATSMQTT5.reauthorize.<prefix>`, by anything allowed to publish there.

There is now a third call site that conflates the two: `authoriseWill` answers
any error with `0x87 Not authorized`, so a policy-service outage tells every
client carrying a Will that it is not permitted. That outcome is already
non-destructive — the check runs before the session is taken over — so only
the Reason Code is wrong; the sentinel should map to `0x83` there too.

## Telling a client which of its filters were dropped

**Today:** a filter denied on resume is torn down silently. The CONNACK says
`SessionPresent: true` and the client finds out by not receiving — there is no
per-filter Reason Code in a CONNACK the way there is in a SUBACK, so the refusal
has nowhere to go. A client written to trust Session Present has no way to know
it must re-subscribe.

**Trade-off:** CONNACK User Properties (MQTT-5.0 §3.2.2.3.10) are unconstrained
and would carry it, but a property name this broker invents is not something a
conforming client is required to read, so it informs a client written for this
broker and nobody else. The alternative — refusing the whole connection — turns a
narrowed permission into an outage and loses the filters that are still allowed.

**Shape:** a User Property per dropped filter, and a line in the README saying
it is this broker's own and not part of MQTT v5.

## Two record writes serialise the delivery of every QoS 2 message

**Today:** a QoS 2 message sent to a persistent session is recorded before its PUBLISH and before
its PUBREL ([MQTT-4.3.3-6]), each a key-value write the delivery goroutine waits for. Measured
(`BenchmarkQoS2Delivery`, file-backed JetStream on the same host, one client in lockstep): 0.77 ms a
message against 0.16 ms with the tick only, so one connection's QoS 2 rate is about 1300 a second.
**Shape:** one write for every message already waiting in the connection's queue (group commit),
or a write only when the record would change a successor's answer; measure before and after, and
with `sync_always` on the NATS server, where the writes cost more.

## Spilled in-flight entries are rewritten whole at every changed checkpoint

**Today:** when the in-flight set outgrows a record value, the entries that do not fit are written to the
payload bucket in chunks (`spillRecord`, 2026-10-09; measured at 225 bytes an entry in the record, not the
150 this file said). A checkpoint after any change writes every chunk again and deletes the superseded ones,
so a session holding thousands of unacknowledged messages under a small `max_payload` writes megabytes per
interval. Past the number of chunk names a record holds (about 35 at 4 KiB) the newest entries are still left
out and logged, as before. Orphaned chunks of a live owner wait on the TTL (`sweepBlobs` judges owners, not
keys).

**Shape:** write only the chunks whose content changed (the keys are content-addressed, so this is a
comparison with the last record's `Spill`); let the sweep delete a live owner's keys that its record no
longer names. Low priority: it needs a very small `max_payload` or tens of thousands of messages unacknowledged.

## Other tests still sleep for a drop to be recorded

**Today:** the five tests that slept 200 ms wait on `waitDetached` now (2026-10-09), and so do the eleven
sites that slept 100 to 300 ms after a `drop()` in `restore_inflight_test.go` and `oversize_inflight_test.go`
(2026-10-10). About twenty other sites sleep 100 to 700 ms after a `drop()` before stopping a broker or reading
the record (`restore_nocopy_test.go`, `restored_rewind_test.go`, `spec_integration_test.go`, `killed_*_test.go`,
and the 200 ms sleeps after a publish in `oversize_inflight_test.go`, which wait for a stored payload and not a
drop). Same flake risk, same fix. `TestSmokeSessionMovesBetweenBrokers`
keeps its 200 ms: it is the back-off of a retry loop against brokers in containers, whose logs the test cannot
read. The same change found that `TestALiveCopyThatRacesTheConnectionsEndIsNeverLost` had been publishing its
live copies to the queue's stored-copy subject, which no subscription matches, so it had never exercised the
race; it does now.

## Tombstone accumulation in the retained stream

**Today:** clearing a retained message writes a zero-length message that stays
in the stream as a tombstone. `MaxMsgsPerSubject: 1` keeps it to one per topic,
so it is bounded, but a workload that creates and clears many distinct topics
grows the stream without bound.

**Shape:** a periodic purge of zero-length messages older than some age.

## CI has no vulnerability scan and no linter

**Today:** `.github/workflows/ci.yml` runs `gofmt`, `go vet`, `go build`,
`go test -race` across two Go versions and two operating systems, fuzzes the
decoder and the topic mapping, and smoke-tests the container image. It does not
run `govulncheck`, so a known CVE in a dependency of a library other people
import goes unnoticed, and it does not run `golangci-lint`.

**Why it is last:** neither limits a deployment today, and adding a linter to
nine thousand existing lines will produce a batch of findings that has to be
worked through rather than merged. `govulncheck` is the half worth doing first
and on its own.

## Offline-queue replay without scanning everything published

The replay reads every queued message since the disconnect and matches each in Go. `FilterSubjects`
would let JetStream do it, but it refuses overlapping filters (`a.>` with `a.b`), so the
session's filters have to be reduced to a non-overlapping set first.

## Offline queue for messages published by plain NATS clients

Only messages that pass through a broker are copied to the queue. They also
reach a shared subscription only through its NATS queue group, as QoS 0
messages do, and NATS still picks members whose client is away, whose share is
dropped. Leaving the queue group while the session has no connection would stop
that, as long as the last member leaving does not leave the group with nobody.

## A delivery goroutine stuck in a user callback blocks finish and the catch-up floor

**Today:** `finish` (`conn.go` around 365) and `awayFloor` (`catchup.go` around 185) wait on
`<-c.loopDone` with no timeout, so a delivery goroutine stuck in a user callback (an Authorizer, a
hook) blocks the connection's cleanup, and a resume on the same client, forever. **Shape:** wait
with a bound, log, and carry on as the resume path already does for the old connection's delivery
goroutine. Test: a callback that blocks, then a takeover.

## DurablePublish and the Will bucket: what each guarantees

**Today:** `DurablePublish`'s flush confirms that the NATS server received the live publish, not that
JetStream stored anything; the stored copy is the queue's. The README says so since v0.11.0. The
`_wills` bucket has no TTL, so an orphaned record is never aged out. **Shape:** decide a TTL or a
sweep for the bucket (it must outlast the longest Will Delay Interval), and add a test that proves
the README's statement about what the flush covers.

## A default queue that failed at startup stays off

When the queue is on only by default and its stream cannot be created, the broker logs a warning
and runs without it until it is restarted, while still answering Session Present 1. Nothing
retries, and nothing but the log says it happened. Retrying in the background, and exposing
whether the queue is active (a method on `Broker`, a line in the startup log at info level), would
make the deviation visible and temporary.

## A shared-subscription member pulls one message per round trip

A connected member pulls one message, waits for room under its client's Receive Maximum, then
pulls the next, so a member's rate is bounded by the round trip to JetStream. Pulling a batch the
size of the free quota would lift that without a member holding more than it can send.

## Without the queue, a client that falls behind still loses QoS 1 and 2

With `DisableOfflineQueue` or without JetStream there is no copy to catch up from, so past 2048
waiting messages the broker drops, as before. Back-pressure (blocking the subscription's NATS
handler, which holds back only that client's subscriptions) would move the limit to nats.go's
pending limits rather than remove it. QoS 0, and messages published straight onto NATS, are
dropped the same way with the queue on.

## The slow-reader replay test only runs when asked

`TestAReplayToASlowReaderIsNotCutOffAfterThirtySeconds` takes about 40 seconds (a 400-message
backlog read at 90 ms a message with a Receive Maximum of 1), so it is skipped unless
`NATSMQTT5_SLOW_TESTS` is set, and CI does not set it. Nothing else would notice a deadline coming
back to the resume replay. Run it in a separate CI job (or nightly), or find a seam that makes the
same point faster.

## Statements covered only by unit tests

Found by the 2026-10-08 audit of the offline-queue, session and replay work (v0.9.1 to v0.10.0 and
after) against the rule in [CONTRIBUTING.md](CONTRIBUTING.md): each statement a change touches needs
an integration test over a real socket against a real NATS server. `spec_integration_test.go` closed
most of them; these remain, each with why:

- **A delivered message above the replay's start, produced by the broker.** The guard against
  sending a QoS 2 message twice on a restored rewind [MQTT-4.3.3-2], [MQTT-4.3.3-6] is proved over
  the wire with a record written from the queue's real sequences and ids
  (`TestARestoredRewindDoesNotRedeliverAQoS2MessageItAlreadyDelivered`), because the state needs two
  publishers' copies to arrive out of order. The unit tests cover how the broker arrives at it
  (`restored_away_internal_test.go`). A stress test with many concurrent publishers, asserting no
  QoS 2 message is delivered twice across a drop and a restored resume, would cover it end to end.
- **The cap on stored delivered ids** (`maxStoredDelivered`, 4096): past it a restored rewind can
  repeat a QoS 2 message [MQTT-4.3.3-6]. Unit test only (`TestTheStoredIDsAreBounded`). It needs a
  behind-by-thousands client dropped and restored on another broker.
- **A session record of a version this broker does not know**, which the broker discards and starts
  the session fresh (`sessionstore.go`, `claim`): Session Present must then be 0 [MQTT-3.2.2-3]. Only
  `decodeRecord` is tested. An integration test writes a `Version: 2` record and reconnects.
- **The offline queue on a broker without JetStream** is not there: a QoS 1 message published while a
  session is away is lost although the CONNACK said Session Present 1 [MQTT-3.1.2-23],
  [MQTT-4.5.0-1]. `TestABrokerWithoutJetStreamStillStartsWithNoQueue` pins it as the known
  deviation. Closing it needs a queue that does not depend on JetStream, which is undecided; until
  then the README and CONNACK should not promise more than the test shows.
