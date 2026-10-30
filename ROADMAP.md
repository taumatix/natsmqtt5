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
subscriptions are all shared still reads the whole window, though it delivers nothing from it (a shared message comes from
its backlog): skip the read when no non-shared subscription remains. Filters that overlap are reduced to a set
JetStream accepts (`joinSubjects`, 2026-10-30: crossing filters join into one covering subject, and a broad filter
arriving after narrower ones no longer leaves them overlapping, which made the replay fail); the per-session cursor
below replaces the span.

## A live copy for a session that has since delivered 8192 more sequences is not noted

**Today:** `lateSeqSafeLocked` refuses to note a copy more than `deliveredSeqWindow` (8192) sequences below
the highest delivered, because the delivered ids that far back are forgotten and a replay from there could
send a QoS 2 message twice [MQTT-4.3.3-2]. The copy is then lost. It needs a copy delayed past 8192 other
deliveries to the same session, so it is rare, but it is the exact case the spec's "MUST resend" covers.

**Shape:** a per-session cursor in the record: the highest sequence below which every matching queued message
has been delivered or acknowledged, advanced only by a stream read, so an unheard copy cannot be skipped over.
A replay starts at the cursor; the delivered ids are only needed above it. That makes the window above moot
too, and it is the redesign this entry and the one before it share. Size L; the entry above is the bounded
version that shipped first. Worked out 2026-10-30, to ship in this order, each slice usable alone:

1. **A read-only advance scan.** The cursor can only move past a sequence the stream has been read through, and a
   live connection reads nothing, so a scan runs (from the checkpoint loop, once the delivered ids above the cursor
   pass a threshold, and at release): read the session's `replayFilters` from the cursor to the stream's last
   sequence without delivering, and move the cursor to just below the lowest matching message whose id is not in
   `delivered`, in the connection's queue, or in hand (the copy stored before its live copy was published is in the
   stream already, so a scan that reaches sequence E has seen every copy at or below E). Ids at or below the cursor
   are then dropped. No change to what a client is sent.
2. **`ReplayCursor` in the record** (`omitempty`, so an older record restores without one and an older broker ignores
   it), written with the checkpoint and at release. A record with a cursor replays from `min(cursor, AwayFromSeq,
   AwayLateSeq)`; one without keeps the time rewind. `lateSeqSafeLocked` then accepts any sequence at or above the
   cursor, and `deliveredSeqWindow` and the time rewind's ids go. The cursor never moves backward, and a restored
   cursor above the queue's first sequence after a purge is clamped.
3. **Drop `OfflineQueueRewind` as a correctness bound** (keep it as the start for records with no cursor).

# Tier 2: MUST statements on rare or optional paths

## Self-fencing on a cluster: what is left

**Done (2026-10-23):** `TestClusterWillOfAClientOnAnIsolatedNATSServerIsPublishedOnceAfterItsConnectionCloses` isolates
the server a broker uses from the other two (three real servers in containers, `natscluster_test.go`), watches the
client's socket close before the survivor publishes, heals, and counts one publication. It found that the fenced
broker's own disconnect path published the Will again when the removal of the record timed out; `fire` now retries
the removal until the store answers. Left, precisely, and none of it changes behaviour a user relies on, so it waits for a pass with the cluster harness at
hand: (1) the isolation test does not choose whether the isolated server leads the Will bucket, so it runs one of the
two shapes by chance (pin each with `StopServer` of the leader first); (2) a partition that leaves the broker's server
with a majority and cuts the *broker* off is the proxy test, not a cluster one; (3) a partition longer than `retryFor`
with the broker still cut off waits for the store for as long as it takes, by design, so there is nothing to assert
but that it does not publish.

## Durable Wills on by default

**Today:** `DurableWills` is off. Turning it on by default is a behaviour change for every deployment that does not
set it: the broker would create a bucket, need JetStream where it ran without, hold Will payloads in clear text on
NATS, and delay the Will of a broker that really died by the survivors' wait (about 4.5 times `WillCheckInterval`).
The preconditions this entry named are met: the partition work is tested against a real three-server cluster, and the
payload's visibility has its answer, a documented requirement (the `Authorizer` cannot see the bucket, so the bucket
needs the session bucket's NATS permissions; README, "Durable Will Messages").

**Why it waits:** the flip is a major-version change (or a minor one behind an opt-in the deployment sets), not a pass's
work. **Shape:** at the next major version make it default whenever JetStream is in use and the offline queue or
sessions already need it, with a CHANGELOG entry that says so and `DisableDurableWills` to opt out (additive today).

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

## A Will's queue copy is not retried when the queue refuses it

**Decided (2026-10-30):** a Will is not held for `DurablePublish`'s flush. The flush protects a client by letting the
broker refuse its PUBLISH with 0x83 so it sends again; a Will has no client, and its record is deleted by the
compare-and-swap that elects the publisher, so a second publication on a failed flush would risk announcing it twice.
**Left:** `publishWillFor` logs and carries on when the queue stream refuses the copy, so a session that was away
never gets that Will. With `DurablePublish` set that is the case the option exists to close. **Shape:** retry the keep
a few times with a short gap before publishing live, only when `DurablePublish` is on; test it with a stream that
refuses once (the existing `cutProxy` in `durable_flush_fail_test.go` cuts after the stored ack, the wrong moment, so
it needs a variant that cuts before).

# Tier 3: SHOULD statements

## A held shared message: the killed-broker window and the packet identifier

A shared member's QoS 1 message now stays unacknowledged in JetStream until its PUBACK
(`held.go`), kept alive by an `InProgress` every third of `sharedAckWait`, and goes back to the
group when the session ends or, with `Options.SharedHoldLimit` set, when it has been held past
that limit. Two edges remain:

- A broker that is killed stops sending the keepalive, so after `sharedAckWait` (30 s) the
  message returns to the group even though the session record may be restored elsewhere with the
  message in flight. The restored session then settles an acknowledgement JetStream no longer
  expects, and the client can see the message twice (the restored copy with Dup=1, a fresh one
  from the other member). That is at-least-once, which QoS 1 allows, but nothing pins it.
- A message handed back by `SharedHoldLimit` keeps its packet identifier in flight on the slow
  member, so it occupies a Receive Maximum slot until that member PUBACKs or disconnects, and it
  can be delivered to the group twice. Withdrawing the identifier (so the slot is freed and a late
  PUBACK is ignored) would close that; the limit is also off by default and applies to every
  group alike, so a per-group value is a possible follow-up.

## A shared subscription's membership: the edges left

A shared subscription now ends when its last session does (`sharedmembers.go`: a key-value
entry per member session, the consumer deleted by whoever leaves last). What remains:

- A session that is dropped because its persistent record could not be restored (a failed
  restore) does not leave the group, and a session killed with its broker leaves only when its
  entry lapses, `MaxSessionExpiry` plus a minute. The consumer's inactivity threshold is the
  backstop for both, so a group can keep collecting for that long. Making a surviving broker
  sweep entries whose session record is gone would close it.
- The race of a last leaver and a joiner is healed by the joiner's puller creating the consumer
  again, on its first failed pull (up to `sharedPullWait` later). A message published in that window
  is not in the new consumer, which starts at the next message.

## Restricting `$` topics by default

`Options.RestrictDollarTopics` (opt in) refuses every `$` topic but
`$share/...`, which is the spec's advice (MQTT-5.0 §4.7.2). Defaulting it on breaks anyone using
`$app/...`, so it waits for the next major version. The release that logs a warning the first time a client uses a
`$` topic is done (2026-10-30; once per broker, naming the client, the action and the topic), so what remains is
the flip itself, with a `MIGRATION.md` written in that release: nothing in this roadmap plans a major version, so
none is drafted now. Also still open:

- A session held in a restricted broker's memory cannot hold a `$` filter, so the resume branch that drops one is
  untested by construction. (The in-flight messages such a filter earned are withdrawn on resume, and a test drives
  that, 2026-10-30.)
- The broker publishes nothing of its own on `$SYS`; the option leaves that namespace unused
  rather than serving it.

## A message's wait is counted from this broker's receipt, and in whole seconds

**Today:** the interval is whole seconds on the wire and the time waited is counted in whole seconds,
rounded down (README, "What it does not do"). A live message's wait starts when this broker's NATS
subscription receives it, not when the publishing broker did, so time spent in NATS between brokers
is not subtracted; a message stored in the queue starts at the JetStream timestamp. An interval of 0
therefore survives a fraction of a second rather than none.

**Shape:** a live copy could carry its publisher's receive time in a header
(`Mqtt5-Published-At`), at the cost of trusting two brokers' clocks to agree. Worth doing only if
someone runs brokers apart enough for the difference to matter.

**Looked at 2026-10-30, not built:** it is additive on the wire but not cheap, and the part that makes it unsafe is the
part that costs. (1) It must be an option, off by default: a publisher's clock behind the receiver's makes a message
wait longer than it did and expire early, which breaks [MQTT-3.3.2-5] for a deployment that never asked for the
trade. (2) The receiver has to bound the stamp (ignore one in the future or older than the interval) and `arrivedAt`
in `expiry.go` has to prefer it only for live copies, since a stored copy already counts from the JetStream
timestamp. (3) Every PUBLISH grows by about 30 bytes of header, also for the plain NATS clients that read the subject.
(4) The test that proves it needs a NATS leg that delays a message while both brokers' clocks are right (and one
where they differ), which the embedded server cannot do and the three-server harness (`natscluster_test.go`) could
with `Freeze`/`Thaw` on the server between the brokers. Take it when a deployment with a slow inter-broker link asks.

# Tier 4: features MQTT 5 defines that the broker does not offer

## Enhanced authentication (AUTH packets)

**Today:** a CONNECT with an Authentication Method is refused with
`0x8C Bad authentication method`. That is the conforming answer, but it rules
out SCRAM and Kerberos.

**Shape:** an `Auther` interface driving the AUTH exchange in MQTT-5.0 §4.12,
plus re-authentication on an established connection.

## Outbound topic aliases: on by default, and measured

**Today:** `Options.OutboundTopicAliases` ships off (2026-10-23): a client that offers a Topic Alias Maximum is sent
aliases when it is on. **Left:** the decision to default it on (a behaviour change for clients that advertise a
maximum but handle aliases badly: a major version, with a CHANGELOG line).

**Measured (2026-10-30, `outalias_measure_internal_test.go`, 100,000 PUBLISHes of a skewed mix: 70% 40-byte telemetry
names over 400 devices, 15% 21-byte command names, 15% names of 4-6 bytes; real codec, the broker's own table):**

| Payload | Table 1 | 8 | 32 | 128 | 400 |
|---|---|---|---|---|---|
| 4 B | -1.3% | 19.2% | 38.9% | 55.5% | 66.4% |
| 64 B | -0.5% | 7.9% | 15.9% | 22.7% | 27.2% |
| 512 B | -0.1% | 1.5% | 2.9% | 4.2% | 5.0% |

The saving is the share of bytes on the wire and depends on the client's Topic Alias Maximum and how many distinct
topics it sees, far more than on the threshold. Findings: (1) a reused name saves its length minus 3 and a first use
costs 3, so 8 bytes pays off at the 2nd send, 5 bytes at the 3rd, 4 bytes at the 5th, 3 bytes never: 8 is a safe,
slightly conservative floor and lowering it to 5 gains under 2 points here. (2) With a table smaller than the working
set the LRU thrashes: a maximum of 1 sends more bytes than no aliases, and raising the floor to 32 saved more than 8
did at tables of 8 and 32 (22.5% against 19.2%, 40.3% against 38.9%) because the 21-byte names stopped evicting the
hot 40-byte ones. **Recommendation:** keep `minAliasedTopic` at 8; when it is on by default, send aliases only to a
client whose maximum is at least 8 (below that the table does not hold a working set), or admit a name to the table on
its second sighting instead of its first; neither is done. Not measured: CPU of the encode (aliasing encodes twice
on a miss), and clients in the field that mishandle aliases, which only a trial tells.

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

## Escaping `*` and `>`: a plain broker reads an escaped subject under a wrong name

**Decided 2026-10-30: no migration tool.** Turning `EscapeNATSWildcards` on needs no rewrite (a topic the plain
mapping could store has the same subject under the escaping one; `TestTurningEscapingOnKeeps...` drives a retained
and a queued message across the switch). Turning it off has nothing to rewrite *to*: a topic containing `*`, `>` or
whitespace has no plain subject, so a tool could only delete, which is what the README's procedure (clear those
retained messages, let the queue age out) does with the tools clients already have.

**What is left:** with escaping off, the broker does not refuse a subject it stored under escaping: it decodes
`ret.a/sb` as the topic `ret/asb` (`TestTurningEscapingOffReads...`), delivering a message under a name nobody
published. **Shape:** the plain `subjectToName` treats a `/` followed by one of the escape letters as an escape
sequence it does not know, so the retained and queue readers can skip such a subject (and say so in a warning)
instead of serving it; a few lines in the decode path plus a test that the wrong name is no longer served.

# Operations and robustness (not conformance)

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

## Other tests still sleep for a checkpoint or a publish

**Today:** every test that slept 100 to 300 ms after a client's connection ended now waits on `waitDetached` (or
`waitDetachedTimes` for a client id that connects more than once), which returns when the broker has logged the
disconnect, after it released the session (2026-10-11: `restore_nocopy_test.go`, `spec_integration_test.go`,
`restored_rewind_test.go`). What remains are sleeps that wait for something else. The checkpoint-with-a-message-in-flight sleeps are gone
(2026-10-11: `awaitCheckpointedInflight` polls the stored record for the packet id, with its PUBREL out for QoS 2, in
`killed_broker_test.go`, `killed_inflight_test.go`, `restore_nocopy_test.go`). Left: the 700 ms in
`killed_broker_test.go` for a checkpoint of the position after the last exchange (the record has no field that says
"this exchange is included", so it needs a checkpoint counter or a field), the 1 s in `restore_nocopy_test.go` and
`oversize_inflight_test.go` that ages a message past the checkpoint skew (a clock condition, not an event), 1500 ms for a
session's expiry to pass (`spec_integration_test.go`), (2026-10-13: the two in `oversize_inflight_test.go` that followed a dropped connection now use `waitDetached`; the "a session that is only away keeps its payload" check is a negative one, so it proves the payload survived the release, not an unbounded wait. Also 2026-10-13: the 100 ms in
`restored_rewind_test.go` and the four 200 ms after a retained QoS 1 publish in `oversize_inflight_test.go` are gone; the
broker stores a retained message before it acknowledges, so the PUBACK already was the condition.) Same flake risk; each needs a condition to wait on rather than a drop log. `TestSmokeSessionMovesBetweenBrokers` keeps its 200 ms: it is the back-off of a retry loop against brokers in
containers, whose logs the test cannot read.

## Retained view: a stale entry that cancels an unseen one

**Done:** a broker cut off from NATS past `RetainedTombstoneTTL` missed a clear whose tombstone was then purged and
kept serving the cleared message (reproduced about once in two runs by
`TestClusterARetainedClearMissedDuringAnOutageIsAppliedAfterTheTombstoneIsPurged`). The sweep now compares the local
view's size with the stream's message count and, on a surplus, drops entries the stream no longer holds. The purge also
treats the server's error 10043 ("sequence N not found") as already purged.

**Left:** the check is a count, so one stale entry cancelled by one message the consumer has not delivered yet goes
unnoticed until the consumer catches up; and a stale entry whose topic was since retained again by another broker is
corrected only when the consumer delivers that message. A reconnect-triggered full rebuild would close both, at the
price of a replay per reconnect; build it if a user reports a stale retained message.

## The weekly scans fail where no pass reads them

**Done:** CI runs `staticcheck` (pinned, 2026-10-11) beside `go vet`; its four findings were fixed. A broader
`golangci-lint` set (errcheck, gosec) was not added and would need its own findings worked through.

**2026-10-30:** `CONTRIBUTING.md` ("Scheduled runs nobody is watching") now says what the maintenance pass reads: the
latest `schedule` run of `Vulnerability scan` through `/actions/runs?event=schedule`. A workflow cannot open an issue
on failure with the `contents: read` it has, and a token with `issues: write` is a permission change, so there is no
in-repo alert. The `golangci-lint` set was tried as `errcheck` and `gosec` run bare: 24 unchecked errors (mostly
`Close` and `fmt.Fprint*`, which golangci-lint's defaults exclude, but also `b.nc.Drain()` in `broker.go` and
`c.nc.Close()` in `conn.go`) and 21 `gosec` findings (15 G115 integer conversions, 5 G104, 1 G118). It would need a
`.golangci.yml` that sets those exclusions and a pass over `broker.go`, `conn.go` and the G115 conversions; it was not
added because that touches files other work is changing.

**Today:** the weekly `Vulnerability scan` run is the only thing that fails when a vulnerability is published against
unchanged code, and nothing routes a failed scheduled run to a pass; the maintenance pass should read it, and
`check-repo-health.py` counts only the `CI` workflow (the same gap as `claude-agent-sdk-go`'s `Upstream surface`). A push
that is red because of a standard-library finding is also not this module's to fix: it clears when the runner's Go does.

## Offline queue for messages published by plain NATS clients

Only messages that pass through a broker are copied to the queue. They also
reach a shared subscription only through its NATS queue group, as QoS 0
messages do, and NATS still picks members whose client is away, whose share is
dropped. Leaving the queue group while the session has no connection would stop
that, as long as the last member leaving does not leave the group with nobody.

## `finish` and `awayFloor` wait for the delivery goroutine with no bound

**Today:** `finish` (`conn.go`) and `awayFloor` (`catchup.go`) wait on `<-c.loopDone` with no timeout, so a
delivery goroutine that never returns blocks the connection's cleanup, and with it a resume on the same client
(`awaitPredecessor` waits for the reader and `awaitPredecessorLoop` bounds its own wait at `predecessorWait`,
10 s, so the resume path itself does not hang on the goroutine). Checked 2026-10-23: the premise this entry used to
have, a user callback holding the goroutine, is false. The delivery goroutine (`deliverLoop`, `subscribe.go`) calls
neither the `Authenticator` nor the `Authorizer`; those run on the reader and in `Reauthorize`. What can hold it is
a socket write (`c.close()` interrupts that), the receive-quota wait, and a NATS or JetStream call made without a
deadline. A bounded wait in `finish` is not a fix by itself: `awayFloor` and `detach` read `pending` and the
in-flight bookkeeping after `loopDone` closes precisely because the goroutine owns them and has stopped, so reading
them after a timeout would race.

**Shape:** audit each JetStream and NATS call on the delivery path (the QoS 2 record writes, the stream reads, the
shared pull) for a context with a deadline, and give any that lacks one the same bound the Will bucket has; then a
test that blocks one such call (a frozen NATS server from the cluster harness, `Freeze`) and shows the connection
still finishes. Only if a call cannot be bounded, hand the goroutine's state over through a mutex so a bounded wait
is safe.

## A default queue that failed at startup: what a session away in the gap is owed

**Done (2026-10-11):** the broker retries a default queue it could not create, with a doubling wait from 5 s to a
minute, logs at info when it comes up, and `Broker.OfflineQueueActive` says whether it is on. Left: a session that
went away and came back while the queue was off still gets Session Present 1 with nothing replayed, as before; the
reply could say 0 (and drop the session) while the queue is known to be off. That changes what a resuming client is
told, so it wants a decision, not a guess. Also untested: shared-subscription hold-back and membership refresh after a
late queue (they start with it, `queueretry.go`, but only the replay is driven by a test).

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
  (`restored_away_internal_test.go`). A stress test with six concurrent publishers across a drop and a move to another
  broker (2026-10-13, `restored_concurrent_qos2_test.go`) finds every message once and none sent as new twice, but it
  is weak evidence for this guard: with the delivered-id test removed from `offline.go` and `catchup.go` it still passes,
  because the replay in that run starts past what was delivered. The out-of-order state still needs a publisher stopped
  between its queue write and its live publish, which the stress test does not produce.
- **The cap on stored delivered ids** (`maxStoredDelivered`, 4096): past it a restored rewind repeats
  a QoS 2 message [MQTT-4.3.3-6]. Driven end to end with the bound lowered to 20
  (`TestAClientAheadOfMoreThanTheStoredIDBoundIsWarnedAndRepeatedOnlyPastIt`: a warning on release,
  and exactly the ids past the bound repeated), because 4100 deliveries take minutes under `-race`;
  that the bound is 4096 is `TestTheStoredIDsAreBounded`'s alone. The test shows which ids the cap
  drops: the highest-sequence ones, which are the newest and the likeliest to be redelivered. Keeping
  the newest instead would trade one repeat for another; the real fix is a record that is not
  bounded by a count, which needs the ids held outside the session value.
- **The offline queue on a broker without JetStream** is not there: a QoS 1 message published while a
  session is away is lost although the CONNACK said Session Present 1 [MQTT-3.1.2-23],
  [MQTT-4.5.0-1]. `TestABrokerWithoutJetStreamStillStartsWithNoQueue` pins it as the known
  deviation. Closing it needs a queue that does not depend on JetStream, which is undecided; until
  then the README and CONNACK should not promise more than the test shows.
