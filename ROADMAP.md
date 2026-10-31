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

No entry is open. The last two, "The replay's time rewind is a bound, not a guarantee" and "A live copy for a
session that has since delivered 8192 more sequences is not noted", were closed by the replay cursor (2026-10-31,
`cursor.go`, `replay_cursor_test.go`): a resume replays from a per-session cursor, so a message whose live copy never
came, or came after the release below any number of later sequences, is sent however long ago it was stored, on any
broker and after a kill, and nothing completed is sent again [MQTT-4.4.0-1], [MQTT-4.3.3-2], [MQTT-4.3.3-6]. What it
left is below.

# Tier 2: MUST statements on rare or optional paths

## The replay cursor: what falls back to the time rewind, and what waits for a resume

**Today:** a session whose record or memory has a replay cursor (`cursor.go`) is replayed from it; one without is
replayed from `OfflineQueueRewind` before its release, as before, and keeps the rewind's limits: a message whose live
copy never came and that is older than the rewind is lost, and a late copy more than `deliveredSeqWindow` (8192)
sequences below what the session has delivered is not noted [MQTT-4.4.0-1]. A session is without a cursor when:

1. it was restored from a record written before the cursor existed, until its first replay reads the queue to its
   end (one absence, once, after the upgrade);
2. it left more than 1024 holes (`maxReplayHoles`), or more than 16384 ids above the cursor (`cursorIDBound`): scans
   that keep failing, or a connection whose lowest unsent message never moves, pin the cursor while ids pile up;
3. its record could not carry every id above the cursor (more than `maxStoredDelivered`, 4096, or trimmed to fit a
   value): the record is written without one, so a broker that restores it falls back, though the broker that held
   it does not;
4. a SUBSCRIBE could not read the queue's last sequence, or its filter cannot be narrowed to queue subjects (a scan
   then never runs).

A cursor that was dropped comes back only at the next replay that reads to the end, or at a SUBSCRIBE by a session with
no other non-shared subscription. **Shape (not worked out):** start a cursor at the next scan instead (the ids above the lowest unsent
message are complete once a replay or catch-up has run), so (2) and (4) recover while connected; let the record carry
ids past 4096 outside the value (as the in-flight spill does), which closes (3) and the cap below.

**Also:** a hole is sent only at the next resume. A message whose live copy never comes stays owed while the client is
connected, which [MQTT-4.4.0-1] allows (it requires the resend on reconnect) but a client cannot tell from a lost one.
A scan that finds a hole older than a few seconds could hand it to the catch-up path. And each SUBSCRIBE now costs a
stream info request and each 1024 deliveries a filtered read; neither is measured over a clustered, replicated stream.

## The replay's read: shared-only sessions and a clustered stream

**Measured** (2026-10-10, `TestReplayReadCost`, one embedded NATS server, file storage): an unfiltered resume read
the whole window at about 2.4 us a message (a million messages: 2.4 s), and 50 sessions resuming at once over 100,000
messages took 7.6 s to the slowest, 200 sessions 31.6 s, since each pays the whole read. The replay reads only the
queue subjects the session's non-shared subscriptions match (`replayFilters`): 50 sessions over 100,000 messages
15 ms to the slowest, one session over a million 3 ms. Filters that overlap are reduced to a set JetStream accepts
(`joinSubjects`). Left: a session whose subscriptions are all shared still reads the whole span, though it delivers
nothing from it (a shared message comes from its backlog): skip the read when no non-shared subscription remains. A
replay from a hole reads from that hole to the stream's end, so a hole kept for hours costs that span once per resume;
reading the holes by sequence (`GetMsg`) and the rest from the cursor would not. Not measured over a clustered,
replicated stream.

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

## DurablePublish: a retry after a refused flush leaves a second queue copy

**Decided (2026-10-30):** left as it is, and tested (`TestDurablePublishRetryAfterARefusedFlushLeavesASecondQueueCopy`,
`durable_flush_fail_test.go`): after the 0x83 the queue stream holds the first copy, the client's retry stores a second,
and a session that was away is offered the message twice. QoS 1 allows that ([MQTT-4.3.2-2], at least once). A
`Nats-Msg-Id` on the queue publish would drop the second copy within JetStream's duplicate window, but the id has to
come from the client and be unique per message, never from the packet id (reused after its PUBACK, so a later, different
message would be dropped as the duplicate). MQTT 5 has no such field; the only carrier is a User Property whose name
and rules the broker would invent, and a client that reuses a value silently loses a message, which is worse than the
duplicate it prevents. Not implemented.
**Left, only if a deployment asks:** an opt-in `Options.DedupeUserProperty` naming the User Property to use as the
`Nats-Msg-Id`, off by default and additive. It would need a documented contract (unique per message, longer than the
stream's `Duplicates` window) and a test that a reused value is dropped and an absent one is not.

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

`SharedHoldLimit` is off by default and applies to every group alike; a per-group value (which
would be an `Options` map or a callback) is a possible follow-up, with no known need yet. A
message it hands back is withdrawn from the slow member (no resend on reconnect, a late PUBACK
ignored), but the member's Receive Maximum slot stays taken until that PUBACK or a reconnect:
the client still counts the message against its own Receive Maximum, so freeing the slot earlier
would let the broker exceed it [MQTT-3.3.4-9].

The killed-broker window is pinned (`shared_killed_test.go`, `NATSMQTT5_SLOW_TESTS=1`): the
message is seen twice at worst, never lost.

## A shared subscription's membership: the edges left

A shared subscription ends when its last session does (`sharedmembers.go`). What remains:

- A killed broker's session still holds its group until the session store reclaims the record
  (`MaxSessionExpiry` after the broker's last write, and only if the broker does not answer), at
  which point a surviving broker sweeps the entry. A record that a broker still serves
  (`Attached`) and that does not list the group is not swept, because a SUBSCRIBE writes the entry
  before the record; a released one that does not list it is.
- The race of a last leaver and a joiner is pinned (`shared_race_test.go`): the joiner's puller
  creates the consumer again on its first failed pull, which comes at once. A message published
  between the consumer's deletion and that re-creation is not in the new consumer, which starts
  at the next message. Closing it would need a deletion that a joining member can veto; JetStream
  has no conditional consumer delete.

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
described above, and QoS 1 is held in JetStream until its PUBACK (`held.go`). The
separate arbitration stream is therefore not needed; what is needed is below, in
the order that ships something each time. SUBACK keeps granting QoS 1 on `$share/`
(`subscribe.go`, the cap before `granted`) until the last piece is done, so the
first two change nothing a client sees.

### 1. Done (2026-10-31): a held QoS 2 message survives a killed broker and a takeover

`Options.SharedQoS2` (`-shared-qos2`, off) lifts the cap; with it a message held
before its PUBREC is resent with DUP and its original identifier after a SIGKILL
and after a takeover, and the group's other member is not given it
(`shared_qos2_test.go`). Found on the way: a broker that lost a session to another
broker handed its held messages back to the group (`discardMoved` now keeps them).
Not covered: a message pulled but not yet checkpointed when its broker dies goes
back to the group after AckWait, which a client that already answered it cannot
tell from a new message (entry 2 narrows it: after PUBREC the record names the PUBREL).

### 2. Done (2026-10-31): the ownership point, acknowledged to the group at PUBREC

Once the record naming the PUBREL is written (`handlePubrec`, `settleOwned`) the
JetStream message is acknowledged, `handBackHeld` skips an entry that
`awaitingPubcomp`, and a restore acknowledges a `Rel` entry still naming one. Tested
past PUBREC with an unsubscribe, a clean start, a session expiry, a returning client
and a SIGKILLed broker; before PUBREC the end of the session still gives the message
to another member. An UNSUBSCRIBE does not hand back a message already sent (QoS 1
either): it stays the session's until the exchange ends. Not covered: a broker
killed after the PUBREC is read and before its record is written (the client has
answered, the group gets the message after AckWait).

### 3. Grant QoS 2 on `$share/`

Make `Options.SharedQoS2` (added by entry 1, off) the default once a release has
carried 1 and 2, and document it in the README. The SUBACK test moves from QoS 1 to the
requested QoS ([MQTT-3.8.4-7]); with the switch off a QoS 2 request is still
granted QoS 1.

# Operations and robustness (not conformance)

## A lone QoS 2 message still waits for two record writes

**Done (2026-10-30):** messages waiting together share the record written before their PUBLISH
(`deliverBurst`, `subscribe.go`) and PUBRELs queued while a record is written share the next one
(`queuePubrel`, `publish.go`), so a burst of 32 costs 0.42 ms a message instead of 1.43
(`BenchmarkQoS2DeliveryBurst/Durable`, a subscriber with a persistent session, one client; the
non-persistent ceiling is 0.09 ms). Killed-broker tests cover the burst (`killed_qos2_burst_test.go`).
**Left:** one client in lockstep (`BenchmarkQoS2Delivery`) has nothing to share: two writes a message,
about 2 ms on a loaded host. That needs a write that is not waited for, which [MQTT-4.3.3-6] does not
allow, or a faster store. Not measured with `sync_always` on the NATS server.

## Spilled in-flight entries: a message acknowledged at the front rewrites every chunk

**Done (2026-10-30):** a checkpoint names a chunk whose content did not change without writing it
(`spillRecordReusing`; a message appended writes the last chunk only, 1 of 10 in the test), until
its payloads near the bucket's TTL, when all are rewritten. The sweep deletes the unnamed payloads
(and counts the chunks as named) of an owner whose record was released.
**Left:** chunks are cut by size from the oldest entry, so an acknowledgement at the front shifts every
boundary and every chunk changes; cutting at boundaries that do not move (a fixed number of identifiers
per chunk) would fix that. The sweep still leaves an attached owner's unnamed payloads to the TTL: its
record can be a step behind what the broker holds (a claimed record names nothing until the first
checkpoint), so judging them needs the record's age and the broker's own liveness.

## Other tests still sleep for a checkpoint or a publish

**Today:** every test that slept 100 to 300 ms after a client's connection ended now waits on `waitDetached` (or
`waitDetachedTimes` for a client id that connects more than once), which returns when the broker has logged the
disconnect, after it released the session (2026-10-11: `restore_nocopy_test.go`, `spec_integration_test.go`,
`restored_rewind_test.go`). What remains are sleeps that wait for something else. The checkpoint-with-a-message-in-flight sleeps are gone
(2026-10-11: `awaitCheckpointedInflight` polls the stored record for the packet id, with its PUBREL out for QoS 2, in
`killed_broker_test.go`, `killed_inflight_test.go`, `restore_nocopy_test.go`). (2026-10-30: the 700 ms in
`killed_broker_test.go` is gone; the test waits for the record to list both messages as delivered with nothing in flight.)
Left: the 1 s in `restore_nocopy_test.go` and
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

**Done (2026-10-31):** after the NATS client has reconnected, the sweep checks every entry of the view against the stream
whatever the counts say: an entry whose message is gone and whose topic is cleared is dropped, and one whose topic was
retained again is replaced by the stream's message without waiting for the consumer. It costs one lookup per entry per
reconnect, not a replay, so it was not made conditional. The three-server test above still passes;
`TestARetainedViewIsVerifiedAfterANATSReconnectEvenWhenTheCountsAgree` puts the view in the state a count cannot see.

**Left:** a stale entry is still served until the next sweep after the reconnect (up to `SessionSweepInterval`), and a
reconnect the broker's NATS client did not notice (a partition shorter than its ping interval) triggers nothing.

## The weekly scans fail where no pass reads them

**Done:** CI runs `staticcheck` and `golangci-lint` (errcheck, gosec; both pinned, config in `.golangci.yml`) beside
`go vet`; every finding was fixed or carries a one-line reason, and `CONTRIBUTING.md` ("Scheduled runs nobody is
watching") says what the maintenance pass reads: the latest `schedule` run of `Vulnerability scan` through
`/actions/runs?event=schedule`. A workflow cannot open an issue on failure with the `contents: read` it has, and a token
with `issues: write` is a permission change, so there is no in-repo alert.

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

Evaluated and not built: the queue group is a property of a NATS subscription on one broker,
while "nobody left" is a fact about every broker's members, which only the membership bucket
knows and which changes between the check and the unsubscribe. A broker that leaves on the
strength of a stale read, or a member that returns while the group is empty, loses live QoS 0
messages that today are at worst dropped for an absent client. Doing it safely needs the
decision to be made against the bucket with a revision check and a re-subscribe on the
reconnect before the CONNACK, which is a design of its own.

## A frozen NATS server still costs a connection one deadline per payload it stores

**Done (2026-10-30):** audited every JetStream and NATS call the delivery goroutine and the connection's end make.
Each has a deadline (`sessionOpTimeout`, `natsFlushTimeout`) except the stream reads (`streamContext`,
`catchup.go`), which go at the client's pace on purpose and are cancelled by `c.done`, which `finish` closes before it
waits. What was unbounded was the total: `stashBlobs`, `reapBlobs` and `commitSpill` took `sessionOpTimeout` per
payload, so a connection holding many in-flight payloads waited that many times 5 s on a NATS server that answered
nothing. They now share one `sessionOpTimeout`; past it the rest fail at once and are counted as unrecorded, as a failed
store always was. `TestClusterAConnectionWithManyUnacknowledgedMessagesStillEndsWhenItsNATSServerFreezes` freezes the
broker's server with eight 200 KiB unacknowledged messages and shows the broker ends within 40 s.

**Left:** that test passes on the old code too (about 9 s either way): with the default queue the in-flight entries
name queue copies and nothing is stashed, so it covers the end of a connection on a frozen server, not the aggregate
bound. A test that stashes many payloads needs a broker without the queue and entries larger than the record allows.

## A default queue that failed at startup: shared subscriptions made before it came up

**Done (2026-10-11):** the broker retries a default queue it could not create, with a doubling wait from 5 s to a
minute, logs at info when it comes up, and `Broker.OfflineQueueActive` says whether it is on.
**Done (2026-10-30):** what the retry starts (`startQueueLoops`) is driven by tests on a broker whose queue came up late
(`queueretry_shared_test.go`): the hold-back loop hands a held shared message to another member after
`SharedHoldLimit`, and the membership refresh keeps the entry of a member that subscribed after the queue came up.
**Decided (2026-10-30):** a session that went away and came back while the queue was off still gets Session Present 1
with nothing replayed, and CONNACK is not changed. [MQTT-3.2.2-3] says the Server MUST set Session Present to 1 when it
has stored Session State for the client, and it has: the subscriptions and the in-flight messages. What the gap lost is
messages that were never kept, the deviation the start-up warning already names; answering 0 would make the client
resubscribe and would drop the state that is there, to report a loss it could not repair.
**Left:** a shared subscription made while the queue was off is a plain queue-group subscription with no backlog
consumer and no membership entry, and the refresh skips it (`sub.backlog == nil`), so it stays that way until its
session subscribes again: the group's backlog and the last-member rules do not cover it, and nothing says so but this
entry. **Shape:** when the retry succeeds, give each live shared subscription its backlog consumer and its entry (the
path SUBSCRIBE takes), with a test that publishes to a member that subscribed before the queue came up and then leaves.

## Without the queue, a client that falls behind still loses QoS 1 and 2

**Decided (2026-10-30):** documented (README, "What it does not do"), and back-pressure is not built. With
`DisableOfflineQueue` or without JetStream there is no copy to catch up from, so past 2048 waiting messages the broker
drops, as before, and says so in its log. Blocking the subscription's NATS handler holds back only that client's
subscriptions, but it does not slow the publishers: messages pile up in nats.go's per-subscription pending buffer,
which has a count and byte limit, and past it nats.go drops them and reports a slow consumer on the broker's one
shared connection. That moves the loss to a place with less information and a wider blast radius, and the memory
held until then is bounded only by those pending limits. Reasoned from nats.go's documented behaviour, not measured.
QoS 0, and messages published straight onto NATS, are dropped the same way with the queue on.
**Left, only if a deployment without JetStream asks:** an opt-in per-connection limit that closes a connection that is
2048 messages behind and leaves the session to resume, which loses nothing it had not already lost and makes the
failure visible to the client. It would need a test with a client that stops reading.

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
  because the replay in that run starts past what was delivered. The out-of-order state is now produced by the broker
  (2026-10-30, `out_of_order_queue_test.go`): `SetPublishQueuedGate` (test-only) holds one publisher between its queue
  copy and its live publish while another is delivered, and the detached session's restored rewind sends the held one
  and not the delivered one again. With the delivered-id test removed from `offline.go` and `catchup.go` that test fails
  (it is sent again), so it proves the guard.
- **The cap on stored delivered ids** (`maxStoredDelivered`, 4096): past it a restored rewind repeats
  a QoS 2 message [MQTT-4.3.3-6]. Driven end to end with the bound lowered to 20
  (`TestAClientAheadOfMoreThanTheStoredIDBoundIsWarnedAndRepeatedOnlyPastIt`: a warning on release,
  and exactly the ids past the bound repeated), because 4100 deliveries take minutes under `-race`;
  that the bound is 4096 is `TestTheStoredIDsAreBounded`'s alone. The test shows which ids the cap
  drops: the highest-sequence ones, which are the newest and the likeliest to be redelivered. Keeping
  the newest instead would trade one repeat for another; the real fix is a record that is not
  bounded by a count, which needs the ids held outside the session value.
- **The replay cursor's bounds and its race with a live copy.** That a cursor is dropped past 1024 holes or 16384
  ids above it, that a record which left ids out carries none, and that a live copy arriving below the cursor while a
  scan moves it becomes a hole (`noteLiveCopy` under `cursorMu`) are unit tests (`cursor_internal_test.go`); the
  wire tests (`replay_cursor_test.go`) reach the cursor's normal paths, not those. Driving the bounds over a socket
  needs thousands of deliveries or a hook to lower them, and the race a publisher stopped at the instant a scan
  applies.
- **The offline queue on a broker without JetStream** is not there: a QoS 1 message published while a
  session is away is lost although the CONNACK said Session Present 1 [MQTT-3.1.2-23],
  [MQTT-4.5.0-1]. `TestABrokerWithoutJetStreamStillStartsWithNoQueue` pins it as the known
  deviation. Closing it needs a queue that does not depend on JetStream, which is undecided; until
  then the README and CONNACK should not promise more than the test shows.
