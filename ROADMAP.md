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
Every entry opens with a **Blocked by:** line saying what stops it: *nothing* (a pass can take it), *a decision*,
*a release*, *a major version*, *a cross-broker design*, or *real deployments* (evidence only a user's workload gives).
The whole file was audited against `main` on 2026-10-31.

# Tier 1: MUST statements a conforming client hits in normal use

No entry is open. The replay cursor (2026-10-31, `cursor.go`, `replay_cursor_test.go`) closed the last two: a
resume replays from a per-session cursor, so a message whose live copy never came, or came late, is sent however long
ago it was stored, and nothing completed is sent again [MQTT-4.4.0-1], [MQTT-4.3.3-2], [MQTT-4.3.3-6]. What it left is
in Tier 2.

# Tier 2: MUST statements on rare or optional paths

## The replay cursor: what falls back to the time rewind, and what waits for a resume

**Blocked by:** nothing measured remains; what is left is design (the cursor in the record).

**Measured (2026-11-01, `TestClusterReplayCursorCostPerSubscribeAndPerDelivery_MQTT_4_4_0_1`, three `nats:2-alpine`
servers in Apple containers, an R3 queue):** a SUBSCRIBE costs one stream info request (3-7 ms). 4496 deliveries
behind a hole take 2.5 s with the scans on (33 consumer creates, 25 deletes, 8 stream infos, about 8 creates per 1024
deliveries) and 3.0 s with them off, so the scans are not visible in the delivery rate. The 400 ids past the record's
bound are written as 2 chunks, at the checkpoint and the release. The cost is bounded and small; no change made.

**Today:** a session whose record or memory has a replay cursor (`cursor.go`) is replayed from it; one without is
replayed from `OfflineQueueRewind` before its release, as before, and keeps the rewind's limits: a message whose live
copy never came and that is older than the rewind is lost, and a late copy more than `deliveredSeqWindow` (8192)
sequences below what the session has delivered is not noted [MQTT-4.4.0-1]. A session is without a cursor when:

1. it was restored from a record written before the cursor existed, until its first replay reads the queue to its
   end (one absence, once, after the upgrade);
2. it left more than 1024 holes (`maxReplayHoles`), or more than 16384 ids above the cursor (`cursorIDBound`);
3. a SUBSCRIBE could not read the queue's last sequence, or its filter cannot be narrowed to queue subjects.

**Recovered while connected (2026-10-31):** after 1024 deliveries without a cursor (`cursorScanAfter`), a restart scan
(`restartCursor`) reads the session's subjects from where a rewind started now would (`OfflineQueueRewind` ago,
the cursor's *base*) and sets the cursor with the holes it found. So (1), (2) and the unreadable-sequence half of (3)
recover without a resume. It promises only what the rewind would, never more, because it cannot know what was owed
before it. Three interleavings defeat a naive restart (the cursor at the stream's end, or from the scan with no base),
and each has a guard. The randomized model in `cursor_model_internal_test.go` (400 seeds) fails with either of the
first two removed; it keeps every id, so the third is a unit test:

- *A live copy below base, late.* Message f, stored more than a rewind ago, has its live copy arrive after the
  restart, and is unsent when the connection ends; the rewind starts at f (`away.fromSeq`) and sends h, f < h < base,
  whose live copy never came. The restart never read h. Guard: a live or late copy below base drops the cursor
  (`addHoleLocked`), and the rewind runs.
- *The connection holds a message below base* when the restart applies: the same, with f already in hand. Guard: the
  restart refuses.
- *A scan longer than the rewind*: the ids delivered from base up are kept for twice the rewind, and are what tell a
  hole from a delivered message; past that a completed QoS 2 message reads as a hole [MQTT-4.3.3-2]. Guard: refused.

A subscription made or replaced while any scan reads (the plain scan included, since 2026-10-31) makes it throw its
result away: the new filter may be owed what the scan read past without looking.

**Left:** a restarted cursor stays in memory, not in the record, because a broker of an earlier version would read it
without its base; a session restored elsewhere falls back to the rewind until its replay starts a cursor. Taking it
into the record needs a field older brokers refuse, or a major version. The other half of (3), a filter that cannot be narrowed, still never scans. Each restart
scan reads up to one rewind of the session's subjects per 1024 deliveries while it keeps being refused (a session
with more than 1024 holes inside one rewind); not measured.

**Recorded with the cursor (2026-10-31):** a record carries the delivered ids past `maxStoredDelivered` (4096) in
chunks in the payload bucket (`DeliveredSpill`, `spillDelivered`), up to 65536 more, so a session with more ids than
that above its cursor keeps the cursor in its record (`replay_cursor_spill_test.go`: 4396 ids, a restart, the message
older than the rewind sent and none repeated). The chunks are cut where an id's hash says (`endsChunk`), so ids trimmed
off the front or appended leave the others' keys alone and only the changed ones are written; they share one
deadline (`sessionOpTimeout`); ids not written (the deadline, a failed put, over 1024 chunks) take the cursor out of
the record as before, and a chunk that cannot be read on restore does the same (the replay uses the time rewind and
may repeat). They go with the session: Clean Start and the sweep delete the owner's payloads, and a record that stops
naming a chunk has it deleted. A record with 4096 ids or fewer is written as it was, so an older broker reads it, and
an older broker reading one with chunks restores the ids in the value and ignores the key. Left from it: the
checkpoint and late-copy writes are tested for the chunks (`spill_delivered_internal_test.go`) but only the release is
driven over a socket and a killed broker's checkpoint with chunks is not; a restore reads the chunks one after the
other (up to 128 gets at the cursor's bound), and a checkpoint sorts every id each time, neither measured; a failed
chunk write is retried at the next change, not by itself.

**Also:** a hole is sent only at the next resume. A message whose live copy never comes stays owed while the client is
connected, which [MQTT-4.4.0-1] allows (it requires the resend on reconnect) but a client cannot tell from a lost one.
A scan that finds a hole older than a few seconds could hand it to the catch-up path. A resume whose queue read fails
(an error other than an empty or unanswered fetch, which are read again since 2026-10-31) logs "what it held is lost
for this session" and sends nothing until the next resume, though the cursor did not move; retrying the read while
the client stays connected would close that. Each SUBSCRIBE costs a
stream info request and each 1024 deliveries a filtered read; both measured above.

## DurablePublish does not cover live delivery to subscribers on other brokers

**Blocked by:** real deployments, for what is left (items 2 and 3 below).

**Reproduced and closed for routes (2026-11-01):** with the subscriber's server cut off from its peers (`Isolate`) and
both brokers still connected to their own servers, a QoS 1 message published meanwhile was stored in the R3 queue and
never reached the subscriber. The event exists after all: the server sends its clients an INFO with the cluster's
client URLs when a route goes or comes, which nats.go keeps as `DiscoveredServers()`. `routewatch.go` polls that set
(every 200 ms) under `ReconnectCatchUp`; a change is treated as a drop (live copies held), and once it has stayed the
same for 1.5 s the sessions catch up from the queue as after a reconnect. Test:
`TestClusterReconnectCatchUpSendsWhatARouteLostWhileBothBrokersStayConnected`. What it does not see: a flap shorter
than the poll, a route the server has not noticed is dead (its pings take minutes unless `ping_interval` is set), and
a broker that sets `nats.IgnoreDiscoveredServers`. An isolation longer than the broker's will-bucket check (about
14.5 s) closes its clients regardless.

**Design (2026-10-31).** No new stream and no subject change: the queue stream already holds a copy of every QoS 1
and 2 message published through a broker, stored before its live copy goes out (`offlineQueue.keep`), and a
connected session can already read it in order (the catch-up of `catchup.go`). So the smallest closure is to make a
connected session catch up when its broker knows it missed live copies. The one event a broker sees is its own NATS
connection dropping (nats.go's disconnect and reconnect handlers, chained onto any the caller set). A stream over
the live subjects would overlap `$retained.>` (JetStream refuses it) and needs a subject layout change; per-session
JetStream consumers cost a consumer per subscription. Neither is needed for the drop.

**Done (2026-10-31):** `Options.ReconnectCatchUp` (`-reconnect-catch-up`), off by default, an error with
`DisableOfflineQueue` (`reconnect.go`). On the drop every connected session holds its live QoS 1 and 2 copies back;
on the reconnect each reads the queue from `OfflineQueueRewind` before the drop, or from just after the newest
sequence the broker had seen if lower (a server dead behind an open socket is noticed only when the pings fail), no
lower than it can read without sending a delivered id twice (`session.catchUpFloor`), and only what was stored after
each subscription (`subscription.since`). A catch-up read cut by a second drop starts again after it rather than
closing the client. Tests: `reconnect_catchup_test.go` (two brokers on one NATS, a proxy that drops or stalls one
broker's connection: QoS 1 in order, QoS 2 once, repeated drops, a stall noticed past the rewind, a subscription
made after the message) and `TestClusterReconnectCatchUpSendsWhatWasPublishedWhileTheSubscribersServerWasDown`
(three servers, the subscriber's server killed and restarted).

**Left:**

1. Route flaps: see above for what is covered and not.
2. A live copy nats.go drops on the broker's side as a slow consumer (its pending limits): reported through the
   connection's error handler, not handled. The same catch-up from the stream would serve, started from the
   subscription's last delivered sequence.
3. A message the drop lost that a cursor scan made a hole before the reconnect's catch-up started (the catch-up
   starts at the cursor): sent on the session's next resume, not at once.

## DurablePublish: a retry after a refused flush leaves a second queue copy

**Done (2026-10-31):** `Options.DurablePublishDedupe` (off, needs `DurablePublish`). The 2026-10-30 decision rejected
deriving the id from the packet id, because a packet id is reused after its PUBACK and a later message would be
dropped, and asked for a client-chosen id. The broker can keep the packet id safe without the client: the id of a
publish is held per session only until it is acknowledged, and a PUBLISH reuses it only when it is marked DUP and
has the same topic and payload. So an acknowledged publish is never matched, and a different message under a refused
packet id is queued (`durable_dedupe_test.go`, over a socket with real NATS and the flush refused by `cutProxy`; the
default-off behaviour stays pinned by `TestDurablePublishRetryAfterARefusedFlushLeavesASecondQueueCopy`).

**Left:** off by default, so the duplicate stays for anyone who does not set it. A retry after the stream's duplicate
window (2 minutes unless the operator raised `Duplicates`), after a Clean Start that dropped the session, or from a
client that does not set DUP is still queued twice, and the live copy is sent again either way, so a connected
subscriber can see it twice (QoS 1 allows both). A client-chosen User Property id is still the only way to cover
those, and is not built.

# Tier 3: SHOULD statements

## A held shared message: the killed-broker window and the packet identifier

**Blocked by:** the Receive Maximum slot is held by [MQTT-3.3.4-9], which no change releases earlier.

**Decided (2026-11-01):** no per-group `SharedHoldLimit`. Measured over a three-server cluster with R3 streams
(`TestClusterAMessageHeldPastTheSharedHoldLimitGoesToAMemberOnAnotherServer`: a member on one server holds a message,
a member on another gets it handed back after the limit, the late PUBACK is accepted, nothing arrives twice), the one
limit behaves the same as on one server, and no deployment has asked for a different value per group. Revisit if one does.

`SharedHoldLimit` is off by default and applies to every group alike; a per-group value (which
would be an `Options` map or a callback) is a possible follow-up, with no known need yet. A
message it hands back is withdrawn from the slow member (no resend on reconnect, a late PUBACK
ignored), but the member's Receive Maximum slot stays taken until that PUBACK or a reconnect:
the client still counts the message against its own Receive Maximum, so freeing the slot earlier
would let the broker exceed it [MQTT-3.3.4-9].

The killed-broker window is pinned (`shared_killed_test.go`, `NATSMQTT5_SLOW_TESTS=1`): the
message is seen twice at worst, never lost.

## A shared subscription's membership: the edges left

**Blocked by:** nothing buildable. The last-leaver race is closed between brokers of this release (2026-11-01, below); what
is left is a documented limit (README, Known limits): JetStream has no conditional consumer delete, so a broker of an
earlier release, which takes no lock, keeps the old window. A NATS release with a delete that checks the consumer is
unchanged would lift it for every broker; until then, upgrading the fleet does. The killed-broker edge waits on the
session store's own reclaim and needs nothing built.

A shared subscription ends when its last session does (`sharedmembers.go`). What remains:

- A killed broker's session still holds its group until the session store reclaims the record
  (`MaxSessionExpiry` after the broker's last write, and only if the broker does not answer), at
  which point a surviving broker sweeps the entry. A record that a broker still serves
  (`Attached`) and that does not list the group is not swept, because a SUBSCRIBE writes the entry
  before the record; a released one that does not list it is.
- **Done (2026-11-01):** the race of a last leaver and a joiner is closed by a lock. JetStream has no conditional consumer
  delete, so a join (`joinGroup`) and the last leave's recheck-then-delete (`endGroup`) take a per-group lock, a create-only
  key `lock.<hash>` in the `<StreamPrefix>_share_members` bucket (taken over by compare-and-set after `shareLockStale`, 10 s,
  if its holder was killed). The leaver rechecks the members under the lock, so it finds a joiner's entry and keeps the
  consumer; a joiner arriving while the leaver deletes waits for the delete and then makes a consumer that begins before
  its SUBACK. Tested with the leaver held at both points, embedded and on a three-server cluster in containers
  (`shared_race_test.go`, `TestClusterALastLeaverDoesNotDeleteTheBacklogOfAJoinerOnAnotherBroker_MQTT_5_0_4_8_2`); both
  fail without the lock. **Limit, not buildable:** the lock binds only brokers that take it. An earlier release's broker
  deletes unconditionally, so in a mixed fleet its leave can still take a joiner's backlog (a message published after the
  joiner's SUBACK and before the delete is lost to that group). The consumer cannot be fenced from the server side: a
  delete cannot name a revision or a consumer-info version, and an update cannot stand in for a delete. Lifted by upgrading
  every broker, or by NATS offering a delete conditional on the consumer being unchanged (none up to nats-server 2.15.2).

## A message's wait is counted from this broker's receipt, and in whole seconds

**Done (opt-in):** `Options.PublisherClockExpiry` (`-publisher-clock-expiry`, off by default) makes the publishing
broker stamp `Mqtt5-Published-At` (nanoseconds) on a PUBLISH that carries a Message Expiry Interval, and a receiving
broker counts a live copy's wait from it (`arrivedAt`, `expiry.go`). A stamp later than the receiver's clock counts as
no wait, so a clock running ahead cannot shorten the interval; a stored copy still counts from the JetStream timestamp.
The trust caveat is on the option and in the README: a publisher's clock behind the receiver's, or a forged stamp from
anything that can publish on the subjects, makes a message expire early. Tested over real sockets and real NATS
(`publisher_clock_expiry_test.go`).

**Left:** the case the option exists for, a slow inter-broker leg, is tested with the stamp set directly and not with a
delayed NATS leg: the embedded server cannot delay a message, and the three-server harness (`natscluster_test.go`)
could with `Freeze`/`Thaw` on the server between the brokers. The interval stays whole seconds on the wire, so an
interval of 0 still survives a fraction of a second. Neither limits a deployment that has asked.

# Tier 4: features MQTT 5 defines that the broker does not offer

## Outbound topic aliases: what is still not measured

**Blocked by:** a trial. A full table now admits a name on its second sighting (CHANGELOG [Unreleased]; measured
2026-11-01 in `outalias_measure_internal_test.go`: the skewed mix at a table of 8 saves 32% against 19%, a round robin
over 40 names 6% against 0%, no mix or table sends more bytes than before). A seen-once set larger than 32 names did
worse on the skewed mix (256: 26%).

- [ ] CPU of the encode (aliasing encodes twice on a miss), and clients in the field that mishandle aliases, which only a
  trial tells; v1.0.0 users who see one pin `PinOutboundTopicAliases: Ptr(false)`.

## QoS 2 on shared subscriptions: a failed record write still lets the PUBLISH go out

**Blocked by:** nothing.

Granted by default since v2.0.0 (`Options.SharedQoS2`, `PinSharedQoS2: Ptr(false)` for the v1 QoS 1 grant; MIGRATION.md,
"v1 → v2"). A member owns a QoS 2 message from the moment it is sent: it survives a killed broker, a takeover and a client
away past the group's AckWait (`shared_qos2_test.go`, `shared_qos2_killed_test.go`, `shared_qos2_late_test.go`), and the
group is told at PUBREC. Every duplicate window that blocked the default is closed (2026-10-31 and 2026-11-01); what is left
is below.

1. **Closed (2026-11-01).** The late backlog (*A default queue that failed at startup*): a message the group acknowledged
   from the backlog before its live copy reached this broker was delivered live as well. A group consumer now carries the
   stream sequence it started after in its metadata (`natsmqtt5-start-seq`, written by whichever broker makes it, while
   nothing has been delivered from it, so the value is exact), and a late backlog skips the live copy of everything
   above it (`TestALateBacklogSkipsTheLiveCopyOfAMessageTheGroupAlreadyAcknowledged_MQTT_4_3_3_2_MQTT_4_8_2_5`, which
   failed before: 3 to 7 of ten came twice). Additive, and no `DeliverPolicy` change, so a mixed-version fleet still
   works; what an earlier release's broker does is update the consumer and drop the metadata, and then the check falls
   back to the acknowledgement floor, with the window as it was. A consumer that has already delivered when a new
   broker first sees it has no start either.
2. **Closed within AckWait.** A message pulled and sent but not yet checkpointed: for QoS 2 there is no such moment,
   since `deliver` and `deliverBurst` (`subscribe.go`) write the record before the PUBLISH. Killed the instant the
   client has the PUBLISH, the survivor resends it with DUP and the original identifier and the other member is never
   given it (`TestAKilledBrokersSharedQoS2MessageJustSentIsCompletedByTheSameClient_MQTT_4_8_2_4_MQTT_4_8_2_5`, which
   fails with the write before the PUBLISH removed).
3. **Closed within AckWait.** A broker killed after reading the PUBREC and before writing the record that names the
   PUBREL: the record still names the PUBLISH, the survivor holds the message again and resends it with DUP, the client
   answers PUBREC again ([MQTT-4.3.3-10]) and the exchange completes; the other member is never given it
   (`TestAKilledBrokersSharedQoS2MessageAnsweredBeforeItsRecordIsCompletedByTheSameClient_MQTT_4_8_2_4_MQTT_4_8_2_5`, with
   `SetPubrecGate` holding the PUBREC before `awaitPubcomp`; it fails if a restore hands a pre-PUBREC entry back).
4. **Closed (2026-11-01).** A killed broker's held messages were held again only when the session was restored, so a
   client away longer than the AckWait (30 s) found its message given to another member and was then sent it as well.
   `Broker.holdOrphans` (`held.go`, started with `SharedQoS2` and persistent sessions) reads the session records every
   AckWait/3 and says `+WPI` for the Ack subjects that a record Attached to a broker that does not answer a liveness
   request names (`sessionStore.orphanedHolds`), without restoring the session. It lasts as long as the record, or
   `SharedHoldLimit` from the first sighting, after which the message goes back to the group once
   (`TestAKilledBrokersSharedQoS2MessageIsHeldForAClientAwayPastTheAckWait_MQTT_4_8_2_4_MQTT_4_8_2_5`, which failed
   before: the other member was given the message). The scan reads every session record each interval, which is why it
   runs only with `SharedQoS2`.
5. **Closed for a held shared message (2026-11-01); the rest is a documented limit.** `checkpointNow` dropped the record
   write's error, so a PUBLISH could go out after a failed write. For a message held in a shared group (`recordQoS2`,
   `subscribe.go`) the delivery now fails when the write does: the message is not sent but handed back to the group, the
   connection ends, and a PUBREL whose record cannot be written is held (`drainPubrels`, `publish.go`) until the client is
   back (`shared_qos2_record_test.go`, both fail without the change; the write is made to fail with
   `SetSessionSaveFault`). **Limit, not buildable:** with `SessionCheckpointInterval` negative, or without
   `PersistentSessions`, no record is written, and a message that is not held in a shared group is still sent after a
   failed write (it is the session's alone, so there is nowhere better for it to go). A broker killed then loses the
   in-flight state, and the client may get the message twice, as for QoS 2 without a shared subscription; the README says
   so. Lifted by turning the checkpoint on (not a NATS limit).

- With `SessionCheckpointInterval` negative, or without `PersistentSessions`, no record is written, and a message that is not held in a shared group is still sent after a failed write; a broker killed then loses the in-flight state and the client may get the message twice, as for QoS 2 without a shared subscription (README, Known limits). Lifted by turning the checkpoint on.
- `Broker.holdOrphans` (`held.go`) reads every session record each AckWait/3 for as long as `PersistentSessions` is on,
  which is now every deployment that uses them with a shared subscription or not. **Left:** measure what the scan costs on
  a bucket of 10 000 sessions and read only the records that name a held message, if the number is not small.
- **Mixed fleets.** A group consumer made by a v1.0.x broker carries no start sequence, so the late-backlog check falls back
  to the acknowledgement floor (the window as it was before v1.1.0). Nothing to do but upgrade the fleet.

# Operations and robustness (not conformance)

## A lone QoS 2 message still waits for two record writes

**Limit, not buildable (audited 2026-11-01; README, Known limits):** the record must say PUBLISH before the PUBLISH and
PUBREL before the PUBREL [MQTT-4.3.3-6], and a client in lockstep has no second message to share a write with, so the two
writes per message are the floor. JetStream has no cheaper durable write than a key-value update, and a write that is not
waited for would let a broker killed after the client has the PUBLISH lose the exchange's state. Lifted only by a faster
store (a quicker disk or `sync_interval` on the NATS server; the broker cannot choose either).

**Done (2026-10-30):** messages waiting together share the record written before their PUBLISH
(`deliverBurst`, `subscribe.go`) and PUBRELs queued while a record is written share the next one
(`queuePubrel`, `publish.go`), so a burst of 32 costs 0.42 ms a message instead of 1.43
(`BenchmarkQoS2DeliveryBurst/Durable`, a subscriber with a persistent session, one client; the
non-persistent ceiling is 0.09 ms). Killed-broker tests cover the burst (`killed_qos2_burst_test.go`).
**Left:** one client in lockstep (`BenchmarkQoS2Delivery`) has nothing to share: two writes a message,
about 2 ms on a loaded host. That needs a write that is not waited for, which [MQTT-4.3.3-6] does not
allow, or a faster store. Not measured with `sync_always` on the NATS server.

A session attached to a broker that died still has its unnamed payloads judged only once its record is reclaimed, or
they age out.

## Retained view: a stale entry that cancels an unseen one

**Done:** a broker cut off past `RetainedTombstoneTTL` no longer serves a retained message another broker cleared
(the sweep compares the view with the stream's count; the purge treats error 10043 as already purged), and after a NATS
reconnect every entry of the view is checked against the stream whatever the counts say
(`TestARetainedViewIsVerifiedAfterANATSReconnectEvenWhenTheCountsAgree`). One lookup per entry per reconnect.
**Done (2026-10-31):** that check runs when the NATS client reports the reconnect (a status listener, so an
application's own reconnect handler on `Options.Conn` is untouched), not at the next sweep up to
`SessionSweepInterval` later (`TestARetainedViewIsVerifiedAsSoonAsTheNATSClientReconnects_MQTT_3_3_1_6`). A check that
fails because JetStream is not answering yet is left to the next sweep.

**Done (opt-in):** `Options.RetainedVerifyInterval` (`-retained-verify-interval`, off by default) makes the sweep check
every entry against the stream at least that often, which finds a stale entry a partition too short for the NATS client to
notice left behind (`TestARetainedViewIsVerifiedOnATimerWhenAskedTo_MQTT_3_3_1_6`). One lookup per entry per interval.

**Left:** without the option, such an entry is served until a count disagrees; no event reveals the partition, so the
default cannot do better without paying a lookup per entry on a timer for every deployment.

## Offline queue for messages published by plain NATS clients

**Blocked by:** nothing for what is left; the two items below are a measurement and a choice.

Only messages that pass through a broker are copied to the queue. Since v2.0.0 that includes a plain publish on a live
subject whose `Mqtt5-Qos` header is 1 or 2 (`Options.QueuePlainPublishes`, `plainpublish.go`; MIGRATION.md): MQTT
subscriptions ignore the first copy, one broker chosen by a queue group on `<prefix>.>` stores the queue copy and
publishes it again with its id and sequence, and `plain_publish_test.go` drives a plain nats.go client against real
brokers (an away session is replayed it once, a connected one is delivered it once, a shared group's work reaches only
the connected member, QoS 0 and header-less messages are untouched, the pin restores v1). What it costs: a plain NATS
**subscriber** sees such a message twice (the original, and the copy that has an id), and every broker gets one more
subscription on `<prefix>.>`.

**Design (2026-10-31).** A copy stored *after* a plain publish cannot be told apart from it: the live copy has
reached subscribers with no id, so every session that had it would get the queue copy again on a resume or a
catch-up, and a shared group would get it twice (queue group and backlog). The broker has to come first. The ingest
subject (below) puts it first by asking the publisher to use another subject; v2.0.0 puts it first on the live subject
itself, by having MQTT subscriptions hold back an id-less message that asked for a queue.

**Done (2026-10-31):** `Options.NATSIngest` (`-nats-ingest`, `ingest.go`): topic and QoS checked as a PUBLISH's, a `$`
topic always refused (it would write `$retained` or `$queue`), retained stored, queue copy kept, live copy published, `DurablePublish`'s flush honoured; a request is answered
with `Mqtt5-Msg-Id` and `Mqtt5-Queue-Seq`, or `Mqtt5-Ingest-Error`. Tests (`ingest_test.go`, two brokers on one
NATS, a plain nats.go client): an away session is replayed, a connected one gets it once with its properties, 30
shared jobs all reach the connected member with the other away, QoS 0 is live only, a retained message is stored,
refusals are answered, and nothing answers with the option off.

**Left:**

1. **The duplicate a plain subscriber sees, and the price of the subscription (2026-11-01).** Removing the duplicate
   needs MQTT subscribers on a subject only brokers publish to (`<prefix>.$live.<subject>`), with brokers publishing
   a copy for plain subscribers too; that is another wire change for every broker and plain subscriber, so it waits for
   v3 or a decision that v2.x may break it. Measure first what the `<prefix>.>` queue-group subscription costs a broker
   on a busy subject space (it sees each message once per cluster and discards it), and whether a per-prefix filter
   (a configured set of subjects to queue) is worth the option.
2. **A refused plain publish is dropped for MQTT sessions** with a WARN and no answer to the publisher; `NATSIngest`
   answers. A plain publish that fails queueing (stream missing, JetStream down) is lost to MQTT sessions although plain
   subscribers had it. Consider delivering the live copy anyway, as v1 did, when only the queue copy failed.
3. **Mixed v1/v2 fleets deliver a plain QoS 1 publish twice** to a v1 broker's sessions, and cannot see each
   other's liveness pings (the ping subject moved out from under the prefix so that the plain-publish
   subscription does not swallow "no responders"). A v2 broker could also answer the old subject and ping both, to
   allow a rolling upgrade; today stopping the fleet is the documented rule.
4. A QoS 0 or unqueued share NATS hands to a member whose client is away is dropped. MQTT allows that
   for QoS 0; it matters only for a plain publish with `Mqtt5-Qos` 1 or 2, which `NATSIngest` now serves. Leaving
   the queue group while away was evaluated and not built:

The queue group is a property of a NATS subscription on one broker,
while "nobody left" is a fact about every broker's members, which only the membership bucket
knows and which changes between the check and the unsubscribe. A broker that leaves on the
strength of a stale read, or a member that returns while the group is empty, loses live QoS 0
messages that today are at worst dropped for an absent client. Doing it safely needs the
decision to be made against the bucket with a revision check and a re-subscribe on the
reconnect before the CONNACK, which is a design of its own.

## A default queue that failed at startup: a duplicate across brokers

**Blocked by:** nothing buildable; what is left is a documented limit (README, Known limits). A consumer's `DeliverPolicy`
cannot be changed once it exists, so a consumer cannot be created by start sequence without breaking the SUBSCRIBE of a
broker that did not; the recorded start (below) is the additive substitute. It is missing for a consumer made by an
earlier release, for one that had already delivered when a broker of this release first saw it, and, until a broker of this
release next joins, for one whose metadata an earlier release's update dropped. Lifted by upgrading the fleet together, or
by a NATS release that lets a consumer's start be changed.

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
**Done (2026-10-31):** when the retry succeeds, each live shared subscription made while the queue was off gets its
membership entry and its group's backlog consumer (`Broker.giveBacklogs`, and the refresh for one a SUBSCRIBE made
just as the queue came up), and its connection pulls from it. `TestASharedSubscriptionMadeWhileTheQueueWasOffKeepsItsShareWhileAway`
publishes to the group while that member is away and sees the message wait for it; before, it was lost.
**Done (2026-10-31):** the backlog given late skips the live copy of every message above the group consumer's
acknowledgement floor as it was given, not above the queue's last sequence. A consumer that members on another broker
had made holds every message stored since; their live copies, reaching this broker after the backlog was given, were
delivered too, so the group got them twice, which a shared QoS 2 grant (`SharedQoS2`) forbids [MQTT-4.3.3-2]
(`TestALateBacklogSkipsTheLiveCopyOfAMessageTheGroupsConsumerAlreadyHeld_MQTT_4_3_3_2`; before, about half of ten held
messages came twice). JetStream starts a new consumer's floor one below its first sequence, so a message stored before
the consumer is still delivered live (`TestALateBacklogStillDeliversTheLiveCopyOfAMessageStoredBeforeItsConsumer_MQTT_4_5_0_1_MQTT_4_3_3_2`).
**Done (2026-11-01):** a message the group acknowledged from the backlog before its live copy reached this broker, which
the floor could not tell from one stored before the consumer, is skipped: the group consumer records, in its metadata,
the stream sequence it started after (exact while nothing has been delivered from it), and the late backlog uses that
instead of the floor (`TestALateBacklogSkipsTheLiveCopyOfAMessageTheGroupAlreadyAcknowledged_MQTT_4_3_3_2_MQTT_4_8_2_5`).
The consumer's `DeliverPolicy` is untouched, so this is additive. **Left:** a consumer with no recorded start (made
by an earlier release, or whose metadata an earlier release's update dropped) is judged by the floor as before.

**Done (2026-11-01):** the start survives an earlier release's update. It is also kept in `start.<hash>` in the
`<StreamPrefix>_share_members` bucket (written with the consumer's creation time, so a copy left by a group that was
deleted and made again is not mistaken for the new one's), and a broker of this release joining the group puts it back
into the consumer's metadata (`TestALateBacklogStillSkipsAnAcknowledgedMessageAfterAnEarlierReleaseDroppedTheConsumersStart_MQTT_4_3_3_2_MQTT_4_8_2_5`,
and on a cluster `TestClusterAJoiningBrokerRestoresTheConsumerStartAnEarlierReleaseDropped_MQTT_4_3_3_2`; the first
fails without the restore). **Limit, not buildable:** a consumer that had already delivered when this release first saw it
has a start that can no longer be known exactly, so it keeps the floor.

**Looked at 2026-10-31, not built, and why it is not an opt-in either:** the fix is to create the group consumer by
start sequence, and there is no additive form of it. (1) A consumer's `DeliverPolicy` cannot be changed on an existing
one, so a broker given the option would fail to update a consumer that a broker without it created `DeliverNew` (and the
reverse), which breaks the SUBSCRIBE of the second broker to join the group: an opt-in that is safe only when every
broker has it set is a flag day, not an option, and the break is in the shared SUBSCRIBE and not in a feature that stays
off. (2) The window is narrower than the cost of closing it: it needs a live copy slower than a member's whole
PUBLISH-to-acknowledgement exchange, in the moment the backlog is given after a default queue failed at startup. (3) A
check by message id against the group's acknowledged set, instead of the floor, would be additive but is a lookup on
every live shared delivery in that window and needs the acknowledged set the consumer does not expose per message.
**Still blocked by:** a release that ships the consumer-by-start-sequence handling readable by old brokers (an updated
consumer config older brokers accept), or a fleet that can be upgraded together; neither is something a pass can decide.

## Without the queue, a client that falls behind still loses QoS 1 and 2

**Done (2026-10-31):** `Options.CloseBehindClients` (off by default): with no queue, the connection of a client that
lost a QoS 1 or 2 message because it was 2048 behind is closed with DISCONNECT 0x97 and its session is kept
(`close_behind_test.go`: a client that reads nothing while 2300 messages are published through a real broker and NATS,
the DISCONNECT, the session resumed, a default broker that stays connected, a client that keeps up, and one with the
queue on that is never closed). The DISCONNECT is written with a short write deadline because the delivery loop holds
the write lock while it is blocked on this very client.
**Decided (2026-10-30), unchanged:** back-pressure (blocking the subscription's NATS handler) is not built. It holds
back only that client's subscriptions, does not slow the publishers, and moves the loss into nats.go's pending buffer
and a slow-consumer report on the broker's one shared connection.
**Done (2026-11-01):** the loss of a message published while a session is away is closed without JetStream:
`Options.AwayBufferMessages` (`-away-buffer-messages`) holds QoS 1 and 2 messages in memory per session and
delivers them in order on resume, and a session that overflows is ended so the CONNACK says Session Present 0
rather than 1 with a gap (`away_buffer_test.go`). Chosen over making Session Present 0 for every no-queue
session because [MQTT-3.1.2-23] requires the session state to be kept for the Session Expiry Interval.
It is memory only: a restart, or another broker taking the session over, loses it (the CONNACK says 0).
**Left:** the message that triggered the close and the ones queued behind it are still lost; without a queue they have
no copy. The limit is the fixed 2048 (`deliveryQueueDepth`), not an option; a deployment that wants a lower or higher
one should say so. QoS 0, and messages published straight onto NATS, are dropped without closing, as with the queue on.

## `Broker.Close` with a client that never reads (found via #216's report, fixed 2026-11-01)

#216's author saw a `Broker.Close` hang. Reproduced under cluster conditions
(`TestClusterCloseReturnsWhenItsNATSServerFreezesDiesOrIsCutOff`: the broker's server frozen, killed and cut off): it
hung, with every fault, because of a client that never reads its socket. Its delivery loop sat inside `Write` holding
the connection's write lock, and `Close` -> `drainConns` -> `conn.shutdown` -> the DISCONNECT write waited for that
lock for ever. `shutdown` now sets a write deadline (`closeBehindGrace`, 2 s) first, which fails the stuck write.
Regression test without a cluster: `TestCloseReturnsWhileAClientThatNeverReadsHasAFullSocket` (fails without the fix).
With the fix all three faults return (about 35 s for the test; the NATS drain's own timeout is 30 s). The shared pull
goroutines (`pullWG`) did not hang with NATS frozen, killed or cut off while one was inside a fetch.

**Left:** `Close` racing the `catchUpAfterReconnect` goroutine, which is not in `b.wg`; no hang seen.

## Statements covered only by unit tests

**Blocked by:** nothing; driving the cursor's bounds over a socket needs a hook to lower them.

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
- **The cap on stored delivered ids** (4096 in the value, 65536 more in chunks since 2026-10-31, hooks
  `SetStoredDeliveredBound`, which also removes the chunks, and `SetInlineDeliveredBound`): past it a restored rewind
  repeats a QoS 2 message [MQTT-4.3.3-6]. Driven end to end with the bound lowered to 20
  (`TestAClientAheadOfMoreThanTheStoredIDBoundIsWarnedAndRepeatedOnlyPastIt`: a warning on release,
  and exactly the ids past the bound repeated), because 4100 deliveries take minutes under `-race`;
  that the bound is 4096 is `TestTheStoredIDsAreBounded`'s alone; 4396 ids in the value and chunks, with the real
  bound, are `TestMoreThan4096IDsAboveTheCursorSurviveARestartWithTheCursor_MQTT_4_4_0_1`. The test shows which ids the cap
  drops: the highest-sequence ones, which are the newest and the likeliest to be redelivered. Keeping
  the newest instead would trade one repeat for another. The chunks moved the cap from 4096 to 69632 ids; a session
  without a cursor that delivers more within the rewind still repeats.
- **The replay cursor's bounds and its race with a live copy: driven end to end since 2026-11-01 except one instant**
  (`cursor_bounds_test.go`, bounds lowered by `SetCursorIDBound` and `SetStoredDeliveredBound`). A cursor dropped past
  its bound of ids above it (16384 in production, 5 in the test) leaves the connected session without one, and a
  message whose live copy never came and is older than the rewind is then not replayed, where with the bound at its
  default it is [MQTT-4.4.0-1]; a record that left ids out carries no cursor, logs the warning, and the broker that
  restores it replays from the time rewind. That the bound is 16384 is `TestTooManyIDsAboveTheCursorDropIt`'s alone.
  The hole bound, the restart after it and a live copy below a restarted cursor's base were driven already
  (`replay_cursor_test.go`). A live copy arriving while a scan applies is driven too (`cursor_scan_live_test.go`, a hook in `scanOnce` holds the scan between its read and its apply). Still unit tests: the restart's other refusals (a message held below base, a scan longer than the rewind, a
  subscription made while it read) are unit tests and the randomized model (`cursor_model_internal_test.go`), which
  drives the session's code with scans applied after other events, not the broker's goroutines. The new test found a
  data race in `noteLiveCopy` (it read the cursor under `cursorMu` while `dropCursorLocked` wrote it under `mu`), fixed
  with it.
- **Driven end to end since 2026-11-01: the offline queue on a broker without JetStream.** A QoS 1 message
  published while a session is away is held in memory, bounded by `AwayBufferMessages`, and delivered on resume;
  an overflowing session is ended and answered with Session Present 0 [MQTT-3.1.2-23], [MQTT-4.5.0-1]
  (`away_buffer_test.go`, over real sockets against a NATS server with no JetStream). Not covered end to end: a
  race between a message arriving and the connection ending (unit-level reasoning in `away.go`, `-race` runs).
- **Driven end to end since 2026-10-31 (these were unit-tested only).** `NATSIngest` when the queue copy cannot be
  stored (stream deleted: error reply, no sequence, nothing live) and when `DurablePublish`'s flush cannot be confirmed
  (the broker's NATS cut after the queue copy: error reply once NATS is back, copy stored; `ingest_fault_test.go`).
  The session-record chunks when they cannot be written (payload bucket deleted under the broker: no cursor, no chunk
  names, a warning) and when one cannot be read (purged: cursor dropped on restore, a warning;
  `replay_cursor_spill_fault_test.go`). `ReconnectCatchUp` across a second drop during a 300-message catch-up held by a
  client at Receive Maximum 2: every message once and in order, the client never closed
  (`reconnect_catchup_cut_test.go`). Still unit-only: `catchUp`'s `retryAfterDrop` branch. A proxy drop does not
  reach it, because the ordered consumer rides over a dropped connection (the server keeps it) and a read blocked on
  a dead connection did not return an error in 40 s; it needs the consumer lost, which only a JetStream server restart
  does (`TestClusterReconnectCatchUp...` kills the server, but its read is not cut mid-way). Also: closing a broker
  whose NATS is down while a catch-up read waits did not return in the test's cleanup (observed once, test
  failing); not investigated, worth its own entry.
