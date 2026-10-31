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

**Blocked by:** nothing for (3) below (the record carrying ids past 4096 outside its value, as the in-flight spill
does); a clustered, replicated stream to measure the added requests on is *real deployments*.

**Today:** a session whose record or memory has a replay cursor (`cursor.go`) is replayed from it; one without is
replayed from `OfflineQueueRewind` before its release, as before, and keeps the rewind's limits: a message whose live
copy never came and that is older than the rewind is lost, and a late copy more than `deliveredSeqWindow` (8192)
sequences below what the session has delivered is not noted [MQTT-4.4.0-1]. A session is without a cursor when:

1. it was restored from a record written before the cursor existed, until its first replay reads the queue to its
   end (one absence, once, after the upgrade);
2. it left more than 1024 holes (`maxReplayHoles`), or more than 16384 ids above the cursor (`cursorIDBound`);
3. its record could not carry every id above the cursor (more than `maxStoredDelivered`, 4096, or trimmed to fit a
   value): the record is written without one, so a broker that restores it falls back, though the broker that held
   it does not;
4. a SUBSCRIBE could not read the queue's last sequence, or its filter cannot be narrowed to queue subjects.

**Recovered while connected (2026-10-31):** after 1024 deliveries without a cursor (`cursorScanAfter`), a restart scan
(`restartCursor`) reads the session's subjects from where a rewind started now would (`OfflineQueueRewind` ago,
the cursor's *base*) and sets the cursor with the holes it found. So (1), (2) and the unreadable-sequence half of (4)
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
into the record needs a field older brokers refuse, or a major version. (3) and the cap: let the record carry ids
past 4096 outside the value. The other half of (4), a filter that cannot be narrowed, still never scans. Each restart
scan reads up to one rewind of the session's subjects per 1024 deliveries while it keeps being refused (a session
with more than 1024 holes inside one rewind); not measured.

**Also:** a hole is sent only at the next resume. A message whose live copy never comes stays owed while the client is
connected, which [MQTT-4.4.0-1] allows (it requires the resend on reconnect) but a client cannot tell from a lost one.
A scan that finds a hole older than a few seconds could hand it to the catch-up path. A resume whose queue read fails
(an error other than an empty or unanswered fetch, which are read again since 2026-10-31) logs "what it held is lost
for this session" and sends nothing until the next resume, though the cursor did not move; retrying the read while
the client stays connected would close that. And each SUBSCRIBE now costs a
stream info request and each 1024 deliveries a filtered read; neither is measured over a clustered, replicated stream.

## Self-fencing on a cluster: what is left

**Blocked by:** nothing, and little is worth building. `TestClusterWillOfAClientOnAnIsolatedNATSServerIsPublishedOnceAfterItsConnectionCloses`
isolates the server a broker uses from the other two (three real servers in containers, `natscluster_test.go`), watches
the client's socket close before the survivor publishes, heals, and counts one publication. It found that the fenced
broker's own disconnect path published the Will again when the removal of the record timed out; `fire` retries the removal
until the store answers. Since 2026-10-31 it runs both shapes, the isolated server leading the Will bucket and a survivor
leading it (`natsCluster.LeadStream`). **Fixed (2026-10-31):** the fence proved reachability with a read of the Will
bucket, which every replica answers: the isolated broker never fenced and a survivor published the Will of a client still
connected [MQTT-3.1.2-8], while a survivor's read, routed now and then to the isolated replica, timed out often enough to
fence the survivor (the three red `cluster` runs of PR #187). The probe is a write a majority must accept; the test's
socket-close helper had hidden the first half by reading its own read deadline as the close.
Left: (1) a partition that leaves the broker's server with a majority and cuts the
*broker* off is the proxy test, not a cluster one; (2) a partition longer than `retryFor` with the broker still cut off
waits for the store for as long as it takes, by design, so there is nothing to assert but that it does not publish;
(3) a Will bucket leader election longer than twice `WillCheckInterval` fences every broker (writes stop until it ends;
NATS starts one 4 to 9 s after the leader goes quiet, against 10 s at the default interval). Not measured on a killed
leader yet; a cluster test that SIGKILLs the bucket's leader and counts closed clients would say whether the default
needs to rise.

## Durable Wills on by default

**Blocked by:** a major version (or a minor one behind an opt-in), and a decision to take it.

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

**Blocked by:** a cross-broker design (a subject layout change or per-subscription JetStream consumers) and the
compatibility decision it carries.

`Options.DurablePublish` makes the PUBACK wait for the queue stream's copy and for a NATS flush of
the live copy. The live copy itself is still core NATS: a subscriber connected to another broker
whose NATS connection drops at the wrong moment misses it, and nothing re-sends it to a session
that was connected (only to one that was away). Closing that needs a stream capturing the live
subjects, which overlaps the retained stream's subjects (JetStream refuses overlap), so it needs
either a subject layout change or per-subscription JetStream consumers. A decision with a
compatibility cost.

## DurablePublish: a retry after a refused flush leaves a second queue copy

**Blocked by:** a decision, taken: not implemented. The opt-in below waits for real deployments to ask.

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

# Tier 3: SHOULD statements

## A held shared message: the killed-broker window and the packet identifier

**Blocked by:** real deployments for a per-group `SharedHoldLimit` (no known need); the Receive Maximum slot is held
by [MQTT-3.3.4-9], which no change releases earlier.

`SharedHoldLimit` is off by default and applies to every group alike; a per-group value (which
would be an `Options` map or a callback) is a possible follow-up, with no known need yet. A
message it hands back is withdrawn from the slow member (no resend on reconnect, a late PUBACK
ignored), but the member's Receive Maximum slot stays taken until that PUBACK or a reconnect:
the client still counts the message against its own Receive Maximum, so freeing the slot earlier
would let the broker exceed it [MQTT-3.3.4-9].

The killed-broker window is pinned (`shared_killed_test.go`, `NATSMQTT5_SLOW_TESTS=1`): the
message is seen twice at worst, never lost.

## A shared subscription's membership: the edges left

**Blocked by:** JetStream, which has no conditional consumer delete (the last-leaver race); the killed-broker edge
waits on the session store's own reclaim and needs nothing built.

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

**Blocked by:** a major version (and a `MIGRATION.md` written in that release).

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

**Blocked by:** real deployments (brokers apart enough for the difference to matter) and a decision on trusting two
brokers' clocks.

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

**Blocked by:** a major version for the default, and real deployments for how clients in the field handle aliases.

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

**Blocked by:** known duplicates (checked again 2026-10-31, after #202). The default stays off: of the three windows
#202 recorded, two are closed for a client that comes back within the group's AckWait, and what is left of them, with the
first, still lets one QoS 2 message reach a group twice, which a QoS 2 grant forbids [MQTT-4.3.3-2], [MQTT-4.8.2-5].

1. **Open.** The late backlog (*A default queue that failed at startup*, **Left:**): a message the group acknowledged
   from the backlog before its live copy reached this broker is delivered live as well. No crash is needed, only a slow
   live copy at the moment the backlog is given. Closing it needs group consumers created by start sequence, which that
   entry's **Blocked by** names as a decision (a mixed-version fleet would fail its shared SUBSCRIBEs). This one alone
   keeps the default off.
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
4. **Open, what is left of 2 and 3.** A killed broker's held messages are held again only when the session is restored,
   which is when its client reconnects. A client that stays away longer than the AckWait (30 s) after its broker was
   killed finds its message given to another member, and is then resent it as well: a probe with a 0.9 s AckWait saw the
   other member get `job-away` at QoS 1 after the kill, and the returning client get it with DUP and its original
   identifier. [MQTT-4.8.2-4] and [MQTT-4.8.2-5] both say the message is that client's. Closing it means a surviving
   broker holding (`+WPI`) the backlog messages named in a dead broker's attached records, the sweep's job for an
   orphaned session (`sessionsweep.go`), without restoring the session. Also here: `checkpointNow` drops the record
   write's error (`checkpoint.go`, `_ = c.broker.checkpointSession(c)`), so a PUBLISH can go out after a failed write,
   and with `SessionCheckpointInterval` negative no record is written at all.

Flip the default when (1) and (4) are closed, each with a test that fails before the fix, or when (4) is written into the
README as the guarantee's limit across a killed broker, as `shared_killed_test.go` already does for QoS 1 ("seen twice
at worst, never lost"). Until then the README keeps "leave it off in production".

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
Not covered then: a message pulled but not yet checkpointed when its broker dies. For
QoS 2 the record is written before the PUBLISH, so within the AckWait this is closed
(**Blocked by**, window 2); a client away past it is window 4.

### 2. Done (2026-10-31): the ownership point, acknowledged to the group at PUBREC

Once the record naming the PUBREL is written (`handlePubrec`, `settleOwned`) the
JetStream message is acknowledged, `handBackHeld` skips an entry that
`awaitingPubcomp`, and a restore acknowledges a `Rel` entry still naming one. Tested
past PUBREC with an unsubscribe, a clean start, a session expiry, a returning client
and a SIGKILLed broker; before PUBREC the end of the session still gives the message
to another member. An UNSUBSCRIBE does not hand back a message already sent (QoS 1
either): it stays the session's until the exchange ends. Not covered then: a broker
killed after the PUBREC is read and before its record is written. Closed for a client
back within the AckWait (**Blocked by**, window 3); one away past it is window 4.

### 3. Grant QoS 2 on `$share/`

Make `Options.SharedQoS2` (added by entry 1, off) the default once the duplicates under **Blocked by** are settled,
keep an explicit opt-out (an `Options` field and a flag, since `false` is the zero value), add a CHANGELOG line that
names the SUBACK change, and document it in the README. The SUBACK test moves from QoS 1 to the
requested QoS ([MQTT-3.8.4-7]); with the switch off a QoS 2 request is still
granted QoS 1.

# Operations and robustness (not conformance)

## A lone QoS 2 message still waits for two record writes

**Blocked by:** a decision the spec has taken: a write not waited for breaks [MQTT-4.3.3-6]; only a faster store helps.

**Done (2026-10-30):** messages waiting together share the record written before their PUBLISH
(`deliverBurst`, `subscribe.go`) and PUBRELs queued while a record is written share the next one
(`queuePubrel`, `publish.go`), so a burst of 32 costs 0.42 ms a message instead of 1.43
(`BenchmarkQoS2DeliveryBurst/Durable`, a subscriber with a persistent session, one client; the
non-persistent ceiling is 0.09 ms). Killed-broker tests cover the burst (`killed_qos2_burst_test.go`).
**Left:** one client in lockstep (`BenchmarkQoS2Delivery`) has nothing to share: two writes a message,
about 2 ms on a loaded host. That needs a write that is not waited for, which [MQTT-4.3.3-6] does not
allow, or a faster store. Not measured with `sync_always` on the NATS server.

## Spilled in-flight entries: what an acknowledgement still rewrites

**Blocked by:** nothing, though nothing has measured the rewrite as a cost.

**Done (2026-10-30):** a checkpoint names a chunk whose content did not change without writing it
(`spillRecordReusing`; a message appended writes the last chunk only, 1 of 10 in the test), until
its payloads near the bucket's TTL, when all are rewritten. The sweep deletes the unnamed payloads
(and counts the chunks as named) of an owner whose record was released.
**Done (2026-10-31):** chunks also end at a fixed identifier count (`defaultSpillChunkIDs`, 1024: a chunk ends after
an entry whose identifier is a multiple of it, or when size cuts it), so an acknowledgement at the front or a message
appended changes the chunk it touches and no other (`TestSpillAnAcknowledgementAtTheFrontWritesFewChunks`). The layout
is still an ordered list of chunk keys, which an older broker reads as before.
**Done (2026-10-31):** the sweep judges the payloads of a session attached to the broker running it by what that
broker holds in memory as well as by the record, so a payload under the owner that neither names is deleted once
older than the grace period instead of waiting for the bucket's TTL
(`TestTheSweepDeletesAnAttachedSessionsUnnamedPayloadAndKeepsTheOneItHolds_MQTT_4_4_0_1`). A session attached to
another broker is left to that broker's sweep.
**Left:** an acknowledgement still re-cuts the first spilled chunk (where the inline part ends), and a chunk cut by
size rather than by identifier shifts as before; identifiers that do not cycle in send order (a client that acknowledges
out of order leaves gaps) make the chunks uneven, not wrong. A session attached to a broker that died has its unnamed payloads judged only once
its record is reclaimed, or they age out.

## Retained view: a stale entry that cancels an unseen one

**Blocked by:** *real deployments*: what is left is a partition shorter than the NATS client's ping interval, which the
broker cannot detect.

**Done:** a broker cut off past `RetainedTombstoneTTL` no longer serves a retained message another broker cleared
(the sweep compares the view with the stream's count; the purge treats error 10043 as already purged), and after a NATS
reconnect every entry of the view is checked against the stream whatever the counts say
(`TestARetainedViewIsVerifiedAfterANATSReconnectEvenWhenTheCountsAgree`). One lookup per entry per reconnect.
**Done (2026-10-31):** that check runs when the NATS client reports the reconnect (a status listener, so an
application's own reconnect handler on `Options.Conn` is untouched), not at the next sweep up to
`SessionSweepInterval` later (`TestARetainedViewIsVerifiedAsSoonAsTheNATSClientReconnects_MQTT_3_3_1_6`). A check that
fails because JetStream is not answering yet is left to the next sweep.

**Left:** a reconnect the broker's NATS client did not notice (a partition shorter than its ping interval) triggers
nothing, and a stale entry it leaves is served until a count disagrees.

## The weekly scans fail where no pass reads them

**Blocked by:** nothing in this repository. The remaining work is in the host (`check-repo-health.py` counting the
`Vulnerability scan` schedule run), outside this repo.

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

**Blocked by:** a cross-broker design (a decision against the membership bucket with a revision check).

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

## A default queue that failed at startup: a duplicate across brokers

**Blocked by:** a decision: closing the last window needs group consumers created by start sequence, and a broker of
an earlier version updates the consumer with `DeliverNew`, which JetStream refuses to change on an existing consumer, so
a mixed-version fleet would fail its shared SUBSCRIBEs. No deployment is known to run a fleet with the queue down on one
broker only.

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
**Left:** a message the group acknowledged from the backlog before its live copy reached this broker is below the floor
and still delivered live, so it reaches the group twice. That needs a live copy slower than a member's whole
PUBLISH-to-acknowledgement exchange, and only in the moment the backlog is given.

## Without the queue, a client that falls behind still loses QoS 1 and 2

**Blocked by:** real deployments without JetStream asking for the opt-in limit.

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

**Blocked by:** a decision for the JetStream-less queue (undecided); nothing for the rest, though driving the cursor's
bounds over a socket needs a hook to lower them.

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
- **The replay cursor's bounds and its race with a live copy.** That a cursor is dropped past 16384 ids above it,
  that a record which left ids out carries none, and that a live copy arriving below the cursor while a scan moves it
  becomes a hole (`noteLiveCopy` under `cursorMu`) are unit tests (`cursor_internal_test.go`). The hole bound, the
  restart after it and a live copy below a restarted cursor's base are driven over a socket (`replay_cursor_test.go`,
  the bound lowered by a hook); the restart's other refusals (a message held below base, a scan longer than the
  rewind, a subscription made while it read) are unit tests and the randomized model
  (`cursor_model_internal_test.go`), which drives the session's code with scans applied after other events, not the
  broker's goroutines.
- **The offline queue on a broker without JetStream** is not there: a QoS 1 message published while a
  session is away is lost although the CONNACK said Session Present 1 [MQTT-3.1.2-23],
  [MQTT-4.5.0-1]. `TestABrokerWithoutJetStreamStillStartsWithNoQueue` pins it as the known
  deviation. Closing it needs a queue that does not depend on JetStream, which is undecided; until
  then the README and CONNACK should not promise more than the test shows.
