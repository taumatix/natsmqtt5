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

**Blocked by:** a clustered, replicated stream to measure the added requests and the chunk writes on is *real
deployments*; nothing else here is.

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
the client stays connected would close that. And each SUBSCRIBE now costs a
stream info request and each 1024 deliveries a filtered read; neither is measured over a clustered, replicated stream.

## DurablePublish does not cover live delivery to subscribers on other brokers

**Blocked by:** real deployments, for what is left: a copy lost while both brokers stay connected has no event
to react to, and the only ways to see it (a periodic read of the queue per connected session, or per-subscription
JetStream consumers) cost every session all the time for a loss nobody has reported.

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

1. A live copy lost between two NATS servers of a cluster (a route that drops) while both brokers stay connected:
   no event reaches either broker. A session with a replay cursor gets it on its next resume (a cursor scan makes
   it a hole); a connected one does not.
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

## Outbound topic aliases: admit a name on its second sighting

**Blocked by:** nothing; a measurement. The thrash itself is fixed (CHANGELOG [Unreleased]): the table now keeps a ledger
of bytes saved against bytes spent on aliases and stops remapping while it has spent more, so a client whose Topic Alias
Maximum is below the set of names it sees is sent at most a constant 3 bytes per slot more than with aliases off, where
it was sent 6% more (a round robin over 40 names, any table under 40) or 1.3% more (the skewed mix at a table of 1).
Tables of 8 and up send exactly the bytes they did.

**Left (measured 2026-11-01, `outalias_measure_internal_test.go`):** the ledger bounds the loss and does not recover the
saving a better admission rule would. At a table of 8 the skewed mix saves 19.2% and raising the name floor to 32 saved
22.5%, because first-seen 21-byte names evict hot 40-byte ones; a round robin over more names than the table holds saves
nothing where holding a fixed subset would save the share of names held. Both come from admitting a name on its first
sighting.

- [ ] admit a name to the table on its second sighting (a bounded set of names seen once), and measure it with the harness
  above against the ledger alone; keep it only if the skewed mix at tables of 8 and 32 gains and nothing loses.
- [ ] not measured: CPU of the encode (aliasing encodes twice on a miss), and clients in the field that mishandle aliases,
  which only a trial tells; v1.0.0 users who see one pin `PinOutboundTopicAliases: Ptr(false)`.

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

**Blocked by:** a major version, for what is left: queueing a message published *unchanged* on a live subject
without a second delivery needs the brokers to be the first to see it, which is a subject layout change (MQTT
subscribers on subjects only brokers publish to), a wire break for every broker and plain NATS subscriber.

Only messages that pass through a broker are copied to the queue. They also
reach a shared subscription only through its NATS queue group, as QoS 0
messages do, and NATS still picks members whose client is away, whose share is
dropped.

**Design (2026-10-31).** A copy stored *after* a plain publish cannot be told apart from it: the live copy has
reached subscribers with no id, so every session that had it would get the queue copy again on a resume or a
catch-up, and a shared group would get it twice (queue group and backlog). The broker has to come first. So a
plain NATS client that wants the queue publishes through a broker: on `<prefix>.$ingest.<subject>`, which
nothing but the brokers subscribes to (one NATS queue group), and the broker that takes it does what it does for
an MQTT PUBLISH. The live copy then carries the id and queue sequence, so away sessions are replayed, the shared
backlog hands QoS 1 and 2 work only to connected members (which closes the shared half of this entry for those
messages without touching queue groups), and `ReconnectCatchUp` covers it. Additive, off by default.

**Done (2026-10-31):** `Options.NATSIngest` (`-nats-ingest`, `ingest.go`): topic and QoS checked as a PUBLISH's, a `$`
topic always refused (it would write `$retained` or `$queue`), retained stored, queue copy kept, live copy published, `DurablePublish`'s flush honoured; a request is answered
with `Mqtt5-Msg-Id` and `Mqtt5-Queue-Seq`, or `Mqtt5-Ingest-Error`. Tests (`ingest_test.go`, two brokers on one
NATS, a plain nats.go client): an away session is replayed, a connected one gets it once with its properties, 30
shared jobs all reach the connected member with the other away, QoS 0 is live only, a retained message is stored,
refusals are answered, and nothing answers with the option off.

**Left:**

1. Messages published unchanged on a live subject (above): not queued, and a shared group still gets them
   through its queue group.
2. For those, a QoS 0 or unqueued share NATS hands to a member whose client is away is dropped. MQTT allows that
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

**Blocked by:** closing the last window needs group consumers created by start sequence, and a broker of an earlier
version updates the consumer with `DeliverNew`, which JetStream refuses to change on an existing consumer, so a
mixed-version fleet would fail its shared SUBSCRIBEs (see "Looked at 2026-10-31" below). No deployment is known to run a
fleet with the queue down on one broker only.

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

## A `Broker.Close` hang reported by #216, not reproduced

**Blocked by:** *real deployments* (a hang needs a trigger the tests did not find).

#216's author saw a `Broker.Close` hang and did not investigate. Looked for on 2026-10-31 with no result: the
three #216 test files and `ReconnectCatchUp|Ingest|SpillChunk|ReplayCursor` under `-race -count=8` (200s, green;
`-count=5 -timeout 120s` is *not* a hang, those tests need about 130s, and its goroutine dump shows tests still
running), the whole suite under `-race` twice (about 520s each, green), and `close_down_test.go`: `Close` with
NATS cut and a client connected, and with a catch-up read held by a client at Receive Maximum 2 while NATS is cut,
both return in under a millisecond. Reading `Close`: it waits on `b.wg` (the connections), and `closeNATS` drains
with nats.go's own 30s drain timeout, so the bounded-by-a-deadline guarantee rests on `conn.shutdown` always ending
the connection. Not yet looked at: a connection whose client socket accepts no writes while its delivery loop is
inside a write (the DISCONNECT write has a deadline only on the close-behind path), and `Close` racing the
`catchUpAfterReconnect` goroutine, which is not in `b.wg`. **Next:** a test with a client that never reads, a full
send buffer and `Close`, under a deadline; if it hangs, give `shutdown`'s write the same short deadline.

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
