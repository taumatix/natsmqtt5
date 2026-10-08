# Roadmap

natsmqtt5's goal is MQTT 5 coverage, so this list is ordered by conformance: violated MUST
statements a conforming client hits in normal use, then MUSTs on rare or optional paths, then
SHOULDs, then features the spec defines that the broker does not offer. Operational work that is
not about conformance comes last. The order comes from the statement-by-statement review in
`CONFORMANCE.md`; re-run it when the code or the spec has moved a lot. Each entry says what breaks
today and cites the statements it violates.

# Tier 1: MUST statements a conforming client hits in normal use

## A killed broker's session replays the whole queue

**Today:** the queue is on by default whenever the broker uses JetStream, so a session with an
expiry gets what was published while it was away [MQTT-3.1.2-23], [MQTT-4.5.0-1], and with
`PersistentSessions` a session restored on another broker replays from the position its record
holds. A broker killed outright records none: the record is `Attached` with no release, and the
next claim (when the old owner no longer answers) replays everything the queue still holds for
the session, up to `OfflineQueueMaxAge` (a day by default). Nothing is lost, but a client that was
caught up gets a day of repeats, QoS 2 included, which [MQTT-4.3.3-2] does not allow.

**Shape:** bound the replay by what is known. A claim could write the time it was made into the
record (`AttachedAt`), and a connection could write its replay position to the record as it
advances (rate-limited, say once a second), so a dead broker's successor replays from the last
position written and skips the ids delivered above it. The cost is one key-value write per second
per behind client; the gain is that a kill costs a second of repeats rather than a day.
`TestASessionLeftAttachedByAKilledBrokerIsReplayedFromTheQueue` rewrites a record to simulate the
kill; a real one needs a harness that ends a broker without its cleanup.

## In-flight state for a broker that is killed

**Today:** the record's in-flight set and received QoS 2 identifiers are written when a connection
ends (`releaseStoredSession`), so a broker stopped cleanly or a client that drops hands them over.
A broker killed outright writes nothing: the next one resumes the session from the start of the
queue and resends nothing it had in flight, and a QoS 2 PUBLISH the client resends is forwarded
twice. [MQTT-4.4.0-1], [MQTT-4.3.3-10].

**Shape:** write the in-flight state while the connection lives, rate-limited as the replay position
is (see "A killed broker's session replays the whole queue"), or only when a QoS 2 identifier is received
or a message has been in flight longer than a second. Needs a harness that ends a broker without its
cleanup, which `leaveAsAKilledBrokerWould` only simulates.

## In-flight messages with no queue copy are not restored

**Today:** the record keeps a reference to the offline queue, so an unacknowledged message that has
none is left out and logged: a retained message sent on subscribing, a message from a shared
subscription's backlog (it is read from the queue stream but carries no queue sequence), and
anything with the queue off. A restored session does not resend them. [MQTT-4.4.0-1].

**Shape:** carry the stream sequence on shared-subscription deliveries, and put a retained message's
payload into the record when it is small, or its sequence in the retained stream. The queue-off case
needs the payload in the record or a stream of its own, and is the heaviest.

## The restore paths without a test over the wire

**Today:** the restore is driven end to end for QoS 1, QoS 2 in both states, the inbound QoS 2
identifier, expiry, a denied filter, no queue, and a record cut for size. Not driven: a queue copy
that has aged out (`OfflineQueueMaxAge`) between the release and the resume, which is logged and
the message dropped, and a filter unsubscribed before the release whose message is still owed.

# Tier 2: MUST statements on rare or optional paths

## A success CONNACK is larger than a very small Maximum Packet Size

**Today:** the CONNACK the broker sends on success states its own limits (Receive Maximum, Maximum
Packet Size, Topic Alias Maximum, and four availability flags), about 24 bytes before an Assigned Client
Identifier and about 54 with the 27-byte one the broker makes (counted by hand). A client whose Maximum Packet Size is under
that gets no CONNACK, because none of those properties is a Reason String or User Property and
dropping them would let the client assume limits the broker does not keep [MQTT-3.1.2-24]. A
client that small is rare.

**Shape:** drop the availability flags that restate their default (all four are 1), which saves 8
bytes losslessly, and decide whether a client under the remainder should be refused with a CONNACK
that fits, or served on assumed defaults. Size S, and a decision.

## A resend that an acknowledgement overtook spends a send-quota slot it does not hold

**Today:** found by reading while closing the late-acknowledgement entry, not by a test. A resend
claims a slot, and `takeQuotaSlot` marks the entry as holding it. If the acknowledgement of the
original lands before the copy is written, `completeInflight` returns that slot, the copy goes out
anyway, and the client's acknowledgement of it is now ignored (`forgetResent`) and returns nothing.
For the length of that exchange the connection has one more message in flight than its Receive
Maximum allows [MQTT-3.3.4-9].

**Shape:** have `forgetResent` say whether the resend was claimed before the acknowledgement it
follows, and take the slot back from the quota then, or have the gate-and-write path re-check the
entry after taking the quota. Size S; it needs a test that holds the write as `late_ack_test.go` does
and reads the quota.

## A withdrawn identifier does not survive a broker restart

**Today:** found by reading while closing the withdrawn-set entries, not by a test. The withdrawn
identifiers are in memory only; the session record carries the in-flight set and nothing about
what was taken back. A session restored after the broker restarted therefore forgets that it owes
the client's acknowledgement of a message withdrawn on resume, and that acknowledgement is answered
with `0x82 Protocol Error`, and its identifier can be handed to a new message first.

**Shape:** store the owed identifiers and their acknowledgement type in the record, with the
attach count they were withdrawn under, so the two-resumptions rule applies across a restart. Size
S; it changes the record, so it needs a `sessionRecordVersion` decision.

## A broker that is alive but cut off from NATS announces its clients as dead

**Today:** with `DurableWills`, a broker that stops answering its liveness
subject is presumed dead and a survivor publishes its clients' Wills. A broker
that is only partitioned from the NATS cluster still holds those connections,
so the Will of a connected client is published. The two-ping confirmation makes
this rare, not impossible.

**Shape:** a broker that loses its NATS connection closes its client sockets
(the connection is the lease), or a lease with a deadline the owner must renew
and a survivor waits out in full. Needs a decision on which, then a test that
partitions a real broker (a proxy in front of NATS) and asserts no Will for a
client that is still connected.

## The Will check reads every record on every tick

**Today:** with `DurableWills`, each broker lists the whole Will bucket every
`WillCheckInterval` and reads each record that is not its own, to learn the
owner. That is a Get per open connection with a Will per tick per broker.
**Shape:** key the records by owner (or keep a per-owner index key) so a tick
lists owners, pings each once, and reads records only for a dead one; measure
with a few thousand connections before and after.

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

## DurablePublish is untested when the flush itself fails

`TestDurablePublishAddsOneNATSRoundTripToThePUBACK` proves the flush is waited for, and the stream
tests prove a queue failure is refused with 0x83. No test makes the flush fail after the queue
stored the message (the NATS connection lost in that instant), so the 0x83 on that branch and the
duplicate queue copy a client's retry then leaves are not driven over a socket. Needs a proxy that
cuts the broker's NATS connection once the JetStream publish-ack has passed.

## Will Messages are not covered by DurablePublish

A Will published by `publishWillFor` goes through the queue's keep but is not flushed, and a Will
has no client to refuse to. Whether a published Will should wait for the flush is undecided.

## A reconnect leaves the previous Will lease behind

**Today:** `setWill` (`handshake.go` around 262, `session.go` around 860) overwrites the session's
live Will lease without dropping it, so the record it replaced stays in the `_wills` bucket, owned by
this broker. After this broker restarts, another broker finds the owner dead and publishes that
orphan. **Shape:** `setWill` returns the previous lease and the caller drops it. Test: connect with a
Will, replace it on the same session, kill and restart the broker, assert one Will at most.

## An old connection's finish can take the new connection's Will

**Today:** if the old connection's `finish` runs after the new connection's `setWill`, `takeWill`
takes the new connection's Will and publishes or cancels it. This predates v0.10.0 and is more
visible with `DurableWills` leases. **Shape:** key the Will to the connection that set it, so
`takeWill` only returns its own. Test: a takeover whose old connection finishes late.

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

The shared backlog changed this: a member now takes a message off the group's
consumer and the message becomes that session's in-flight state, exactly the
handover described above. What QoS 2 still needs is that state surviving a
broker restart, which the session record now carries for QoS 1 and 2 on a non-shared subscription;
the shared subscription's own in-flight messages are the entry "In-flight messages with no queue
copy are not restored".

## WebSocket transport

**Today:** TCP and TLS only. MQTT over WebSocket is what browser clients and
many hosted deployments need.

**Shape:** an `http.Handler` that upgrades and hands the connection to the same
`conn` state machine, so it is a transport change and nothing more.

## Escaping `*` and `>` in topics

**Today:** a topic containing `*` or `>` is refused with
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

- **A real kill.** The harness cannot end a broker without its cleanup, so the killed-broker
  fallback ([MQTT-3.1.2-23], [MQTT-4.5.0-1]) is tested against a record rewritten to look like one
  (`TestASessionLeftAttachedByAKilledBrokerIsReplayedFromTheQueue`). A test that runs the broker
  as a subprocess and sends it SIGKILL would prove the record a real kill leaves.
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
