# Roadmap

natsmqtt5's goal is MQTT 5 coverage, so this list is ordered by conformance: violated MUST
statements a conforming client hits in normal use, then MUSTs on rare or optional paths, then
SHOULDs, then features the spec defines that the broker does not offer. Operational work that is
not about conformance comes last. The order comes from the statement-by-statement review in
`CONFORMANCE.md`; re-run it when the code or the spec has moved a lot. Each entry says what breaks
today and cites the statements it violates.

# Tier 1: MUST statements a conforming client hits in normal use

## Offline message queue: what it still loses

**Today:** the queue (`offline.go`) is on by default whenever the broker uses JetStream (merged
after v0.9.1, ships on the next release train), so a session with an expiry gets what was
published while it was away [MQTT-3.1.2-23], [MQTT-4.5.0-1]. `DisableOfflineQueue` turns it
off, which is a deviation, and a broker without JetStream cannot have it. One gap remains inside
it, on the restored-session path (`PersistentSessions`). Two earlier attempts were stopped by
a safety classifier (2026-10-05), so attempt this with my human present.

### A restored replay that can rewind

v0.8.0 replays restored sessions from `AwayAt` exactly, because the ids the previous connection
delivered are not stored, and rewinding without them could deliver a QoS 2 message twice. Storing
the last few delivered ids in the record (bounded, as `noteDelivered` already is) would let a
restored replay rewind like an in-memory one. That pairs with "Retransmission that survives a
broker restart" below, which needs in-flight state in the record too. Also: a broker killed
outright records no `AwayAt`. The record's `Attached` with no release could fall back to the
claim time minus the queue's age, at the cost of duplicates.

## A connection that drops while behind loses what was waiting for it

**Today:** a connection keeps up to 2048 messages waiting for its client, and catches up from the
offline queue when it falls further behind (`catchup.go`). If the connection drops, what was
waiting is discarded, and the resume replay (`replayOffline`) rewinds only 2 seconds before the
disconnect. A client minutes behind when its network failed loses those minutes
[MQTT-3.1.2-23], [MQTT-4.5.0-1]. The resume replay also gives up after 30 seconds
(`offlineReplayTimeout`), which a slow client with a long backlog reaches.

**Shape:** the live copies now carry their stream sequence, so on detach the session can record
the lowest sequence it has not delivered (what is waiting, or where a catch-up stood) and the
resume replay can start there instead of at a time. That is also most of what 1b ("a restored
replay that can rewind") needs. The replay's deadline should go, as the catch-up's did: it goes at
the client's pace. Size M.

## Message Expiry Interval enforcement

**Today:** the property is forwarded unaltered. MQTT-5.0 §3.3.2.3.3 says a
server must delete a message whose expiry has passed before onward delivery,
and must decrement the value by the time the message waited. [MQTT-3.3.2-5],
[MQTT-3.3.2-6]. A probe confirmed it: a retained message with a 1 s expiry was
still delivered after it had passed, with the interval unchanged. The same
applies to the offline replay and to resends.

**Shape:** for retained messages, an expiry sweep over the retained stream. The
decrement needs the message's arrival timestamp, which JetStream already
records.

## Retransmission that survives a broker restart

**Today:** the resend on resume reaches as far as this broker's memory. A
session restored from the JetStream record — after a restart, or on another
broker — has an empty in-flight set, so its unacknowledged messages are lost
even though its subscriptions come back. `TestPersistentSessionOnAnotherBroker
HasNothingToResend` pins that boundary so it cannot quietly be assumed wider.

**Why it is not simply fixed:** the record is a JetStream KV value and the
payloads do not belong in it. Persisting Packet Identifiers alone would record
that a message was in flight without being able to resend it — a promise the
broker cannot keep, which is why they were left out in the first place.

**Shape:** once the offline queue exists, the unacknowledged set is the durable
consumer's ack-pending set and the payloads are already in the stream. What
still has to go in the record is the Packet Identifier each pending message was
sent under, so a resend after a restart reuses the original one
[MQTT-4.4.0-1].

This only binds with `PersistentSessions`: without it a restart answers Session
Present=0, which is honest. With it the CONNACK says Session Present=1 and
nothing in flight is resent, which breaks [MQTT-4.4.0-1] and [MQTT-3.1.2-5]. The
record also has to carry the inbound QoS 2 state, the Packet Identifiers received
and not yet released. Without it a QoS 2 message the client resends after a
failover is forwarded twice [MQTT-4.3.3-10]. That part is small once the record
changes.

## A retained message keeps its publisher's Topic Alias

**Today:** the broker's in-memory copy of a retained message keeps the Topic Alias property the
publisher sent (`retain.go`), and a subscriber on the same broker can receive it, an alias that
means nothing on its connection. 1 in 4 probe runs. [MQTT-3.1.2-26], [MQTT-3.1.2-27],
[MQTT-3.3.2-11].

**Shape:** strip the Topic Alias when storing, as the publish path does for live messages. Size
S.

# Tier 2: MUST statements on rare or optional paths

## A packet over the client's Maximum Packet Size is dropped whole

**Today:** a CONNACK, PUBACK, PUBREC or DISCONNECT that would exceed the client's Maximum Packet
Size is discarded entirely (`conn.go`, the write path), where the spec says to drop the Reason
String and User Properties first and send the rest. A CONNECT with Maximum Packet Size 40 and an
empty Client Identifier got no CONNACK at all, and a lost PUBACK or PUBREC leaves the client's
exchange unfinished. [MQTT-3.2.2-19], [MQTT-3.2.2-20], [MQTT-3.4.2-2], [MQTT-3.5.2-2],
[MQTT-3.14.2-3].

**Shape:** on an oversize ack, retry without the optional properties before giving up. Size S.

## No Local on a shared filter is answered with an invalid SUBACK code

**Today:** a SUBSCRIBE with No Local on a `$share/` filter gets 0x82 inside the SUBACK
(`subscribe.go`). 0x82 is not a valid SUBACK Reason Code; the spec requires a Protocol Error, a
DISCONNECT with 0x82 and the connection closed. [MQTT-3.9.3-2], [MQTT-4.13.1-1].

**Shape:** disconnect with 0x82. Size S.

## A Subscription Identifier in a client PUBLISH is accepted

**Today:** a PUBLISH from a client carrying a Subscription Identifier is accepted and
acknowledged. It is a Protocol Error. [MQTT-3.3.4-6], [MQTT-4.13.1-1].

**Shape:** disconnect with 0x82 in the packet validation. Size S.

## Packets may still go out after a server DISCONNECT

**Today (unsure):** after the broker sends a DISCONNECT, the socket stays open while `finish()`
runs, so a queued PUBLISH may still be written. The spec says the sender of a DISCONNECT MUST NOT
send more packets and MUST close the connection. [MQTT-3.14.4-1].

**Shape:** a test that queues deliveries and triggers a server DISCONNECT, then the order fixed
if it fails. Size S.

## A Response Topic with wildcards is forwarded

**Today:** a PUBLISH whose Response Topic contains wildcards is forwarded unchecked; a Response
Topic MUST be a valid Topic Name. [MQTT-3.3.2-14].

**Shape:** validate it as a Topic Name and refuse the PUBLISH with 0x82 if not. Size S.

## An Authenticator's Reason Code is not checked

**Today:** a Reason Code returned by an `Authenticator` is sent in the CONNACK as given
(`handshake.go`), so an implementation can put a code the CONNACK does not allow on the wire.
[MQTT-3.2.2-8].

**Shape:** map a code that is not valid for CONNACK to 0x80 and log it. Size S.

## A late acknowledgement can get a client disconnected

**Today:** `conn.resend` re-reads the live in-flight entry immediately before
writing, so the long window — the wait for send quota — is closed. A
microsecond one is not: an acknowledgement that lands between that read and the
write leaves the broker resending a completed exchange, and the client's
acknowledgement of *that* arrives for a Packet Identifier the session no longer
holds, which `handlePuback` answers with `0x82 Protocol Error`. A conforming
client is disconnected for the broker's mistake, and its next reconnect can hit
the same window.

**Reaching it** needs an acknowledgement for a message received on the previous
connection to arrive during that window, from a client that flushes owed
acknowledgements on reconnect. The other route, a displaced connection still
decoding packets its socket had buffered, is closed since v0.4.3: completing an
exchange its successor resent leaves the identifier owed (`resentOn` in
`completeInflight`). This entry is the same-connection case, which `resentOn`
cannot tell from an ordinary acknowledgement.

**Shape:** keep the set of Packet Identifiers this connection resent, and answer
an unmatched acknowledgement for one of them by ignoring it rather than by
disconnecting. Making the read and the write atomic is the other option and the
worse one: it means holding the session lock across a socket write.

Half the mechanism now exists: `session.withdrawn` and `forgetWithdrawn` are
exactly "an identifier the client may still acknowledge, whose acknowledgement
is ignored", built for the filters denied on resume. What this entry needs is
the same set populated from `conn.resend` rather than from the handshake.

## The withdrawn-identifier set only empties when the client acknowledges

**Today:** `session.withdrawn` holds the Packet Identifier of every in-flight
message taken back on resume, so that a late PUBACK or PUBCOMP for one is
ignored rather than answered with `0x82 Protocol Error`, and so that `nextID`
does not hand the identifier to a new message. Nothing else removes an entry. A
client that never flushes the acknowledgement it owed leaves the identifier
spent for the life of the session — bounded by the 65535 that exist, but
monotonic, so a long-lived session narrowed repeatedly slowly runs out.

Since v0.4.3 the set also takes an identifier when a displaced connection's
acknowledgement completes an exchange its successor resent. If the successor
drops before acknowledging its copy, that identifier is owed for good as well,
and so is the send-quota slot it names (only until that connection ends,
since quota is per connection).

**Why it is not simply fixed:** a timer is the wrong instrument — the
acknowledgement is owed by a client that may be offline, and MQTT puts no
deadline on it. What is needed is evidence the client will never send it.

Exhaustion is not merely a slow leak: `nextID` returning false makes `deliver`
error, and `deliverLoop` answers that by closing the connection — on every
reconnect, for as long as the session lives.

**Shape:** forget a withdrawn identifier on the second resumption after its
withdrawal. A client that has completed two CONNECTs without flushing the
acknowledgement is not going to, and the count is already there in the session.

## `forgetWithdrawn` does not check which acknowledgement was owed

**Today:** the withdrawn set stores Packet Identifiers and nothing else, so a
withdrawn QoS 1 identifier can be closed by a PUBCOMP and a withdrawn QoS 2 one
by a PUBACK. A client that sends the wrong one consumes its own withdrawal
record and then gets itself disconnected with 0x82 when it sends the right one.
Self-inflicted, and confined to that client's session.

**Shape:** store the packet type still owed alongside the identifier and match
on both. It falls out of any change to the set's contents, which is why it sits
next to the entry above rather than on its own.

## The Will Message of a broker that was killed

**Today:** a Will is published by the broker holding the connection. A broker
that shuts down cleanly publishes its clients' Wills; one killed outright does
not, whether or not sessions are persisted.

**Why it is not simply fixed:** the Will is not in the session record on
purpose. It belongs to the network connection (MQTT-5.0 §3.1.2.5), and a broker
reading a record cannot distinguish a connection that has ended from an owner
that is merely busy — so a broker acting on someone else's stored Will would
announce live clients as dead, which is worse than the silence it replaces.

**Shape:** a lease the owning broker renews while a connection is open, so that
a lease which stops being renewed is evidence the connection ended rather than
a guess. The Will then becomes safe to store, and its Delay Interval becomes
enforceable across a broker's death.

## A PUBACK that means the message is safe

**Today:** the broker hands a QoS 1 message to its NATS connection and
acknowledges immediately. nats.go flushes that write from another goroutine, so
a broker killed in the window between the two loses a message it has told the
client it has. The subscribe path does not have this problem: a SUBACK waits for
the NATS server to confirm the subscription, because a client cannot detect a
lost subscription the way it can retry a publish.

**Why it is not simply fixed:** flushing before every PUBACK costs a round trip
to NATS per QoS 1 publish, which is the broker's throughput. Core NATS also has
no acknowledgement of its own, so a flush proves the bytes left the broker, not
that the server accepted them.

**Shape:** publish QoS 1 and 2 through JetStream and acknowledge on the
publish-ack, as an opt-in `Options.DurablePublish`. That buys a real
end-to-end guarantee rather than a cheaper illusion, at a latency cost the
deployment chooses.

# Tier 3: SHOULD statements

## A shared member's unacknowledged QoS 1 message dies with its session

A shared subscription's backlog (`shared.go`) hands a message to a member and acknowledges it to
JetStream as soon as the PUBLISH is in flight, so from then on the message is that session's. If
the client never comes back and the session ends, the message ends with it. For QoS 1 the spec
says the Server SHOULD then send it to another member of the group (MQTT-5.0 §4.8.2). Doing it
means keeping the JetStream message unacknowledged until the PUBACK (with `InProgress` to hold
off redelivery), and handing it back to the group when the session expires. That needs the
session expiry sweep from "Expiring a detached session that is never resumed".

## A shared subscription with no members keeps its backlog

§4.8.2: a shared subscription ends when no session is subscribed to it, and its undelivered
messages are deleted. The backlog consumer is removed by JetStream only after no member has pulled
for longer than `MaxSessionExpiry`, so a group whose last member unsubscribed keeps collecting
messages for that long, and a new member under the same name gets them. Deleting the consumer on
the last UNSUBSCRIBE needs a count of members across brokers, a key-value entry per group for
instance.

## Keeping clients off `$` topics

v0.9.1 reserves `$retained` and `$queue`, the two levels the broker's own data lives under. The
spec's broader advice is that applications do not use `$` topics for their own purposes, and
that a Server SHOULD prevent clients exchanging messages on them (MQTT-5.0 §4.7.2). Refusing every
`$` topic other than `$share/` would follow it, at the cost of breaking anyone who uses `$app/…`
today. A decision for a minor release, with an option to keep the old behaviour.

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
broker restart, which is "Retransmission that survives a broker restart".

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

## Expiring a detached session that is never resumed

**Today:** a session whose client disconnects with a non-zero Session Expiry
Interval keeps its NATS subscriptions on the broker that held it until either
the client comes back or the broker stops. `session.expired` is only consulted
on reconnect, so a workload that churns through Client Identifiers grows the
broker's subscription set without bound. This predates persistent sessions and
is not caused by them; the durable records now have a sweep and the in-memory
sessions still do not.

**Shape:** the same sweep the session store already runs, extended to walk
`Broker.sessions` and discard the expired ones. It is a few lines; the work is
in the test, which has to prove the NATS subscriptions actually go away.

## A takeover in the middle of a stored-record resume is untested

The restore path in `resumeSubscriptions` has the same ownership guard as SUBSCRIBE, but no test
drives a takeover while a session is being restored from its stored record. It is covered by
reasoning only. The technique in `displaced_ack_test.go` (Keep Alive 0, a held QoS 0 PUBLISH) can
reach it.

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

## The queue's cost per publish is unmeasured

Every QoS 1 and 2 publish now waits for a JetStream store before its PUBACK, and the queue holds a
day of that traffic by default. There is no benchmark of the throughput this costs, so the README
can only say "one round trip". A benchmark against a file-backed embedded server, and a line in
the README with the number, would let a deployment size it before upgrading.
