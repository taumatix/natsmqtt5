# Roadmap

Ordered by how much they limit real deployments, not by how interesting they
are to build. Each entry says what breaks today, so it can be judged on its
own.

## Re-authorising an in-memory session on resume

**Today:** `handshake.mayResume` re-runs `Options.Authorizer` over every
subscription it restores from the durable record, because a session is keyed by
Client Identifier alone and whoever the Authenticator lets use that identifier
inherits it. A session resumed from *this broker's memory* gets no such check:
`resumeSubscriptions` takes its `len(stored) == 0` early return, and the NATS
subscriptions that were never torn down go on delivering. So a permission
revoked between two connections does not take effect until the session expires.

**Why it is now worth more:** retransmission on resume means a resumed session
hands over the previous connection's undelivered payloads at CONNACK time,
before the client has sent anything. The subscription staying live was already
the larger leak — it covers every *future* message, not just the buffered ones —
but the two share one fix and one test.

**Shape:** run `mayResume` over the session's live `subs` map on the in-memory
branch too, unsubscribing the denied filters before `deliverLoop` starts, and
drop any in-flight entry whose topic no longer matches a surviving filter.
`Options.Authenticator` should also document that an implementation must bind
the authenticated principal to `req.ClientID`, or assign one with
`AssignClientID`, since the session key is the Client Identifier.

## Offline message queue: a durable consumer per session

**Today:** messages published while a session is disconnected are dropped for
it, whether or not the session is persisted. The broker now resends what it was
already holding when a connection died (MQTT-5.0 §4.4), but a message published
into the gap was never held by anything — core NATS fans out to whoever is
subscribed at the time and keeps nothing.

**Shape:** a durable JetStream consumer per session, filtered to the session's
subject set, delivering on resume from where the session left off. This is the
entry the other two below depend on, and the largest of the three.

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
connection to arrive during that window — either from a client that flushes
owed acknowledgements on reconnect, or from a displaced connection still
decoding packets its socket had already buffered.

**Shape:** keep the set of Packet Identifiers this connection resent, and answer
an unmatched acknowledgement for one of them by ignoring it rather than by
disconnecting. Making the read and the write atomic is the other option and the
worse one: it means holding the session lock across a socket write.

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

## Enhanced authentication (AUTH packets)

**Today:** a CONNECT with an Authentication Method is refused with
`0x8C Bad authentication method`. That is the conforming answer, but it rules
out SCRAM and Kerberos.

**Shape:** an `Auther` interface driving the AUTH exchange in MQTT-5.0 §4.12,
plus re-authentication on an established connection.

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

## Message Expiry Interval enforcement

**Today:** the property is forwarded unaltered. MQTT-5.0 §3.3.2.3.3 says a
server must delete a message whose expiry has passed before onward delivery,
and must decrement the value by the time the message waited.

**Shape:** for retained messages, an expiry sweep over the retained stream. The
decrement needs the message's arrival timestamp, which JetStream already
records.

## Outbound topic aliases

**Today:** the broker accepts topic aliases from clients but never sends them,
which is permitted and costs bandwidth on long topic names.

**Shape:** a per-connection alias table bounded by the client's Topic Alias
Maximum, with a least-recently-used policy.

## Tombstone accumulation in the retained stream

**Today:** clearing a retained message writes a zero-length message that stays
in the stream as a tombstone. `MaxMsgsPerSubject: 1` keeps it to one per topic,
so it is bounded, but a workload that creates and clears many distinct topics
grows the stream without bound.

**Shape:** a periodic purge of zero-length messages older than some age.

## WebSocket transport

**Today:** TCP and TLS only. MQTT over WebSocket is what browser clients and
many hosted deployments need.

**Shape:** an `http.Handler` that upgrades and hands the connection to the same
`conn` state machine, so it is a transport change and nothing more.

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
