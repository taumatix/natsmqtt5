# Roadmap

Ordered by how much they limit real deployments, not by how interesting they
are to build. Each entry says what breaks today, so it can be judged on its
own.

## Offline message queue, and QoS 1/2 retransmission on reconnect

**Today:** messages published while a session is disconnected are dropped for
it, and an unacknowledged QoS 1 or QoS 2 message is not resent after a
reconnect. MQTT-5.0 §4.4 expects both for a session that persists — and now
that `Options.PersistentSessions` makes a session genuinely persist, this is
the largest remaining gap between what the broker stores and what a client is
entitled to assume it stored.

**Shape:** a durable JetStream consumer per session, filtered to the session's
subject set, with the consumer's ack state carrying the in-flight set. That
makes the in-flight Packet Identifiers worth persisting into the session
record, which today they deliberately are not: recording that a message was
in flight without being able to resend it would be a promise the broker cannot
keep.

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
