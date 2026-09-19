# Resending what the last connection left unacknowledged

**Date:** 2026-09-19

## Goal

When a client reconnects with Clean Start 0 to a session this broker still holds, resend the
PUBLISH and PUBREL packets that connection left unacknowledged, with their original Packet
Identifiers.

## What it fixes

`natsmqtt5` advertises MQTT 5.0 and did not do this, which is a MUST:

> When a Client reconnects with Clean Start set to 0 and a session is present, both the Client
> and Server MUST resend any unacknowledged PUBLISH packets (where QoS > 0) and PUBREL packets
> using their original Packet Identifiers. This is the only circumstance where a Client or Server
> is REQUIRED to resend messages. Clients and Servers MUST NOT resend messages at any other time
> [MQTT-4.4.0-1].

So a QoS 1 message delivered into a connection that died before the PUBACK was lost, silently,
and the client had no way to notice: from its side the message simply never arrived. A QoS 2
exchange stalled halfway in the same way. The broker already stored everything needed to fix it —
`session.inflight` survives a reconnect to the same broker — and nothing read it.

This is the first of the three entries the roadmap's "offline message queue, and QoS 1/2
retransmission on reconnect" was split into. It is deliberately the smallest: it uses only state
the broker already keeps in memory, and it needs no JetStream.

## Approach chosen

`conn.retransmit`, run once at the head of `deliverLoop` before that loop reads its first queued
message.

- **Once per connection, not on a timer.** [MQTT-4.4.0-1]'s last sentence rules out a retry
  timer, which also means there is no back-off to tune and no duplicate-suppression to get wrong.
  MQTT 3.1.1 did expect timed retries; 5.0 does not, and this broker speaks 5.0 only.
- **No "was this session resumed?" condition.** A new session and a Clean Start both get a fresh
  `session` whose in-flight map is empty, so the loop is a no-op for them by construction. A
  condition would be a second place to keep in step with the handshake, and the wrong answer
  there is a conformance break in either direction.
- **At the head of `deliverLoop`, not on its own goroutine.** A Server "MUST send PUBLISH packets
  to consumers (for the same Topic and QoS) in the order that they were received from any given
  Client" [MQTT-4.6.0-5] and by default treats every topic as ordered [MQTT-4.6.0-6]. Running in
  the delivery goroutine before it starts consuming means arrears go out ahead of anything
  published since, which a parallel goroutine would interleave with. It is also where the
  receive-quota wait already lives, so the blocking case needs no new machinery.
- **PUBREL for an exchange past its PUBREC, PUBLISH for everything else.** Once the client has
  sent PUBREC it owns the message [MQTT-4.3.3-8]; resending the PUBLISH would ask it to take
  ownership twice.
- **A resent PUBLISH takes send quota, a resent PUBREL does not.** The quota counts PUBLISH
  packets at QoS > 0 and is re-initialised per network connection (MQTT-5.0 §4.9), so a client
  that reconnects with a smaller Receive Maximum than it left gets its arrears in instalments.
- **`session.unacknowledged` returns copies ordered by a send sequence number.** Packet
  Identifiers cannot order the set — they cycle and wrap. Copies, because the caller resends
  without the session lock while acknowledgements are deleting entries underneath, and because
  setting DUP on the packet the session is holding would be a write shared with a delivery loop
  that may still be draining on the displaced connection.
- **The snapshot says what to expect; the live entry decides.** `resend` re-reads the entry
  after the quota wait and immediately before the write. The wait is unbounded, and a displaced
  connection keeps decoding the packets its socket had already buffered, so an acknowledgement
  for a later entry can land while an earlier one is waiting for room. Acting on the stale
  snapshot would resend a completed exchange — and the client's acknowledgement of *that*
  arrives for a Packet Identifier the session no longer holds, which `handlePuback` answers by
  disconnecting the client. A microsecond window between the re-read and the write remains; it
  is a roadmap entry, not a claim of atomicity.

`handlePubrec` also moves its `awaitingPubcomp = true` behind a new `session.awaitPubcomp`, so
that field is read and written under the session lock rather than only written under it, and
gains the QoS check that `handlePuback` already had — without it a PUBREC naming a QoS 1
identifier would leave an entry that is resent as a PUBREL on every resumption for ever.

## Two defects this uncovered in code it did not set out to change

Both were found by reviewing the diff, both are reachable without it, and both turn from a
bounded loss into a permanent one once a session starts resending. Fixing them is part of this
change rather than a follow-up, because shipping the resend without them would make the broker
worse than it was.

**A packet too large for the client stranded its send-quota slot for ever.** `conn.write`
discards a packet exceeding the client's Maximum Packet Size and returns nil, which is half of
[MQTT-3.1.2-24]. The other half was missing: "Where a Packet is too large to send, the Server
MUST discard it without sending it and then behave as if it had completed sending that
Application Message" [MQTT-3.1.2-25]. The entry stayed in flight awaiting an acknowledgement
that could never come, holding a Packet Identifier and a quota slot until the session ended —
so a client that reconnected with a smaller Maximum Packet Size than it left could reach a state
where *nothing* at QoS 1 or 2 ever arrived again, with no error visible to it. `writePublish`
now reports whether the packet reached the socket, and both `deliver` and `resend` complete the
exchange when it did not.

**The send quota was a bare count, and the in-flight set outlives the connection that filled
it.** A resumed session's entries spent their slots on the *previous* connection's quota, but
their acknowledgements arrive on the new one and were draining it. So a client that reconnected
advertising Receive Maximum 1 and then acknowledged what it had received before could be handed
several concurrent QoS 1 publications — against the number it had just said it could hold. The
slot is now recorded on the entry (`outbound.quotaHeld`), cleared for every carried-over entry
in `session.attach`, and returned only by the acknowledgement that completes the entry holding
it. That also settles the PUBREL case: a resent PUBREL spends no quota, so the PUBCOMP answering
it returns none.

§4.9's own model — a counter capped at the initial send quota — permits both of those, because
it counts acknowledgements rather than outstanding messages. This broker does better than the
letter of it, on the grounds that a device advertising Receive Maximum 1 means it.

## Alternatives considered

- **A `retransmitting` flag consulted by `deliver`, with the resends pushed through the normal
  delivery queue.** Rejected: `deliver` allocates a new Packet Identifier and a resend must reuse
  the original, so the queue would need a second kind of entry and `deliver` a branch on it.
  More moving parts for the same ordering guarantee the head-of-loop version gets for free.
- **Resending from `negotiate`, before the CONNACK.** Rejected: nothing may go to the client
  before the CONNACK, and the quota wait would block the handshake goroutine.
- **Persisting the in-flight set into the session record now, so a resume on another broker
  resends too.** Rejected as a separate roadmap entry: the record is a JetStream KV value and the
  payloads do not belong in it, so doing it properly means the durable consumer that the offline
  queue needs anyway. Shipping half of it would be a promise the broker cannot keep — which is
  exactly why the in-flight identifiers were left out of the record in the first place.

## Libraries added or used

None. The change uses `sort` from the standard library and the module's own `packet` package.

The tests add one new usage of an existing dependency, `github.com/eclipse/paho.golang`:
`ClientConfig.EnableManualAcknowledgment`, which stops Paho acknowledging a received message
until `Client.Ack` is called. That is what lets a test leave a QoS 1 delivery unacknowledged:

```go
sub, _ := connectClient(t, addr, durableConnect("q1", 300), manualAck) // sets the flag
got := sub.expectMessage()                                            // no PUBACK sent
require.NoError(t, got.Ack())                                         // now it is
```

Paho batches manual acknowledgements through a ticker (50 ms by default) and sends them in
receipt order, so `Ack` is not synchronous with the PUBACK reaching the broker.

## Trade-offs and risks

- **Only a resume on the same broker process resends.** A session restored from the JetStream
  record on another broker, or after a restart, has no in-flight set to resend — the record does
  not carry one. So the guarantee this adds is real but narrower than [MQTT-4.4.0-1] taken
  literally. The README and the roadmap say so, and
  `TestPersistentSessionOnAnotherBrokerHasNothingToResend` pins the boundary so it cannot drift
  into being assumed wider.
- **A client that reconnects and never acknowledges holds send quota for the life of the
  session.** That is the same exposure a client which stops acknowledging mid-connection already
  has, bounded the same way, by Receive Maximum.
- **An in-memory resume still does not re-run `Options.Authorizer`.** It never did — only
  subscriptions restored from the durable record are re-checked — so a permission revoked
  between two connections goes on delivering until the session expires. This change hands over
  the buffered payloads too, which is strictly less than the future messages the live
  subscription already carries, so it widens the consequence rather than the hole. It is now
  the top entry on the roadmap.
- **The check-then-write in `resend` is not atomic.** See above; the long window is closed, the
  short one is a roadmap entry.
- **The QoS 2 tests are driven by a hand-rolled client using this module's own codec**, because
  the behaviour under test is an acknowledgement being withheld mid-handshake and no conforming
  third-party client will do that on request. They are worth less as conformance evidence than
  the Paho tests around them; `rawClient`'s doc comment says so at the point of use.
- **Ordering is guarded by a unit test, not the end-to-end one.** Go randomises map iteration
  less than the word suggests: for a set that fits one bucket it comes out in insertion order
  whenever the random start slot lands at or past the last entry. Measured against an
  implementation with the ordering removed, the five-message end-to-end order test passed 6 runs
  in 8. The unit test asks 64 times and fails 20 runs in 20.
