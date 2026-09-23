# Re-authorising a session resumed from this broker's memory

**Date:** 2026-09-23

## Goal

Run `Options.Authorizer` over a resumed session's subscriptions when the session came from this
broker's memory, and withdraw what the resuming connection may no longer have — the live NATS
subscriptions and the unacknowledged messages that would otherwise be resent to it.

## What it fixes

A session is keyed by its Client Identifier alone (MQTT-5.0 §4.1), so whoever the Authenticator
lets use that identifier inherits the session and everything it is subscribed to. `handshake`
already knew that for the *stored* branch: `mayResume` re-runs the Authorizer over every
subscription rebuilt from the durable record, and drops the denied ones.

A session resumed from this broker's memory got no such check. `resumeSubscriptions` took its
`len(stored) == 0` early return and the NATS subscriptions — which were never torn down —
went on delivering. So a permission revoked between two connections of the same client to the
same broker did not take effect until the session expired. That is the common case, not the
exotic one: a client that reconnects usually reconnects to the broker it just left.

Two things were leaking, and they share one fix:

1. **The subscription itself**, covering every future message on the filter.
2. **The unacknowledged in-flight set.** Since #13 (retransmission on resume), a resumed session
   hands the previous connection's undelivered payloads over at CONNACK time, before the client
   has sent anything. A payload the client is no longer allowed to receive would be re-sent to it.

## Approach chosen

`conn.reauthoriseLive`, called on the in-memory branch of `resumeSubscriptions`, before the
CONNACK and before `deliverLoop` starts.

- **Walk the live `subs` map, not the record.** On this branch there is no record to walk — for a
  broker without `PersistentSessions` there never is one. The live set is the only statement of
  what the session holds.
- **Remove from the session, then unsubscribe**, in the order `handleUnsubscribe` already uses. A
  message already in the NATS client's hands is delivered against the `*subscription`, so the
  reverse order leaves a window with neither guard in place.
- **`mayResume` now takes a filter and a QoS** rather than a `storedSubscription`, so both branches
  ask the Authorizer the same question. The `AuthzRequest` it builds is unchanged.
- **Withdraw in-flight entries whose topic no longer matches any surviving filter**, and only those
  that would put a *payload* back on the wire. An entry past its PUBREC owes a PUBREL, which
  carries no payload; withholding it would leave the client waiting for a PUBCOMP forever and its
  Packet Identifier unusable, to protect a disclosure that already happened.
- **A withdrawn Packet Identifier is remembered, and an acknowledgement for it is ignored** rather
  than answered with `0x82 Protocol Error`. Without this the change would have introduced a
  deterministic version of the race the roadmap records under "A late acknowledgement can get a
  client disconnected": a client that flushes owed acknowledgements on reconnect would be
  disconnected for acknowledging a message the broker had itself sent it.

## Alternatives considered

- **Discard the whole session when any filter is denied.** One line, and it makes the client
  re-subscribe to everything. Rejected: it turns a narrowed permission into a session loss,
  discarding the in-flight set and the QoS 2 receive state for filters that are still permitted.
  The stored branch does not do this either, and the two branches should not disagree about what a
  denial means.
- **Re-authorise on delivery instead of on resume.** Checking every message against the Authorizer
  as it arrives would close this and several other gaps at once. Rejected for cost: it puts a
  user-supplied call on the hot path of every delivered message, and the Authorizer interface is
  documented as a connection-time and subscribe-time decision.
- **Keep the withdrawn in-flight entry and suppress only the resend.** Rejected as more state for
  the same outcome: the entry would sit in the set holding a Packet Identifier for the life of the
  session, and every future resumption would have to re-decide whether to skip it. Completing it
  matches what `writeResend` already does for a message too large for the client to accept
  [MQTT-3.1.2-25].

## Libraries added or used

None. The change is confined to `handshake.go`, `session.go` and `publish.go`, and uses
`topic.Match`, which already implements MQTT-5.0 §4.7 filter matching including the shared-filter
rule from §4.8.2.

## Trade-offs and risks

- **The Authorizer is now called during every resumed handshake**, once per live filter. A slow
  Authorizer therefore delays a CONNACK in proportion to the session's subscription count. The
  stored branch already had this cost; the in-memory branch did not.
- **A denied filter is dropped silently.** The CONNACK has no per-filter Reason Code, so the client
  learns about it only by not receiving. It is logged at warn level. This matches the stored
  branch, and it is why the log line names the filter and the client.
- **A withdrawn identifier is remembered for the life of the session**, one `uint16` each, bounded
  by the 65535 identifiers that exist. The entry is deleted when the client acknowledges it, so the
  set only grows for clients that never do. `nextID` treats a withdrawn identifier as in use, which
  is what the first draft got wrong: an identifier still owed an acknowledgement, handed to a new
  message, lets the late acknowledgement complete the wrong one. The wire tests cannot reach it —
  identifiers are handed out in order, so a reuse is 65535 sends away — so it is pinned by
  `TestNextIDSkipsAWithdrawnIdentifier` against the session type directly.
- **Nothing re-authorises a session that stays connected.** A permission revoked while the client
  is online still takes effect no earlier than its next reconnect. That is unchanged by this work
  and is what the "re-authorise on delivery" alternative above would address.
