# Changelog

What changed for someone depending on this broker. The format is
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[semantic versioning](https://semver.org/spec/v2.0.0.html) — the public Go API
is additive within a major version.

This file starts at v0.3.0. The three releases before it are summarised from
their merged pull requests, which is less than they deserve and all that can
honestly be reconstructed now.

## [Unreleased]

### Fixed

- **A connection that drops while behind no longer loses what was waiting for it.** The messages
  queued for a client, and the stretch of the offline queue a catch-up had still to send, used to
  be discarded when its connection ended, and the resume replay started only 2 seconds before
  the disconnect, so a client minutes behind lost those minutes [MQTT-3.1.2-23], [MQTT-4.5.0-1].
  The replay now starts at the lowest queue sequence the client had not been sent. It also no
  longer gives up after 30 seconds: a slow client with a long backlog gets all of it, at its own
  pace. A session restored from the session store still replays from the time it was released.

## [0.10.0] - 2026-10-08

**Behaviour change for existing deployments.** The offline queue is now on by default whenever
the broker uses JetStream, so a broker upgraded from v0.9.x with no configuration change starts
creating the `MQTT5_queue` stream and storing every QoS 1 and 2 message for up to 24 hours. Size
JetStream storage for that, set `OfflineQueueMaxAge` to change the window, or set
`DisableOfflineQueue` (`-no-offline-queue`) to keep the old behaviour. No Go API was removed.

### Changed

- **The offline queue is on by default whenever the broker uses JetStream**, which it does with
  retained messages (the default) or `PersistentSessions`. MQTT 5 requires it: QoS 1 and 2
  messages pending for a session are session state that outlives a disconnect
  [MQTT-3.1.2-23], [MQTT-4.5.0-1], and until now a resumed session was told Session Present 1
  and given none of what was published while it was away. What changes for an existing
  deployment that set nothing:
  - A new stream, `MQTT5_queue` (file storage, 24 hours by default), holds every QoS 1 and 2
    message published through a broker. Size its JetStream storage for a day of that traffic,
    or set `OfflineQueueMaxAge`.
  - A QoS 1 or 2 PUBACK or PUBREC now waits for JetStream to store the copy, one extra round
    trip per publish.
  - If the stream cannot be created, the broker logs a warning and runs as before rather than
    failing to start.
  - A broker with `DisableRetained` and without `PersistentSessions` is unchanged: it does not
    use JetStream and has no queue.

- **A shared subscription's QoS 1 and 2 messages go only to members whose client is connected**,
  on any broker, and wait for the first member back when none is. Before, NATS chose among all
  members, and a member whose client was away dropped its share: a probe lost 27 of 40 messages
  [MQTT-4.5.0-1], [MQTT-4.1.0-2]. Each shared subscription now has a durable consumer on the
  offline-queue stream (`MQTT5_share_<hash>`), removed by JetStream once no member has pulled for
  longer than `MaxSessionExpiry`. It needs the offline queue; without it, shared subscriptions
  behave as before. Brokers sharing a `SubjectPrefix` must agree on whether the queue is on.

- **A connected client that falls behind no longer loses QoS 1 and 2 messages.** Past 2048
  messages waiting for one connection the broker dropped them [MQTT-4.1.0-1], [MQTT-4.5.0-1]. With
  the offline queue on, the connection now reads the stream from the first message it could not
  hold, at the client's pace, in order, and returns to live delivery once it has caught up. A
  message's live copy carries its stream position in a new `Mqtt5-Queue-Seq` header. QoS 0, and
  everything when the queue is off, is still dropped at that point.

### Added

- `Options.DisableOfflineQueue` and `-no-offline-queue` (`NATSMQTT5_NO_OFFLINE_QUEUE`) turn the
  queue off. `OfflineQueue` and `-offline-queue` now mean the queue is required: a broker that
  cannot create its stream does not start. Setting both is `ErrInvalidOptions`.

## [0.9.1] - 2026-10-07

A security fix. No API change and no configuration change.

### Security

- **A client could replace any retained message, and read the offline queue, through the
  broker's internal subjects.** The retained store and the offline queue keep their messages
  under `<prefix>.$retained.>` and `<prefix>.$queue.>`, which is inside the subject space MQTT
  topics map onto. A PUBLISH to `$retained/news` replaced the retained message for `news`, with
  the `Authorizer` asked only about `$retained/news`, so a permission on `news` itself did not
  apply. A SUBSCRIBE to `$queue/#` (with `OfflineQueue` on) received a copy of every QoS 1 and
  QoS 2 message on the broker, and to `$retained/#` every retained-message write. Topic Names and
  Filters whose first level is `$retained` or `$queue` are now refused: PUBLISH with `0x90`, SUBSCRIBE
  with `0x8F`, a Will with a `0x90` CONNACK. Other `$` topics are unaffected. Found by a
  conformance review against the MQTT 5 specification. The retained-message forgery affects
  every release with retained messages; reading the queue affects v0.7.0 to v0.9.0 with
  `OfflineQueue` on (reproduced on v0.9.0: a `$queue/#` subscriber received another client's
  `private/payroll` message).

  **Check your retained messages** if untrusted clients could publish: any retained message may
  have been replaced. A session that had subscribed to one of these filters before the upgrade
  fails to restore from the session store; reconnecting with Clean Start clears it.

## [0.9.0] - 2026-10-07

### Changed

- **The minimum Go version is now 1.26** (was 1.25). If you build on Go 1.25, stay on v0.8.0.
  Go 1.25 has been out of upstream security support since 2026-08-19, and the NATS releases below
  declare `go 1.26.0`, so the floor could not stay without holding them back.
  ([#11](https://github.com/taumatix/natsmqtt5/issues/11))
- `github.com/nats-io/nats.go` v1.53.1 → v1.54.0, the NATS client the broker is built on.
- `github.com/nats-io/nats-server/v2` v2.14.5 → v2.15.0, used only by the tests.
- The Docker image builds with `golang:1.26-alpine`, and CI tests Go 1.26 and the current stable
  release.

### Security

- `golang.org/x/crypto` v0.55.0 → v0.57.0, past [GO-2026-6354] and [GO-2026-6355], two DoS
  advisories in `x/crypto/ssh`. `govulncheck` never found them on a call path from this module,
  which does not speak SSH, but they could not be patched from a Go 1.25 floor.

### Fixed

- **`compose.yaml` started v0.4.2, whatever release you downloaded it from.** Its two
  `image:` lines were not bumped from v0.4.3 to v0.8.0, so the compose quickstart the README
  hands out at each tag ran the v0.4.2 broker. At this tag it starts v0.9.0. A new test,
  `TestReleaseVersionsAgree`, fails when `compose.yaml` or a README install line names a version
  other than the newest one in this file.

[GO-2026-6354]: https://pkg.go.dev/vuln/GO-2026-6354
[GO-2026-6355]: https://pkg.go.dev/vuln/GO-2026-6355

## [0.8.0] - 2026-10-05

### Added

- **The offline queue covers sessions restored from the session store.** With
  `PersistentSessions` and `OfflineQueue`, a client that comes back after a broker restart, or to
  another broker, now gets what was published while it was away, as one returning to the broker
  that held it in memory already did. The stored record carries when the session was released
  (`AwayAt`), and the claim that restores it clears that again, so a later claim cannot replay an
  old absence. A restored replay starts at the release, not just before it. A broker killed
  without releasing its sessions records no release, and nothing is replayed for them.

## [0.7.0] - 2026-10-05

### Added

- **An offline queue (`Options.OfflineQueue`, `-offline-queue`).** QoS 1 and 2 messages
  published while a session is disconnected used to be dropped for it. With the queue on, they
  are kept in a JetStream stream (`MQTT5_queue`, for `OfflineQueueMaxAge`, 24 hours by default)
  and delivered when the session resumes. Delivery is in publish order, before live traffic, and
  exactly once across the drop and the resume. This covers sessions held in the memory of the
  broker the client returns to. Sessions restored from `PersistentSessions` come next (see
  ROADMAP.md).
  The queued copy is stored, and confirmed by JetStream, before the message is published live
  and acknowledged. That adds a JetStream round trip to each QoS 1 and 2 publish, and refuses
  the publish if JetStream cannot store it. Storing it afterwards could lose a message published
  just before a client resumed. CI caught that on the first version, and it is reproduced in
  the tests by delaying the store.
- `Options.OfflineQueueMaxAge`, `OfflineQueueStorage` and `OfflineQueueReplicas`.

### Changed

- **Every message the broker publishes carries a `Mqtt5-Msg-Id` header**, so its live delivery
  and its queued copy are known to be the same message. NATS-native subscribers see one more
  header.

## [0.6.0] - 2026-10-05

### Added

- **`Broker.Reauthorize` reaches the broker holding the session, from any broker.** With several
  brokers on one NATS cluster, a deployment had to call it on the one serving the client, which it
  had no easy way to know. Brokers sharing a `SubjectPrefix` now listen on
  `_NATSMQTT5.reauthorize.<prefix>`. A broker that does not hold the session asks there, and the
  one that does runs the sweep and answers. When none does, `Reauthorize` returns nil after
  `ReauthorizeForwardWait` (one second) or when its context ends.
- `ReauthorizeForwardWait`.

### Security

- Anything able to publish on `_NATSMQTT5.reauthorize.<prefix>` can make a broker re-run its
  `Authorizer` over a session. That can only narrow the session, but an `Authorizer` outage turns
  it into removed subscriptions. If untrusted clients share your NATS, restrict `_NATSMQTT5.>` to
  the brokers' NATS user.

## [0.5.0] - 2026-10-04

### Added

- **`Broker.Reauthorize(ctx, clientID)`: revoke a permission from a client that is still
  connected.** The `Authorizer` was asked at SUBSCRIBE, at publish, at CONNECT for the Will, and
  at resume, never while a client simply stayed connected. A client that kept its connection for
  a week kept receiving on a filter its principal lost on day one. `Reauthorize` re-runs the
  `Authorizer` over the session's subscriptions and Will:
  - a denied subscription stops delivering at once, including messages already queued for the
    connection;
  - its unacknowledged messages are withdrawn, so a resume does not resend them, and their
    acknowledgements still return the send-quota slot;
  - a denied Will is discarded, even one waiting out its Will Delay Interval.

  The connection stays up. Call it from whatever tells you about revocations.

### Fixed

- A message queued for a subscription that was then removed (by an UNSUBSCRIBE, or by
  `Reauthorize`) is no longer delivered after the removal, including one waiting for room in the
  client's Receive Maximum. MQTT-5.0 §3.10.4 allows either behaviour after an UNSUBSCRIBE.
- A delayed Will cancelled by a resumption stays cancelled even if the resumed connection ends
  before the delay does. Before, the original timer published it when it woke.
- `Broker.Close` no longer clears the NATS connection it drains, which raced with `Serve`'s
  start-up and with delayed Wills firing on shutdown. `Broker.NATS()` after `Close` now returns
  the drained connection rather than `nil`.

## [0.4.3] - 2026-10-04

A fix. No API change and no configuration change.

### Fixed

- **A takeover could get the new connection disconnected for acknowledging a message.** When a
  connection is taken over, an acknowledgement it had already buffered is still applied: the
  client did receive that message, and the session belongs to the Client Identifier
  (MQTT-5.0 §4.1). But by then the new connection may have resent the same message, and its
  client acknowledges that copy too. The broker answered that second acknowledgement with
  `0x82 Protocol Error` and closed the connection. It now ignores it, and does not reuse the
  Packet Identifier before it arrives.
- **The same race cost the new connection a send-quota slot.** The slot its resend took was
  returned to the old connection's quota instead, so the new connection could send one fewer
  message at a time from then on. With a Receive Maximum of 1, delivery stopped. The slot now
  comes back to the connection that spent it.

## [0.4.2] - 2026-10-04

A security fix. No API change and no configuration change.

### Security

- **A connection being taken over could remove its successor's subscriptions.** A connection
  displaced by a second CONNECT on its Client Identifier goes on decoding what its socket had
  already buffered. An UNSUBSCRIBE the client sent before the takeover was handled against the
  session the new connection now owned: it tore down the new connection's filter, and that client
  was never told. Reproduced end to end over a real NATS server before the fix, with a Keep Alive
  of 0. With a non-zero Keep Alive the race is narrower but not closed. UNSUBSCRIBE now checks
  that its connection still owns the session in the same locked step that removes the filter, and
  a displaced connection's UNSUBSCRIBE is refused. Its client never received an UNSUBACK, so it
  cannot have assumed the unsubscribe happened.
- Acknowledgements from a displaced connection are still applied, deliberately. They come from the
  same client, about messages it received, and dropping them makes the successor resend a message
  the client already acknowledged or refused.

## [0.4.1] - 2026-09-28

A security fix. No API change and no configuration change.

### Security

- **A connection being taken over could install a subscription into the
  session it had just lost.** An `Authorizer` call is not interrupted by a
  takeover, so a SUBSCRIBE decided on the old connection's principal could
  finish after a second CONNECT had claimed the Client Identifier — under a
  principal that may not read that filter — and after that connection's resume
  check had already run. The filter was installed anyway, and its messages were
  delivered to the new principal. Reproduced end to end over a real NATS server
  before the fix. A subscription is now installed only while the connection that
  asked for it still owns the session, checked under the lock the takeover
  takes; otherwise it is torn down and answered `0x80` on a socket nothing
  reads. The same check guards filters restored from a stored session record.
- Its NATS subscription no longer delivers until it is installed. The NATS side
  is bound first, and the handler delivers to whichever connection holds the
  session at the time, so a subscription refused at install could briefly carry
  messages to the wrong connection.

## [0.4.0] - 2026-09-27

A security fix with one behaviour change to check before upgrading: an
`Authorizer` is now asked about every Will Message, and one that denies a Will's
topic refuses that client's CONNECT. The Go API change is one new field, so
existing code compiles unchanged.

### Security

- **A Will Message now goes past the `Authorizer`** ([#24]). Before this, the Will was
  checked for QoS, RETAIN and topic syntax and then published with no
  authorization at all, so a principal denied `ActionPublish` on a topic could
  publish — and retain — to it by setting it as the Will and dropping its
  connection, at a moment of its choosing via the Will Delay Interval. The Will
  is now asked about at CONNECT as an `ActionPublish` under the connecting
  principal, and a denial refuses the connection with `0x87 Not authorized`.
  The check runs before the session is taken over, so a refused CONNECT does not
  displace a live connection on the same Client Identifier.

  **Check your `Authorizer` before upgrading** if your clients set Wills: one
  that denies publishing to a Will's topic now refuses those clients' CONNECTs
  where it used to let them in. A deployment without an `Authorizer` is
  unaffected, and so is a CONNECT without a Will.

### Added

- **`AuthzRequest.Will`** ([#24]), set only on that check and only with `ActionPublish`,
  so an implementation that audit-logs publishes can tell a Will — which may
  never be sent — from a PUBLISH the client made. The zero value is every other
  call site, so an existing `Authorizer` compiles unchanged.

## [0.3.1] - 2026-09-26

Documentation only. No code changed, so upgrading from 0.3.0 changes nothing at
runtime — this release exists because the instructions inside the `v0.3.0` tag say
something that is no longer true, and a tag cannot be edited.

### Fixed

- **The container image is public**, so `docker run ghcr.io/taumatix/natsmqtt5` and
  the `compose.yaml` quickstart work with no credential ([#7]). The README and
  `compose.yaml` carried a warning that they would fail with `denied`; that warning
  was correct until today and is now removed. Verified anonymously: every published
  tag resolves for `linux/amd64` and `linux/arm64`, with a nonexistent tag returning
  404 in the same run to prove the check discriminates.

  `v0.1.0` has no image and never did — the release workflow was added after that
  tag — so the oldest pullable version is `v0.1.1`.

### Added

- CI pulls the **published** image anonymously and drives the documented quickstart
  against it. The existing compose smoke test overrides `image:` with a locally
  built `natsmqtt5:ci`, which is right for testing the working tree and means it
  never checked that the thing users are told to run is reachable. That gap is why
  the image stayed unpullable for fourteen days with CI green throughout.

## [0.3.0] - 2026-09-26

### Added

- **The Authorizer is consulted while a session is resumed** ([#19]). A session
  is keyed by its Client Identifier alone, so a permission narrowed between two
  connections of the same client used not to take effect until the session
  expired — and the common case is a client reconnecting to the broker it just
  left. Every live Topic Filter of a resumed session now goes back past the
  `Authorizer` before the CONNACK, and a filter the connection may no longer
  hold is unsubscribed from NATS.

  Two things to plan for if you implement `Authorizer`:

  - It is now called once per live filter during a resumed handshake, so a slow
    implementation delays the CONNACK in proportion to the session's size. The
    `ctx` carries no deadline of the broker's making.
  - **A denial there is silent and final.** A CONNACK has no per-filter Reason
    Code, so the filter is torn down, logged at warn level, and the client is
    told `SessionPresent: true` with nothing to say a subscription is missing.
    With `Options.PersistentSessions` the reduced set is written straight to the
    durable record. An error returned because your policy store was unreachable
    is therefore indistinguishable from a decision to deny, and costs the client
    its subscription permanently — refuse the connection from the
    `Authenticator` instead if you cannot decide.

- **`AuthzRequest.Resume`** ([#19]), set only on a re-check of this kind and
  only with `ActionSubscribe`. The client performed no action and the same
  filters return on every reconnect, which matters if you audit-log, meter or
  rate-limit subscriptions — and it is the cheap way out of the added CONNACK
  latency, since you may answer a resume from a cache you would not trust for a
  fresh SUBSCRIBE. The zero value is the behaviour of v0.2.0 at both existing
  call sites, so an existing `Authorizer` compiles and behaves unchanged.

- **QoS 1 and QoS 2 retransmission on resume** ([#13]). What the previous
  connection left unacknowledged is resent when the session is taken over: the
  PUBLISH packets with DUP set and their original Packet Identifiers, and the
  PUBRELs for exchanges already past their PUBREC. Before this, a message
  accepted at QoS 1 or 2 was lost if the connection dropped between delivery and
  its acknowledgement.

### Fixed

- **A packet larger than the client's Maximum Packet Size stranded a send-quota
  slot for the life of the session** ([#13]). It was discarded without being
  completed; [MQTT-3.1.2-25] says the server must behave as if it had finished
  sending. A client with a small Receive Maximum eventually stopped receiving
  anything.
- **Send quota was a bare count while the in-flight set outlives the connection
  that filled it** ([#13]), so a client reconnecting with Receive Maximum 1
  could be handed several concurrent publications after acknowledging what it
  had received before.
- **A withdrawn Packet Identifier could be reallocated to a new message**
  ([#19]). When a resumed session loses a filter, the messages it had earned are
  taken back — but the client was sent those messages and may still owe an
  acknowledgement. `nextID` looked only at the in-flight set, so the identifier
  was free to be reused, and the late acknowledgement would complete whichever
  new message then held it, which was silently never resent. PUBACK and PUBCOMP
  now ignore a withdrawn identifier rather than answering 0x82 Protocol Error,
  which would disconnect a conforming client for the broker's own withdrawal.
- A message already past its PUBREC is **not** withdrawn: the client owns it
  [MQTT-4.3.3-8] and what is outstanding is a PUBREL carrying no payload, so
  withholding it would strand a Packet Identifier to prevent a disclosure that
  already happened.

### Changed

- `Options.Authorizer`'s documentation said a denied subscription is answered
  with 0x87 in the SUBACK. That is no longer true for a whole class of denials:
  on a resume there is no SUBACK and no packet of any kind.
- The README's `Authenticator` example and the runnable `Example_authentication`
  showed an implementation that verifies a principal and accepts any Client
  Identifier — which is exactly how one principal inherits another's session.
  Both now bind the identifier to the principal.

### Documentation

- [`UPSTREAM.md`](UPSTREAM.md) declares what this broker was built against — the
  MQTT v5 specification, the `nats-server` release whose subject mapping is
  copied, and the Paho client the end-to-end tests drive it with — each with the
  date it was last checked ([#12]).

### Known limitation — resolved in 0.3.1

The published container image was not public when 0.3.0 shipped, so `docker run
ghcr.io/taumatix/natsmqtt5:v0.3.0` failed with `denied`. The image existed and every
release pushed it; the package's visibility was settable only by a human in the web
UI, which happened later the same day. Tracked in [#7]. `go get`, `go install` and
building from source were never affected. **The tag still carries this warning in
its own README, because a tag cannot be edited — that is what 0.3.1 is for.**

## [0.2.0] - 2026-09-13

### Added

- **Sessions that outlive the broker process** ([#4]) — `Options.PersistentSessions`
  keeps session state in a JetStream key-value store, so a client's
  subscriptions and QoS state survive a restart and can be resumed against a
  different broker in front of the same NATS server.

### Fixed

- A Clean Start deleting its own freshly written session record ([#5]).
- The reported version came from a hardcoded string rather than the build
  information stamped into the binary ([#3]).

## [0.1.1] - 2026-09-12

### Fixed

- A SUBACK was written before the NATS subscription was established, so it
  promised interest that had not been registered yet, and the response was not
  flushed ([#2]).

## [0.1.0] - 2026-09-12

First release: an MQTT v5 broker that speaks to clients over TCP and carries
their messages on an existing NATS server, using the same topic-to-subject
mapping `nats-server` uses for its own MQTT support.

[#2]: https://github.com/taumatix/natsmqtt5/pull/2
[#3]: https://github.com/taumatix/natsmqtt5/pull/3
[#4]: https://github.com/taumatix/natsmqtt5/pull/4
[#5]: https://github.com/taumatix/natsmqtt5/pull/5
[#7]: https://github.com/taumatix/natsmqtt5/issues/7
[#12]: https://github.com/taumatix/natsmqtt5/pull/12
[#13]: https://github.com/taumatix/natsmqtt5/pull/13
[#19]: https://github.com/taumatix/natsmqtt5/pull/19
[#24]: https://github.com/taumatix/natsmqtt5/pull/24
[Unreleased]: https://github.com/taumatix/natsmqtt5/compare/v0.10.0...HEAD
[0.10.0]: https://github.com/taumatix/natsmqtt5/compare/v0.9.1...v0.10.0
[0.9.1]: https://github.com/taumatix/natsmqtt5/compare/v0.9.0...v0.9.1
[0.9.0]: https://github.com/taumatix/natsmqtt5/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/taumatix/natsmqtt5/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/taumatix/natsmqtt5/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/taumatix/natsmqtt5/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/taumatix/natsmqtt5/compare/v0.4.3...v0.5.0
[0.4.3]: https://github.com/taumatix/natsmqtt5/compare/v0.4.2...v0.4.3
[0.4.2]: https://github.com/taumatix/natsmqtt5/compare/v0.4.1...v0.4.2
[0.4.1]: https://github.com/taumatix/natsmqtt5/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/taumatix/natsmqtt5/compare/v0.3.1...v0.4.0
[0.3.1]: https://github.com/taumatix/natsmqtt5/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/taumatix/natsmqtt5/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/taumatix/natsmqtt5/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/taumatix/natsmqtt5/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/taumatix/natsmqtt5/releases/tag/v0.1.0
