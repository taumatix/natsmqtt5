# Changelog

What changed for someone depending on this broker. The format is
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[semantic versioning](https://semver.org/spec/v2.0.0.html) — the public Go API
is additive within a major version.

This file starts at v0.3.0. The three releases before it are summarised from
their merged pull requests, which is less than they deserve and all that can
honestly be reconstructed now.

## [Unreleased]

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
[Unreleased]: https://github.com/taumatix/natsmqtt5/compare/v0.3.1...HEAD
[0.3.1]: https://github.com/taumatix/natsmqtt5/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/taumatix/natsmqtt5/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/taumatix/natsmqtt5/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/taumatix/natsmqtt5/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/taumatix/natsmqtt5/releases/tag/v0.1.0
