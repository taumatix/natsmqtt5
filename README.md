# natsmqtt5

An MQTT v5.0 broker that uses an existing NATS server as its messaging core.

`nats-server` speaks MQTT 3.1.1 and [will not be adding v5][issue3369]: "we don't
want to own the ongoing maintenance cost for the *massive* API surface increase
that comes with MQTT 5 at this time." This is the other option raised in that
thread — a separate broker that keeps NATS as the messaging engine, so you point
it at the NATS cluster you already run rather than deploying a second,
unrelated message bus.

[issue3369]: https://github.com/nats-io/nats-server/issues/3369

> **Built against:** MQTT 5.0 (OASIS, 2019-03-07 — still the current revision), `nats.go` v1.54.0
> and `nats-server` v2.15.0, checked 2026-10-07. Both are the latest releases. Since v0.9.0 this
> module needs **Go 1.26** ([#11](https://github.com/taumatix/natsmqtt5/issues/11)). Go 1.25 left
> upstream security support on 2026-08-19, and both NATS releases declare `go 1.26.0`. Holding the
> floor had kept `golang.org/x/crypto` below the fix for two `x/crypto/ssh` DoS advisories
> ([GO-2026-6354][g54], [GO-2026-6355][g55]). See [UPSTREAM.md](UPSTREAM.md),
> [CHANGELOG.md](CHANGELOG.md) for what each release changed under you, and
> [ROADMAP.md](ROADMAP.md) for where the broker knowingly does not yet conform.

[g54]: https://pkg.go.dev/vuln/GO-2026-6354
[g55]: https://pkg.go.dev/vuln/GO-2026-6355

```
MQTT v5 clients ──TCP/TLS──▶ natsmqtt5 ──▶ your existing NATS server
                             (a NATS client, not a fork)
```

## What it does

- **Speaks MQTT v5 to clients.** All fifteen control packets, all 27 properties.
  Verified against the [Eclipse Paho v5][paho] client over a real socket.
- **Uses NATS for everything underneath.** Topics become NATS subjects,
  subscriptions become NATS subscriptions, shared subscriptions become NATS
  queue groups, retained messages live in a JetStream stream.
- **Interoperates on the subject level.** The topic-to-subject mapping is
  byte-for-byte the one `nats-server` uses for its own MQTT support, so
  `sensors/7/temp` lands on `mqtt.sensors.7.temp` either way. A NATS-native
  publisher reaches MQTT subscribers and vice versa, with no bridge.
- **Scales horizontally by adding brokers.** Two brokers against one NATS server
  are one logical broker: a client on either reaches subscribers on both.
- **Keeps sessions across brokers, if you ask it to.** With
  `PersistentSessions`, a client that reconnects with Clean Start 0 resumes its
  subscriptions on a broker that has never served it, and after a restart. Off
  by default; see [Persistent sessions](#persistent-sessions).
- **Queues messages for a client that is away, if you ask it to.** With
  `OfflineQueue`, QoS 1 and 2 messages published while a session is
  disconnected are delivered when it resumes. Off by default; see
  [Offline queue](#offline-queue).
- **Embeds as a library.** `natsmqtt5.New` returns a `*Broker` you run inside
  your own process, sharing your `*nats.Conn` if you want.

[paho]: https://github.com/eclipse/paho.golang

## Install

As a container, against a NATS server you already run:

```sh
docker run --rm -p 1883:1883 \
  -e NATSMQTT5_NATS=nats://your-nats-server:4222 \
  ghcr.io/taumatix/natsmqtt5:v0.9.0
```

Images are published for `linux/amd64` and `linux/arm64` on every release, as
`:v0.9.0`, `:0.9.0`, `:0.9` and `:latest`. They are built from
[distroless/static][distroless], so there is no shell and no package manager in
them, and the broker runs as a non-root user.

If you have no NATS server yet, [compose.yaml](compose.yaml) starts one with
JetStream enabled and the broker in front of it:

```sh
curl -O https://raw.githubusercontent.com/taumatix/natsmqtt5/v0.9.0/compose.yaml
docker compose up -d
mosquitto_pub -V 5 -h localhost -p 1883 -t sensors/7/temp -m 21.5
```

Every flag has an environment variable twin — upper-case, `-` becomes `_`,
behind a `NATSMQTT5_` prefix — so `-subject-prefix` is
`NATSMQTT5_SUBJECT_PREFIX`. An explicit flag beats the environment. Run
`docker run --rm ghcr.io/taumatix/natsmqtt5:v0.9.0 -h` for the full list.

[distroless]: https://github.com/GoogleContainerTools/distroless

As a binary:

```sh
go install github.com/taumatix/natsmqtt5/cmd/natsmqtt5@v0.9.0
natsmqtt5 -nats nats://localhost:4222 -listen :1883
```

As a library:

```sh
go get github.com/taumatix/natsmqtt5@v0.9.0
```

Requires Go 1.26 or newer (v0.8.0 and earlier build on Go 1.25), and a NATS server with JetStream
enabled (or
`-no-retained` / `Options.DisableRetained` to run without it).

## Use

```go
package main

import (
	"context"
	"log"

	"github.com/taumatix/natsmqtt5"
)

func main() {
	broker, err := natsmqtt5.New(natsmqtt5.Options{
		NATSURL: "nats://localhost:4222",
		Listen:  ":1883",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer broker.Close()

	if err := broker.Serve(context.Background()); err != nil {
		log.Fatal(err)
	}
}
```

Authentication and authorization are two small interfaces:

```go
opts := natsmqtt5.Options{
	Authenticator: natsmqtt5.AuthenticatorFunc(
		func(ctx context.Context, req *natsmqtt5.AuthRequest) (*natsmqtt5.AuthResult, error) {
			if !credentialsOK(req.Username, req.Password) {
				// Choose the CONNACK Reason Code the client sees.
				return nil, &natsmqtt5.ConnectError{Code: packet.BadUserNameOrPassword}
			}
			// A Session is keyed by its Client Identifier alone, so an
			// authenticated user must not be free to choose any. Equality on
			// the first segment, not a prefix test: user "a" passes
			// HasPrefix("a/b/sensor", "a/") and would inherit "a/b"'s session.
			if owner, _, found := strings.Cut(req.ClientID, "/"); !found || owner != req.Username {
				return nil, &natsmqtt5.ConnectError{Code: packet.ClientIdentifierNotValid}
			}
			return &natsmqtt5.AuthResult{Identity: req.Username}, nil
		}),

	Authorizer: natsmqtt5.AuthorizerFunc(
		func(ctx context.Context, req *natsmqtt5.AuthzRequest) error {
			if req.Action == natsmqtt5.ActionPublish && !mayWrite(req.Identity, req.Topic) {
				return errors.New("denied") // becomes 0x87 Not authorized in the PUBACK
			}
			return nil
		}),
}
```

`AuthRequest` carries the TLS `ConnectionState`, so client-certificate
authentication is a few lines.

The Client Identifier check above is not decoration. A Session is identified by
its Client Identifier alone (MQTT-5.0 §4.1), so an `Authenticator` that verifies
a principal but accepts any identifier lets that principal inherit another's
session — its subscriptions, its unacknowledged messages and its QoS 2 receive
state. Bind the identifier to the principal, or — for a client that sends a
zero-length identifier to have one assigned [MQTT-3.1.3-6] — derive one with
`AuthResult.AssignClientID`, as `Example_authentication` shows.

The broker re-runs the `Authorizer` over a resumed session's filters, with
`AuthzRequest.Resume` set, so a permission narrowed between two connections
takes effect on the second one rather than when the session expires. That bounds
the damage; it does not remove the need to bind the identifier, and it has two
costs worth knowing about. A slow `Authorizer` delays the CONNACK in proportion
to the session's filter count. And a denial there is silent — there is no
per-filter Reason Code in a CONNACK, so the filter is torn down and the client
is still told `SessionPresent: true`.

A Will Message goes past the `Authorizer` too, at CONNECT, as an
`ActionPublish` with `AuthzRequest.Will` set. The broker publishes a Will on the
client's behalf after the client has gone, so CONNECT is the only moment it can
be refused: a denied Will refuses the connection with 0x87, and the Will is not
checked again when it fires.

A client that stays connected is not re-checked by itself, since asking on every
delivery would put your `Authorizer` on the hot path of every message. When a
principal's permissions change, call `Broker.Reauthorize(ctx, clientID)`, from
whatever tells you about revocations:

- each subscription goes back past the `Authorizer` (with `Resume` set), and a
  denied one stops delivering at once, including what was already queued;
- its unacknowledged messages are withdrawn, so a later resume does not resend
  them;
- the Will is checked again, and a denied one is discarded, even one already
  waiting out its Will Delay Interval.

The connection stays up, because a DISCONNECT would publish the Will being
revoked. As at a resume, the client is not told which filters it lost, and any
error from the `Authorizer` counts as a denial.

Any broker will do. Brokers sharing a NATS cluster and a `SubjectPrefix` answer
one another on `_NATSMQTT5.reauthorize.<prefix>`, and the one holding the
session runs the sweep. When none holds it, `Reauthorize` returns nil after
`ReauthorizeForwardWait` (one second), since an offline session is re-checked
when it next connects. Anything that can publish on that subject can make a
broker re-ask your `Authorizer`, which can only narrow a session; restrict
`_NATSMQTT5.>` to the brokers' NATS user if your other NATS clients are not
trusted.

## Offline queue

Core NATS delivers a message to whoever is subscribed when it is published and
keeps nothing, so by default a session whose client is disconnected misses what
is published meanwhile. Turn on the queue:

```go
natsmqtt5.Options{OfflineQueue: true, OfflineQueueMaxAge: 24 * time.Hour}
```

or `-offline-queue` / `NATSMQTT5_OFFLINE_QUEUE=true` (and
`-offline-queue-max-age`) for the binary and the image.

When the session resumes, the client gets what its filters matched while it was
away, in publish order, before anything published after it reconnected. A
message is delivered once: what the client received before it dropped and what
arrives live during the resume are not repeated.

What it costs and what it does not cover yet:

- Each QoS 1 and 2 message is published twice: first under
  `<prefix>.$queue.<subject>` into the `MQTT5_queue` stream, which keeps it for
  `OfflineQueueMaxAge` (24 hours by default), and then live. The broker waits
  for JetStream to confirm the first before publishing live and acknowledging
  the client, which adds a JetStream round trip to every QoS 1 and 2 publish.
  If JetStream cannot store it, the publish is refused, so the client tries
  again. QoS 0 is not queued.
- With `PersistentSessions` it also covers a session restored after a restart
  or on another broker: the stored record says when the session was released.
  Two limits there. A broker killed outright never releases its sessions, so
  nothing is replayed for them. And a restored replay starts at the release
  rather than a moment before it, because what the old connection had already
  delivered is not stored, and going back further could deliver a QoS 2 message
  twice. A message caught in the moments the connection went down can be lost
  that way, like the in-flight messages a restored session does not carry.
- A message published straight onto NATS rather than through a broker is not
  queued ([ROADMAP.md](ROADMAP.md)).
- A resume reads everything queued since the client left and matches it in the
  broker, so a long absence on a busy broker takes longer to catch up.

## How MQTT maps onto NATS

| MQTT | NATS |
|---|---|
| Topic `sensors/7/temp` | Subject `mqtt.sensors.7.temp` |
| Topic level `/` | Subject token separator `.` |
| A literal `.` in a topic | `//` |
| An empty topic level | `./` or `/.` depending on position |
| Filter wildcard `+` | `*` |
| Filter wildcard `#` | `>`, plus a second subscription on the parent |
| Shared subscription `$share/g/f` | Queue group on the subject for `f` |
| Retained message | JetStream stream, `MaxMsgsPerSubject: 1` |
| Session state (opt-in) | JetStream key-value bucket, one key per Client Identifier |
| v5 properties | NATS headers (`Mqtt5-Content-Type`, `Mqtt5-User`, …) |

`#` gets two NATS subscriptions because MQTT's `#` matches the parent level
(`sport/#` matches `sport`) and NATS's `>` does not.

The `mqtt` subject prefix is configurable with `SubjectPrefix`; brokers that
should share traffic must agree on it, and brokers that must not share traffic
must differ.

A NATS message with no `Mqtt5-*` headers is delivered as a QoS 0 message with
no properties, which is what makes plain `nats pub` reach MQTT subscribers.

## Persistent sessions

By default a session lives in the broker process: a client reconnecting to the
*same* broker within its Session Expiry Interval resumes its subscriptions, and
one reconnecting to a different broker, or after a restart, has to subscribe
again.

Turn that off with one setting:

```go
natsmqtt5.Options{PersistentSessions: true}
```

or `-persistent-sessions` / `NATSMQTT5_PERSISTENT_SESSIONS=true` for the binary
and the image. Every broker that should share sessions needs it set, and needs
the same `StreamPrefix`, since that names the bucket.

The subscription set is mirrored into a JetStream key-value bucket
(`MQTT5_sessions` by default), keyed by Client Identifier. A CONNECT claims the
record with a compare-and-swap on its revision, so of two brokers racing for one
Client Identifier exactly one wins — and the loser disconnects its client with
`0x8E Session taken over` rather than serving it in parallel [MQTT-3.1.4-3].
That claim is also what makes the `Session Present` flag mean something across a
restart.

[compose.yaml](compose.yaml) runs two brokers this way:

```sh
NATSMQTT5_PERSISTENT_SESSIONS=true docker compose --profile cluster up -d
```

What it costs and what it does not cover:

- One JetStream write per CONNECT, SUBSCRIBE, UNSUBSCRIBE and DISCONNECT. None
  of those are the throughput path; PUBLISH is untouched.
- **The Client Identifier is capped at 128 bytes** (`MaxPersistentClientIDLen`),
  because it becomes part of a NATS subject. A longer one is refused with
  `0x85 Client Identifier not valid`. Without persistence there is no limit.
- **In-flight QoS 1 and QoS 2 messages are not persisted.** A client that
  reconnects to the broker still holding its session gets its unacknowledged
  messages back; one whose session is restored from the store — after a restart,
  or on another broker — does not, because the record carries the subscription
  set and not the in-flight set. Storing Packet Identifiers without the payloads
  to go with them would promise a resend the broker could not perform.
- **A filter the `Authorizer` no longer permits is dropped without telling the
  client.** Every filter of a resumed session goes back past the `Authorizer`
  before the CONNACK, on both branches, so a permission narrowed while the
  client was away takes effect immediately. A CONNACK has no per-filter Reason
  Code to carry the refusal, so the dropped filters are logged and the client
  still sees `SessionPresent: true`; it finds out by not receiving. The
  unacknowledged messages a dropped filter earned go with it.
- **The Will Message is not persisted.** A Will belongs to the network
  connection (MQTT-5.0 §3.1.2.5), and a broker reading a record cannot tell a
  connection that has ended from an owner that is merely busy, so publishing
  from there would announce live clients as dead. A broker that shuts down
  cleanly still publishes its clients' Wills; one killed outright does not.
- **A session whose broker was killed outright is resumed whenever its client
  comes back**, however long that takes. Expiry is measured from the moment a
  session is released, and a `kill -9` records no release. A background sweep
  reclaims such a record once it is older than `MaxSessionExpiry` and its broker
  has stopped answering.

## What it does not do

Stated plainly, because a broker you cannot trust the limits of is worse than
one with fewer features:

- **No offline message queue.** Messages published while a session is
  disconnected are not stored for it, whether or not the session is persisted.
- **Retransmission on reconnect reaches as far as the broker's memory.** A
  client that reconnects with Clean Start 0 to a broker still holding its
  session is resent the PUBLISH and PUBREL packets that connection left
  unacknowledged, with their original Packet Identifiers [MQTT-4.4.0-1]. A
  session restored from the store instead — after a restart, or on another
  broker — has nothing to resend.
- **Shared subscriptions are granted QoS 1 at most.** A QoS 2 request on a
  `$share/` filter is answered with `Granted QoS 1` in the SUBACK, which
  [MQTT-3.8.4-7] permits. Exactly-once delivery to *one* member of a group needs
  per-member ownership of a half-finished PUBREC/PUBREL exchange, and
  JetStream's unit of ownership is a consumer, not a queue-group member.
- **No enhanced authentication (`AUTH` packets).** A CONNECT carrying an
  Authentication Method is refused with `0x8C Bad authentication method`, which
  is the conforming answer from a server that does not support it.
- **The broker never sends topic aliases**, though it accepts them from clients.
- **`Message Expiry Interval` is forwarded but not enforced** on stored
  retained messages.
- **A PUBACK does not mean the message reached NATS.** The broker hands the
  message to its NATS connection and acknowledges; the write is flushed
  microseconds later. A broker killed in that window loses a QoS 1 message it
  has already acknowledged. A SUBACK, by contrast, does wait for the NATS
  server to confirm the subscription — otherwise a message published
  immediately after subscribing could be dropped, which is a loss the client
  has no way to notice.
- **Topics containing a space, a tab, `*`, `>` or `DEL` are refused** with
  `0x90 Topic Name invalid`. MQTT permits them; a NATS subject cannot carry
  them. `nats-server` lets `*` and `>` through, which silently turns an MQTT
  subscription to `a/*/b` into a NATS wildcard that receives topics it must not
  see. Failing loudly is the deliberate choice here; see
  [ROADMAP.md](ROADMAP.md) for the escaping option.

## Correctness

The MQTT layer is written against the [OASIS MQTT v5.0 Standard][spec] of
07 March 2019, and the tests cite it:

- The codec tests assert the literal bytes from the spec's own worked examples
  (Figures 3-6, 3-9, 3-19, 3-21) and every boundary in Table 1-1, rather than
  only round-tripping our own encoder against our own decoder.
- The topic mapping is checked against all 40 conversion fixtures from
  `nats-server`'s `TestMQTTTopicAndSubjectConversion` and
  `TestMQTTFilterConversion`, so a divergence from `nats-server` fails the
  build.
- The matching tests use the examples in §4.7.1.2, §4.7.1.3, §4.7.2 and §4.8.2.
- Integration tests run a real NATS server in-process with JetStream, a real
  broker, and the Eclipse Paho v5 client over a real TCP socket.
- The packet decoder and the topic mapping are fuzzed in CI.

[spec]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html

## Packages

| Package | What it is |
|---|---|
| `github.com/taumatix/natsmqtt5` | The broker |
| `.../packet` | MQTT v5 wire codec — standard library only, no NATS |
| `.../topic` | Topic validation, matching and the NATS subject mapping |

`packet` and `topic` are usable on their own; neither imports NATS.

## Licence

Apache 2.0. See [LICENSE](LICENSE).
