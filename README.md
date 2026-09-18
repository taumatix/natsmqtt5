# natsmqtt5

An MQTT v5.0 broker that uses an existing NATS server as its messaging core.

`nats-server` speaks MQTT 3.1.1 and [will not be adding v5][issue3369]: "we don't
want to own the ongoing maintenance cost for the *massive* API surface increase
that comes with MQTT 5 at this time." This is the other option raised in that
thread — a separate broker that keeps NATS as the messaging engine, so you point
it at the NATS cluster you already run rather than deploying a second,
unrelated message bus.

[issue3369]: https://github.com/nats-io/nats-server/issues/3369

> **Built against:** MQTT 5.0 (OASIS, 2019-03-07) and `nats-server` v2.14.5, checked 2026-09-19.
> The nats-server pin is a deliberate hold — v2.15.0 needs Go 1.26 and this module's floor is
> 1.25 ([#11](https://github.com/taumatix/natsmqtt5/issues/11)). See [UPSTREAM.md](UPSTREAM.md),
> and [ROADMAP.md](ROADMAP.md) for where the broker knowingly does not yet conform.

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
- **Embeds as a library.** `natsmqtt5.New` returns a `*Broker` you run inside
  your own process, sharing your `*nats.Conn` if you want.

[paho]: https://github.com/eclipse/paho.golang

## Install

> **The published image is not public yet, so the `docker run` below fails with
> `denied` (checked 2026-09-14).** The image exists — every release pushes it —
> but the GitHub package is still private, and making it public needs an
> admin action that cannot be done from this repository. Tracked in
> [#7](https://github.com/taumatix/natsmqtt5/issues/7). Until it is resolved,
> use the `go install` instructions below, which work today. This note goes
> away when the package does.

As a container, against a NATS server you already run:

```sh
docker run --rm -p 1883:1883 \
  -e NATSMQTT5_NATS=nats://your-nats-server:4222 \
  ghcr.io/taumatix/natsmqtt5:v0.2.0
```

Images are published for `linux/amd64` and `linux/arm64` on every release, as
`:v0.2.0`, `:0.2.0`, `:0.2` and `:latest`. They are built from
[distroless/static][distroless], so there is no shell and no package manager in
them, and the broker runs as a non-root user.

If you have no NATS server yet, [compose.yaml](compose.yaml) starts one with
JetStream enabled and the broker in front of it:

```sh
curl -O https://raw.githubusercontent.com/taumatix/natsmqtt5/v0.2.0/compose.yaml
docker compose up -d
mosquitto_pub -V 5 -h localhost -p 1883 -t sensors/7/temp -m 21.5
```

Every flag has an environment variable twin — upper-case, `-` becomes `_`,
behind a `NATSMQTT5_` prefix — so `-subject-prefix` is
`NATSMQTT5_SUBJECT_PREFIX`. An explicit flag beats the environment. Run
`docker run --rm ghcr.io/taumatix/natsmqtt5:v0.2.0 -h` for the full list.

[distroless]: https://github.com/GoogleContainerTools/distroless

As a binary:

```sh
go install github.com/taumatix/natsmqtt5/cmd/natsmqtt5@v0.2.0
natsmqtt5 -nats nats://localhost:4222 -listen :1883
```

As a library:

```sh
go get github.com/taumatix/natsmqtt5@v0.2.0
```

Requires Go 1.25 or newer, and a NATS server with JetStream enabled (or
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
- **In-flight QoS 1 and QoS 2 messages are not persisted**, so a message
  awaiting its PUBACK when the broker dies is not resent — see the offline
  queue below, which is the feature that would make resending meaningful.
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
- **No QoS 1/2 retransmission on reconnect**, which follows from the above.
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
