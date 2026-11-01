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
> and `nats-server` v2.15.0, checked 2026-10-07. Both are the latest releases. Since v0.9.1 this
> module needs **Go 1.26** ([#11](https://github.com/taumatix/natsmqtt5/issues/11)). Go 1.25 left
> upstream security support on 2026-08-19, and both NATS releases declare `go 1.26.0`. Holding the
> floor had kept `golang.org/x/crypto` below the fix for two `x/crypto/ssh` DoS advisories
> ([GO-2026-6354][g54], [GO-2026-6355][g55]). See [UPSTREAM.md](UPSTREAM.md),
> [CHANGELOG.md](CHANGELOG.md) for what each release changed under you, and
> [CONFORMANCE.md](CONFORMANCE.md) for a statement-by-statement review against the MQTT 5
> specification, and [ROADMAP.md](ROADMAP.md) for where the broker knowingly does not yet conform,
> in the order it will be fixed.

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
- **Queues messages for a client that is away.** QoS 1 and 2 messages
  published while a session is disconnected are delivered when it resumes, as
  MQTT 5 requires. On by default whenever JetStream is in use; see
  [Offline queue](#offline-queue).
- **Speaks MQTT over WebSocket as well as TCP and TLS.** `Broker.WebSocketHandler()` is an
  `http.Handler`; see [MQTT over WebSocket](#mqtt-over-websocket).
- **Embeds as a library.** `natsmqtt5.New` returns a `*Broker` you run inside
  your own process, sharing your `*nats.Conn` if you want.

[paho]: https://github.com/eclipse/paho.golang

## Install

As a container, against a NATS server you already run:

```sh
docker run --rm -p 1883:1883 \
  -e NATSMQTT5_NATS=nats://your-nats-server:4222 \
  ghcr.io/taumatix/natsmqtt5:v1.1.0
```

Images are published for `linux/amd64` and `linux/arm64` on every release, as
`:v1.1.0`, `:1.1.0`, `:1.1` and `:latest`. They are built from
[distroless/static][distroless], so there is no shell and no package manager in
them, and the broker runs as a non-root user.

If you have no NATS server yet, [compose.yaml](compose.yaml) starts one with
JetStream enabled and the broker in front of it:

```sh
curl -O https://raw.githubusercontent.com/taumatix/natsmqtt5/v1.1.0/compose.yaml
docker compose up -d
mosquitto_pub -V 5 -h localhost -p 1883 -t sensors/7/temp -m 21.5
```

Every flag has an environment variable twin — upper-case, `-` becomes `_`,
behind a `NATSMQTT5_` prefix — so `-subject-prefix` is
`NATSMQTT5_SUBJECT_PREFIX`. An explicit flag beats the environment. Run
`docker run --rm ghcr.io/taumatix/natsmqtt5:v1.1.0 -h` for the full list.

[distroless]: https://github.com/GoogleContainerTools/distroless

As a binary:

```sh
go install github.com/taumatix/natsmqtt5/cmd/natsmqtt5@v1.1.0
natsmqtt5 -nats nats://localhost:4222 -listen :1883
```

As a library:

```sh
go get github.com/taumatix/natsmqtt5@v1.1.0
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

### MQTT over WebSocket

Browsers and many hosted deployments reach MQTT only through WebSocket. Mount the broker's handler on
any `http.Server`, plain or TLS; the connection joins the same broker, sessions and subscriptions as the
TCP listener:

```go
mux := http.NewServeMux()
mux.Handle("/mqtt", broker.WebSocketHandler())
go http.ListenAndServe(":8083", mux) // or ListenAndServeTLS
```

The handler negotiates the `mqtt` subprotocol and closes a client that offers none, and closes one that
sends a text frame (MQTT-5.0 section 6). A browser's `Origin` must match the request's host or one of
`Options.WebSocketOriginPatterns`. `Broker.Close` disconnects WebSocket clients with 0x8B like any other;
shutting the `http.Server` down does not, because it cannot reach a hijacked connection. The TLS state and
remote address an `Authenticator` sees are those of the HTTP request. A message is read up to the
maximum packet size plus 64 KiB (for packets pipelined behind it); a larger one is refused with close
status 1009 before it is buffered.

The command serves it with `-ws-listen :8083` (path `-ws-path`, `/mqtt` by default). `-tls-cert` and
`-tls-key` make both that listener (`wss://`) and `-listen` (`mqtts://`) serve TLS; there is no
certificate reload, so restart the command to rotate.

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
keeps nothing. MQTT 5 counts the QoS 1 and 2 messages pending for a session as
part of its state, which outlives a disconnect while the Session Expiry Interval
lasts [MQTT-3.1.2-23], so the broker keeps them in a JetStream stream and
delivers them when the session resumes.

The queue is on by default whenever the broker uses JetStream: with retained
messages (the default) or `PersistentSessions`. That default arrived in
v0.10.0; up to v0.9.1 the queue is off unless `OfflineQueue` is set. A broker run with
`DisableRetained` and no `PersistentSessions` does not touch JetStream and has
no queue. If the queue's stream cannot be created, because the JetStream account
is out of streams or storage for instance, the broker logs a warning and runs
without it, retrying in the background (every 5 seconds, doubling to a minute) and
logging at info when it comes up; `Broker.OfflineQueueActive` says whether it is on. The settings:

```go
natsmqtt5.Options{OfflineQueueMaxAge: 24 * time.Hour} // on by default
natsmqtt5.Options{OfflineQueue: true}                 // required: fail to start without it
natsmqtt5.Options{DisableOfflineQueue: true}          // off
```

For the binary and the image they are `-offline-queue-max-age`,
`-offline-queue` / `NATSMQTT5_OFFLINE_QUEUE=true` and `-no-offline-queue` /
`NATSMQTT5_NO_OFFLINE_QUEUE=true`.

**Without the queue (no JetStream, or `DisableOfflineQueue`) the broker still
does not promise what it cannot keep.** It holds the QoS 1 and 2 messages
published while a session is away in its own memory, up to
`Options.AwayBufferMessages` per session (1000 by default, at most 2048;
`-away-buffer-messages` / `NATSMQTT5_AWAY_BUFFER_MESSAGES`), and delivers them in
publish order when the client resumes, ahead of anything newer. A session that
is away and misses more than that is ended: its client's next CONNECT with
Clean Start 0 is answered with Session Present 0 and it subscribes again. So a
client told Session Present 1 has been sent everything published while it was
away [MQTT-3.1.2-23], [MQTT-4.5.0-1]. What the memory buffer does not give you
is survival of a broker restart (sessions held in memory end with the process,
and the CONNACK says so with Session Present 0), a second broker taking the
session over, or a bound in bytes; for any of those keep the queue on.

The same stream serves a client that is connected but slower than its
publishers. A connection holds up to 2048 messages waiting for its client;
past that it reads the rest from the stream, at the client's pace and in order,
and goes back to live delivery once it has caught up. Without the queue, QoS 1
and 2 messages past that point are dropped, as QoS 0 always is.

When the session resumes, the client gets what its filters matched while it was
away, in publish order, before anything published after it reconnected. A
message is delivered once: what the client received before it dropped and what
arrives live during the resume are not repeated.

What it costs and what it does not cover yet:

- Each QoS 1 and 2 message is published twice: first under
  `<prefix>.$queue.<subject>` into the `MQTT5_queue` stream, which keeps it for
  `OfflineQueueMaxAge` (24 hours by default), and then live. A message whose live copy never
  reached the session (its publisher stopped between the two writes), or reached it after the
  session was released, is replayed however long ago it was stored: each session keeps a replay
  cursor, a queue sequence below which everything it was owed was sent except a short list of
  holes, and moves it by reading the stream without delivering once 1024 ids have piled up above
  it. That costs one read of the session's subjects per 1024 messages it is sent, and one
  JetStream request per SUBSCRIBE, to note where the new subscription starts (waited for at most
  250 ms; without an answer the highest queue sequence the broker has seen is used, which can only
  owe the subscription more). A session without a
  cursor (one restored from a record written before it existed, or one that left more than 1024
  holes or 16384 ids above it) replays from `OfflineQueueRewind` (10 seconds by default) before it
  was released instead, and starts a cursor once that replay has read the queue to its end. The
  broker waits
  for JetStream to confirm the first before publishing live and acknowledging
  the client, which adds a JetStream round trip to every QoS 1 and 2 publish.
  If JetStream cannot store it, the publish is refused, so the client tries
  again. QoS 0 is not queued.
- With `PersistentSessions` it also covers a session restored after a restart
  or on another broker: the stored record says when the session was released,
  and, for a client that was behind, the lowest queue sequence it had not been
  sent and the ids it had been sent above that (up to 4096 in the record, 65536 more beside it), so the replay starts
  where the client stopped, as for a session held in memory. Limits there. A
  record without that position (one from a connection that had delivered
  everything) replays from the release itself rather than a moment before it,
  because going back further could deliver a QoS 2 message twice; a message
  caught in the moments the connection went down can be lost that way. And a
  broker killed outright never releases its sessions, so a connected session
  writes its replay position to its record while it moves, once per
  `Options.SessionCheckpointInterval` (`-session-checkpoint-interval`,
  `NATSMQTT5_SESSION_CHECKPOINT_INTERVAL`; one second by default, negative turns
  it off). The next broker to resume the session sees it attached to an owner
  that no longer answers and replays from the last position written, so a kill
  costs the client the messages it was sent in that last interval, sent again,
  and loses none. A QoS 2 message is the exception: its record is written before the
  PUBLISH goes out and before the PUBREL does (about 0.3 ms each against a local NATS
  server, so a lockstep QoS 2 delivery takes about 0.77 ms instead of 0.16 ms;
  `BenchmarkQoS2Delivery`), so a kill at any step of the exchange resends the PUBLISH or
  the PUBREL and never delivers the message again as a new one [MQTT-4.3.3-6]. A QoS 1
  message acknowledged in the last interval is sent again, as any at-least-once sender
  that dies before it records the PUBACK must. With the checkpoint off, or for a
  record written before it existed, the replay is everything the queue still
  holds, up to `OfflineQueueMaxAge`. The cost is one key-value write per
  interval for each client that received or was sent something in it.
- A message published straight onto NATS rather than through a broker is not
  queued ([ROADMAP.md](ROADMAP.md)).
- A resume reads everything queued since the client left and matches it in the
  broker, so a long absence on a busy broker takes longer to catch up.

### What the queue and a durable PUBACK cost

One client publishing QoS 1 (128-byte payload) and waiting for each PUBACK, over loopback TCP to
an embedded NATS server with file-backed JetStream, on an Apple M2 Pro (`go test -run '^$' -bench
QoS1Publish -benchtime 3s`, three runs each, see `bench_publish_test.go`):

| | time per publish | publishes per second (one client) |
|---|---|---|
| no queue (`DisableOfflineQueue`) | about 34 µs | about 29,000 |
| queue, the default | about 88 µs | about 11,000 |
| queue and `DurablePublish` | about 111 µs | about 9,000 |

So the queue costs about 54 µs per QoS 1 publish here and `DurablePublish` about 23 µs more. Those
are loopback numbers with a local disk: against a NATS server across a network the cost is the
round trips, which is why the figures are not a forecast. The queue adds one JetStream round trip,
`DurablePublish` one more, so add twice or three times your round trip to NATS, plus the server's
fsync policy for file storage. A busy broker serves many clients at once, so total throughput is
higher than one client's.

### A PUBACK that means the message is safe

By default the broker waits for JetStream to store the queue's copy, then hands the live copy to its
NATS connection and acknowledges at once. nats.go writes that from another goroutine, so a broker
killed in that window loses a live copy it told the client it had. `Options.DurablePublish`
(`-durable-publish`, `NATSMQTT5_DURABLE_PUBLISH=true`) closes it: for QoS 1 and 2 the PUBACK or
PUBREC is sent only after (1) the queue stream stored the message (JetStream's publish
acknowledgement) and (2) a flush of the broker's NATS connection came back, which means the NATS
server has received the live publish. The flush says nothing about JetStream: the live copy is core
NATS and is not stored by it, and the only stored copy is the queue's, from (1). If either
fails or takes over 5 seconds the client gets 0x83 Implementation specific error and may send the
message again. It implies the offline queue (the broker will not start without it) and is an error
with `DisableOfflineQueue`. The default is off.

What the guarantee is: an acknowledged message is in the queue stream (for `OfflineQueueMaxAge`, to
the stream's replication) and the NATS server has routed the live copy. A session that was away gets
it from the queue; a connected subscriber gets it once, as without the option.

What it is not: live delivery stays core NATS, at most once per hop, so a connected subscriber whose
own NATS path fails at the wrong moment can still miss it; `ReconnectCatchUp` below closes the case
where that path is its broker's NATS connection ([ROADMAP.md](ROADMAP.md)). A retry after
a refusal can leave a second queue copy; `Options.DurablePublishDedupe` (`-durable-publish-dedupe`,
`NATSMQTT5_DURABLE_PUBLISH_DEDUPE=true`, off by default) gives the publish a `Nats-Msg-Id` so the
stream drops the retransmission (same packet identifier, DUP set, same topic and payload) inside its
duplicate window, 2 minutes unless you changed the stream's. The live copy is still sent again. QoS 0 is not covered. Nor are Wills: a Will goes through
the queue's keep like any other message but is not flushed, because there is no client to refuse to
and nothing a refusal could ask it to retry; the record of a durable Will is already removed by the
broker that publishes it ([ROADMAP.md](ROADMAP.md)).

### Messages published while a broker was off NATS

A broker whose NATS connection drops has no interest on the server until it reconnects, so a
message another broker publishes meanwhile reaches none of its connected sessions, and by default
nothing sends it later: only a session that was away is replayed. `Options.ReconnectCatchUp`
(`-reconnect-catch-up`, `NATSMQTT5_RECONNECT_CATCH_UP=true`) closes that for QoS 1 and 2 messages
that have a queue copy. When the connection drops, the broker holds its sessions' live QoS 1 and 2
copies back; when it is back, each connected session reads the queue from `OfflineQueueRewind`
before the drop, or from just after the newest message the broker had seen if that is earlier (a
server that died behind an open socket is noticed only when the pings fail). What the session has
not been sent goes out in stream order, then live delivery resumes, so nothing newer overtakes
it. The ids each session keeps stop a message going out twice, and a subscription is sent only
what was stored after it was made. Off by default; an error with `DisableOfflineQueue`.

It costs one read of the queue stream per connected session after each reconnect, filtered to the
session's subjects. It also catches up after a route between NATS servers drops while both brokers stay
connected: the broker watches the server list its connection learns from the server, and a change
starts the same hold and catch-up. Known limits: it does not see a route the server has not yet
noticed is dead (set `ping_interval` on the servers to shorten that), a flap shorter than 200 ms, or
a connection created with `nats.IgnoreDiscoveredServers`; and it does not see messages published straight onto a live subject, which have no
queue copy (publish them through `NATSIngest` instead; [ROADMAP.md](ROADMAP.md)).

## How MQTT maps onto NATS

| MQTT | NATS |
|---|---|
| Topic `sensors/7/temp` | Subject `mqtt.sensors.7.temp` |
| Topic level `/` | Subject token separator `.` |
| A literal `.` in a topic | `//` |
| An empty topic level | `./` or `/.` depending on position |
| Filter wildcard `+` | `*` |
| Filter wildcard `#` | `>`, plus a second subscription on the parent |
| Shared subscription `$share/g/f` | Durable JetStream consumer on the offline queue for QoS 1 and 2, pulled only by connected members; queue group on the subject for `f` for QoS 0 |
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

### Queueing plain NATS publishes (opt-in)

A message a plain NATS client publishes on a live subject, `<prefix>.<subject>`, with an
`Mqtt5-Qos` header of 1 or 2 is queued when `Options.QueuePlainPublishes`
(`-queue-plain-publishes`, `NATSMQTT5_QUEUE_PLAIN_PUBLISHES=true`) is on. It is off by default, and
then nothing below applies. Every broker with an offline queue leaves the first copy to plain NATS
subscribers and one broker, chosen by a NATS queue group on `<prefix>.>`, handles it as it handles an
ingest message (below): the queue copy is stored and the live copy is published again with
`Mqtt5-Msg-Id` and `Mqtt5-Queue-Seq`, which is the copy MQTT sessions are delivered. A session that
was away gets it when it comes back, and a shared group's QoS 1 and 2 work goes only to members whose
client is connected.

    nats pub 'mqtt.sensors.7.temp' 21.5 -H 'Mqtt5-Qos: 1'

What to know before turning it on:

- A plain NATS **subscriber** sees such a message twice: the original (no `Mqtt5-Msg-Id`) and the
  broker's copy (with one). Filter on the header. A message with no `Mqtt5-Qos`, QoS 0, a subject
  starting with `$` and a message that already has an id are not touched.
- Nothing answers the publisher; the broker logs a refusal (a topic that fails validation, a QoS above
  `MaximumQoS`) at WARN, and the message is then dropped for MQTT sessions. Use `NATSIngest` to be told.
- The broker liveness subject moves from `<prefix>.$broker.<id>.ping` to `<prefix>$broker.<id>.ping`,
  out of reach of the subscription on `<prefix>.>`; a dead broker would otherwise answer a liveness
  request with a timeout instead of "no responders". **Restricted NATS permissions must allow the new
  subject**, and with `DurableWills` or `PersistentSessions` every broker of the fleet must have the
  same value, or a broker cannot see the others' pings. With the option off the subject is the one
  v1.1.0 used, so a mixed v1.1/v1.2 fleet works.
- Every broker has one more NATS subscription, on `<prefix>.>`, which sees every message under the
  prefix; measure it on a busy subject space.
- A publish to a JetStream stream that does not exist (a missing offline queue under `DurablePublish`)
  waits for its timeout, up to 5 s, instead of failing at once with "no responders".
- Brokers sharing a `SubjectPrefix` must agree on the option, or a broker that does not hold the first
  copy back delivers it as well as the republished one.

### Publishing from plain NATS through a broker

A message published straight onto a live subject has no queue copy, whatever its `Mqtt5-Qos`
header says: a session that was away never gets it, and a shared subscription gets it only through
its NATS queue group, which can pick a member whose client is away and drop it there. With
`Options.NATSIngest` (`-nats-ingest`, `NATSMQTT5_NATS_INGEST=true`) the brokers also listen on
`<prefix>.$ingest.<subject>`, in one NATS queue group, and the broker that takes a message handles
it as an MQTT PUBLISH on `<subject>`. It checks the topic and the QoS (and refuses a topic starting
with `$` whatever `RestrictDollarTopics` says, so the subject cannot write `$retained` or `$queue`), stores a retained message,
stores the queue copy of a QoS 1 or 2 message, and publishes the live copy with its id and
sequence. That copy reaches away sessions, shared groups and `ReconnectCatchUp` like any other.

    nats request 'mqtt.$ingest.sensors.7.temp' 21.5 -H 'Mqtt5-Qos: 1'

The reply is empty, with `Mqtt5-Msg-Id` and, when queued, `Mqtt5-Queue-Seq`. If the broker refused
the message, the reply carries `Mqtt5-Ingest-Error` with the reason, and nothing was published.
A plain `nats pub` without a reply subject works too, but nothing tells it the outcome. Headers
that do not start with `Mqtt5-` are passed on to the live copy. The `Authorizer` is not asked,
because a NATS client has no MQTT identity: NATS permissions on `<prefix>.$ingest.>` decide who
may publish there. Each broker handles ingest messages one at a time, so a publisher that waits for
each reply keeps its order. Off by default, and then nothing answers on the subject.

## Persistent sessions

By default a session lives in the broker process: a client reconnecting to the
*same* broker within its Session Expiry Interval resumes its subscriptions, and
one reconnecting to a different broker, or after a restart, has to subscribe
again.

A session whose client never comes back is dropped once its Session Expiry Interval (capped by
`MaxSessionExpiry`) has passed: a sweep, every `SessionSweepInterval` (default one minute),
unsubscribes it from NATS and forgets it. A session can therefore outlive its interval by up to one
sweep interval, never fall short of it, and a client that returns after the interval gets Session
Present 0 whether or not the sweep has run.

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
- **In-flight QoS 1 and QoS 2 messages are persisted as references.** When a
  connection ends, the record keeps the Packet Identifier, QoS state and
  offline-queue sequence of each message the client has not acknowledged, and the
  identifiers of the client's QoS 2 PUBLISH packets received and not yet
  released. A session restored from the store, after a restart or on another
  broker, reads the payloads back from the queue and resends them with their
  original identifiers and DUP 1, resends the PUBREL for a QoS 2 exchange the
  client had taken ownership of, and acknowledges a resent QoS 2 PUBLISH without
  forwarding it twice. A message that expired in the meantime is dropped. Limits:
  a message with a copy in the offline queue is read back from it; one with none
  (retained, or with the queue off) travels in the record as its encoded PUBLISH
  when that is 16 KiB or less, and a larger one is written to the `<StreamPrefix>_inflight`
  bucket (named by the client and the SHA-256 of the PUBLISH, created on first use, deleted when the
  message is acknowledged, when the session ends or is replaced by a Clean Start, or when the
  expired record is swept, and aged out after twice the maximum Session Expiry Interval otherwise) and read back by the broker that restores the session. One whose
  queue copy has aged out, or whose PUBLISH exceeds the server's `max_payload` less 512 bytes,
  is logged and not resent; a broker killed outright leaves what its connections last checkpointed
  (`-session-checkpoint-interval`, one second): a QoS 1 message sent or
  acknowledged in that last interval is not (or no longer) in the record and is sent
  again, while a QoS 2 message is recorded before each packet that moves its exchange on;
  and a record is cut to fit
  the NATS server's `max_payload` (1 MiB by default), newest in-flight entries
  first, which is about 15000 entries, so a client with a Receive Maximum beyond
  that and that many messages unacknowledged loses the newest, logged.
- **A filter the `Authorizer` no longer permits is dropped, and the CONNACK names it in a User
  Property that is this broker's own.** Every filter of a resumed session goes back past the
  `Authorizer` before the CONNACK, on both branches, so a permission narrowed while the client was
  away takes effect immediately. A CONNACK has no per-filter Reason Code, so the CONNACK carries one
  `natsmqtt5-dropped-filter` User Property per dropped filter (`natsmqtt5.DroppedFilterProperty`);
  it is not part of MQTT v5, and a client that does not read it still sees `SessionPresent: true`
  and finds out by not receiving. It is left out when the CONNACK would exceed the client's Maximum
  Packet Size. The unacknowledged messages a dropped filter earned go with the filter.
- **Clients may not use `$` topics, unless you allow them (since v1.0.0).** MQTT 5.0 §4.7.2 says
  a server SHOULD prevent clients exchanging messages on topics starting with
  `$`. A PUBLISH or Will on a `$` topic is refused with Topic Name invalid (0x90)
  and a subscription to one with Topic Filter invalid (0x8F); `$share/...` stays
  allowed unless the filter inside it starts with `$`. Applications using
  `$app/...` keep working with `Options.PinRestrictDollarTopics: natsmqtt5.Ptr(false)`
  or `-restrict-dollar-topics=false` / `NATSMQTT5_RESTRICT_DOLLAR_TOPICS=false`
  (`$retained` and `$queue`, where the broker keeps its own data, are refused either way).
  A subscription a persistent session stored earlier is still resumed.
- **The Will Message of a killed broker is published by a survivor whenever JetStream is in use
  (since v1.0.0).** With `-durable-wills=false` (`Options.PinDurableWills: natsmqtt5.Ptr(false)`) a
  Will belongs to the connection in memory: a broker that shuts down cleanly publishes its
  clients' Wills, one killed outright does not. See [Durable Will Messages](#durable-will-messages).
- **A session whose broker was killed outright is resumed whenever its client
  comes back**, however long that takes. Expiry is measured from the moment a
  session is released, and a `kill -9` records no release. A background sweep
  reclaims such a record once it is older than `MaxSessionExpiry` and its broker
  has stopped answering.

## Durable Will Messages

A Will Message is published by the broker holding the connection, so a broker
that is killed outright (`kill -9`, a lost machine) takes its clients' Wills
with it. Since v1.0.0 this is on whenever JetStream is in use (retained messages,
`PersistentSessions` or `OfflineQueue`); `Options.PinDurableWills` or `-durable-wills[=false]`
(`NATSMQTT5_DURABLE_WILLS`) chooses either way. Every connection's Will is
also written to the `<StreamPrefix>_wills` key-value bucket, keyed by connection
so a reconnect never overwrites its predecessor's record.

- A broker whose owner has stopped answering its liveness subject (two missed
  pings, so a busy broker is not mistaken for a dead one) has its Wills adopted
  by a survivor, which waits out the rest of the Will Delay Interval and
  publishes. Whoever wins a compare-and-swap delete of the record publishes it,
  so there is one publication however many brokers noticed
  [MQTT-3.1.2-8], [MQTT-3.1.2-10].
- A client that reconnects inside the delay cancels the Will if its session is
  resumed [MQTT-3.1.3-9]; one that starts a new session (Clean Start, or the
  session had ended) gets the old Will published first.
- A DISCONNECT with Reason Code 0x00 leaves nothing to publish
  [MQTT-3.1.2-8], [MQTT-3.1.2-10]; a Will on a live connection stays put.
- `WillCheckInterval` (`-will-check-interval`, default 5s) is how often a
  broker looks; the delay is counted from when a survivor notices, up to that
  long after the connection was lost.

The bucket has no time-to-live, because the record of a connection that stays open can
be years old: a record is removed by the broker that publishes, cancels or drops it, or
adopted by a survivor if that broker dies. A record that ended and has been overdue for more than
24 hours (counted from its due time, so no Will Delay Interval can outlast it) is deleted by any
broker without being published, since what is left is one a fault leaked.
Each broker's index holds a small fixed reference per record, not the Will, so many large Wills
cost the bucket's storage and not every broker's memory.

It is off by default because the Will's payload sits in clear text in that
bucket, and because it needs JetStream. **Requirement:** the `Authorizer` governs MQTT actions and
cannot see the bucket, so whoever can read the key-value bucket `<StreamPrefix>_wills` (its
`$KV.<StreamPrefix>_wills.>` subjects and `$JS.API` calls on it) can read every client's Will, and
whoever can write it can have a Will published that no client set. Give the bucket the NATS
permissions of the session bucket: the brokers' own NATS user and no other. It works with or without `PersistentSessions`.
A broker cut off from NATS but still serving clients looks dead to the others,
so it fences itself: if it cannot reach NATS (its connection, its own liveness
subject and the Will bucket) for twice `WillCheckInterval`, it closes its client
sockets, and a survivor adopts a silent owner's Wills only after the silence has
outlasted that and the time the owner takes to notice (about 5 times
`WillCheckInterval`, on top of the ping timeout). A Will is therefore published
only after its client's connection is closed [MQTT-3.1.2-8]. The price is that
the Will of a broker that really died waits that long longer than it did.

## Three defaults that changed in v1.0.0

Three behaviours are on by default since v1.0.0. Each can be pinned either way with an option that
beats the plain one, and a pin is the only way to keep the old behaviour (a plain `false` cannot
turn off what is on by default). [MIGRATION.md](MIGRATION.md) says per change what differs, who is
affected and the one setting that keeps the old behaviour.

| Behaviour | Pin it with | Binary |
|---|---|---|
| Durable Wills | `Options.PinDurableWills` | `-durable-wills[=false]` |
| Restricting `$` topics | `Options.PinRestrictDollarTopics` | `-restrict-dollar-topics[=false]` |
| Outbound topic aliases | `Options.PinOutboundTopicAliases` | `-outbound-topic-aliases[=false]` |


## What it does not do

Stated plainly, because a broker you cannot trust the limits of is worse than
one with fewer features:

- **The offline queue needs JetStream, and covers what passes through a
  broker.** A broker without JetStream, or with `DisableOfflineQueue`, holds QoS
  1 and 2 messages for a session that is away in memory only, up to
  `AwayBufferMessages` (an overflowing session is ended, Session Present 0), and
  messages published straight onto NATS are never queued. See [Offline queue](#offline-queue).
- **A shared subscription hands QoS 1 and 2 work only to connected members,
  through a backlog in the offline queue.** A QoS 1 message stays unacknowledged
  in the backlog until the member's PUBACK, and goes to another member if the
  member's session ends first. The backlog is deleted when no session is
  subscribed to the group any more (§4.8.2), tracked in a key-value bucket
  `<StreamPrefix>_share_members`. Without the queue it is a plain NATS
  queue group, which also chooses members whose client is away, and their share
  is dropped. Brokers sharing a `SubjectPrefix` must agree on the queue, or a
  message can reach a group twice. QoS 0 messages always go through the queue
  group.
- **Without the queue, a client that falls behind loses QoS 1 and 2.** A
  connection holds 2048 messages waiting for its client; with the queue on, a
  client slower than its publishers reads the rest from the stream, and with
  `DisableOfflineQueue` (or no JetStream) there is no copy to read, so the
  broker drops what does not fit and logs "dropping a message: the client is not
  keeping up". That is a deviation from [MQTT-4.1.0-1] and [MQTT-4.5.0-1] that
  the queue exists to close, and there is no option to hold publishers back
  instead: the only place to block is the NATS subscription's handler, which
  does not slow a publisher down, it moves the loss into the NATS client's
  pending buffer (and the server's slow-consumer handling of the broker's one
  connection, which every client shares). Run with the queue on if QoS 1 and 2
  must not be lost to a slow client. `Options.CloseBehindClients`
  (`-close-behind-clients`, `NATSMQTT5_CLOSE_BEHIND_CLIENTS=true`, off by default)
  makes the loss visible instead: with no queue, the connection of a client that
  lost a QoS 1 or 2 message this way is closed with DISCONNECT 0x97 (Quota
  exceeded). The message is still lost; the client now knows. Its session is
  kept to resume while what waits for it fits `AwayBufferMessages` and ended
  (Session Present 0) when more than that was lost.
- **Retransmission on reconnect reaches as far as the broker's memory.** A
  client that reconnects with Clean Start 0 to a broker still holding its
  session is resent the PUBLISH and PUBREL packets that connection left
  unacknowledged, with their original Packet Identifiers [MQTT-4.4.0-1]. A
  session restored from the store instead — after a restart, or on another
  broker — has nothing to resend.
- **Shared subscriptions are granted QoS 1 by default; `SharedQoS2` is opt-in.** A QoS 2 request on a
  `$share/` filter is answered with `Granted QoS 1` in the SUBACK, which
  [MQTT-3.8.4-7] permits. `Options.SharedQoS2` (`-shared-qos2`) grants QoS 2: the
  member that is sent a message owns it from then on, a held message survives a
  killed broker and a takeover (also for a client that stays away longer than the
  group's 30 s AckWait: the surviving brokers keep saying the message is being
  worked on while the dead broker's session record stands), and the group is told
  at PUBREC. Across a killed broker the guarantee is that of QoS 2 without a
  shared subscription: it needs `PersistentSessions` and a positive
  `SessionCheckpointInterval`, and a record write that fails still lets the
  PUBLISH go out. It stays off by default because turning it on changes what a
  v1 client is granted ([ROADMAP.md](ROADMAP.md)); a group consumer made by a
  release before the start was recorded (CHANGELOG [Unreleased]) does not carry
  the start the late-backlog check reads. `Options.SharedHoldLimit` hands a
  message a connected member never acknowledges back to the group, and one held
  for a dead broker's session once the limit has passed.
- **Enhanced authentication is opt-in.** With `Options.Auther` set, a CONNECT naming an
  Authentication Method runs the AUTH-packet exchange of MQTT-5.0 §4.12 (challenge, response,
  CONNACK) and a client may re-authenticate on the open connection (AUTH 0x19); a failed
  re-authentication ends in a DISCONNECT. A re-authentication that yields another identity
  re-checks the session's subscriptions and Will for it (as `Reauthorize` does) and drops
  what it no longer allows. `hmacAuther` in `example_test.go` is a complete
  reference method. Without an `Auther` a CONNECT carrying a method is refused with
  `0x8C Bad authentication method`, as before. The `Authenticator` is not asked for such a
  CONNECT; the `Authorizer` sees the identity the exchange returns. The `scram` sub-package
  is a ready SCRAM-SHA-256 method (RFC 7677, no channel binding) over a credential lookup
  you supply: `Options.Auther = &scram.Auther{Lookup: ..., OwnsClientID: ...}`.
- **The broker sends topic aliases only if asked to.** It accepts them from clients always; Since v1.0.0 it sends them to a client that offered a Topic Alias Maximum; `Options.PinOutboundTopicAliases: natsmqtt5.Ptr(false)` (`-outbound-topic-aliases=false`) sends none.
- **`Message Expiry Interval` counts whole seconds.** A message is deleted once it
  has waited its whole interval, and one still live goes out with the interval less
  the whole seconds it waited (2 s with 1.3 s waited leaves 1), on every delivery
  path including retained messages, which wait from the moment they were stored.
  The time is the broker's own clock and the JetStream timestamp, so brokers that
  share a NATS server agree to within clock skew between them. A retained message
  found expired is removed from the stream by the first subscription that looks;
  one nobody subscribes to stays until replaced. A live message's wait starts when
  the receiving broker got it, so time spent in NATS between brokers is free.
  `Options.PublisherClockExpiry` (`-publisher-clock-expiry`) counts it from the
  publishing broker's receipt instead, at the price of trusting the clocks: a
  publisher running behind, or a forged `Mqtt5-Published-At` header from anything
  that can publish on the subjects, makes a message expire early (a stamp in the
  future counts as no wait). Off by default.
- **A broker's retained view can briefly serve a message another broker cleared.**
  It is checked against the stream when the counts disagree and after every NATS
  reconnect it notices, but a partition shorter than the client's ping interval is
  neither. `Options.RetainedVerifyInterval` (`-retained-verify-interval`, off by
  default) also checks every entry on that timer, at one lookup per entry.
- **A PUBACK does not mean the message reached NATS.** The broker hands the
  message to its NATS connection and acknowledges; the write is flushed
  microseconds later. A broker killed in that window loses a QoS 1 message it
  has already acknowledged. A SUBACK, by contrast, does wait for the NATS
  server to confirm the subscription — otherwise a message published
  immediately after subscribing could be dropped, which is a loss the client
  has no way to notice.
- **Topics containing a space, a tab, `*`, `>` or `DEL` are refused** with
  `0x90 Topic Name invalid` unless `Options.EscapeNATSWildcards` is set. MQTT
  permits them; a NATS subject cannot carry them. `nats-server` lets `*` and `>` through, which silently turns an MQTT
  subscription to `a/*/b` into a NATS wildcard that receives topics it must not
  see. Failing loudly is the default. `Options.EscapeNATSWildcards` (`-escape-nats-wildcards` /
  `NATSMQTT5_ESCAPE_NATS_WILDCARDS=true`) accepts them
  instead and writes each into the subject as `/` and a letter (`*` is `/s`, `>`
  is `/g`, space is `/p`, tab, line feed, carriage return, form feed and DEL are
  `/t` `/n` `/r` `/f` `/d`), a sequence the `nats-server` mapping never
  produces. Topics without those characters keep exactly the subject they had;
  the ones with them are no longer readable as the same subject by
  `nats-server`'s own MQTT listener, so choose before you have data. **Switching
  on** needs nothing done: every topic the plain mapping could store has the same
  subject under the escaping one, so retained, queued and in-flight messages are
  found as before. **Switching off** has no rewrite to offer, because a topic that
  needed escaping cannot be written under the plain mapping at all, but what is
  stored is still read back under its real name (`ret/a*b` stays `ret/a*b`), so
  it keeps being served; clients can no longer publish to such a topic, nor
  subscribe to one by name, so publish an empty retained message to clear one
  before switching off if it should go.

## Known limits

What the broker cannot do because of NATS or the specification, as opposed to what it does not do yet
([ROADMAP.md](ROADMAP.md)). Each says what would lift it.

- **A shared group's last leave is serialised against a join only among brokers of this release.** JetStream has no
  conditional consumer delete, so the broker takes a per-group lock in the `<StreamPrefix>_share_members` bucket around a
  join and around the last leave's recheck-then-delete. A broker of an earlier release takes no lock, so a fleet that
  mixes releases keeps the old window (a message published just after a joiner's SUBACK can go with the consumer the
  leaver deletes). A broker killed holding the lock delays the group's next join by up to 10 s. Lifted by upgrading every
  broker, or by a NATS release with a delete that checks the consumer is unchanged.
- **A group consumer's start can be missing.** A consumer made by an earlier release, or one that had already delivered
  when a broker of this release first saw it, has no recorded start, and the late backlog of a default queue that failed at
  startup falls back to the acknowledgement floor: a message the group acknowledged before its live copy arrived can be
  delivered twice. JetStream cannot change a consumer's `DeliverPolicy` after it is made, so a consumer cannot be created
  by start sequence without breaking brokers that did not; lifted by upgrading the fleet together.
- **A lone QoS 2 delivery costs two record writes.** The record must say PUBLISH before the PUBLISH and PUBREL before the
  PUBREL goes out [MQTT-4.3.3-6]; messages waiting together share a write, one client in lockstep has nothing to share.
  Lifted only by a faster key-value store.
- **QoS 2 across a killed broker can duplicate where no record is written.** With `SessionCheckpointInterval` negative, or
  without `PersistentSessions`, there is no record, and a message that is not held in a shared group is sent after a failed
  write. A shared message (`SharedQoS2`) is not sent when its record cannot be written.

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
- Every conformance statement a change touches is proved by an integration test against a real NATS
  server, citing its `[MQTT-x.y.z-n]` id; see [CONTRIBUTING.md](CONTRIBUTING.md).

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
