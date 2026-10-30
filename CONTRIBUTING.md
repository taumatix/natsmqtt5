# Contributing

Fixes and features go through a pull request with CI green. `go build ./...`, `go vet ./...`,
`gofmt -l .` (empty) and `go test -race ./...` must pass locally first. Public API changes are
additive within a major version ([CHANGELOG.md](CHANGELOG.md)); a user-visible change adds a line
under `## [Unreleased]`.

## Tests that need a NATS cluster

A failure the embedded server cannot produce (a server cut off from its cluster, one member
restarted, the whole cluster down for a long time) is tested against three real servers in
containers, started by `natscluster_test.go`. They skip unless asked for:

    NATSMQTT5_CLUSTER_TESTS=1 go test -run '^TestCluster' -count=1 -v .

It needs Apple's `container` or `docker` on `PATH` (`NATSMQTT5_CLUSTER_RUNTIME` forces one;
`NATSMQTT5_CLUSTER_IMAGE` changes the NATS image, a public `nats:2-alpine` by default), and
`NATSMQTT5_CLUSTER_LOG=1` prints the brokers' own log. Name such a test `TestCluster...`: CI's
`cluster` job runs every test with that prefix and fails if one skipped. `startNATSCluster(t)`
returns a handle with `URL(i)`, `URLs()`, `StopServer`/`StartServer` (SIGKILL or SIGTERM), `Freeze`/`Thaw`
(SIGSTOP/SIGCONT) and `Isolate`/`Heal` (a blackhole route between one node and the other two, so
every process stays alive); the containers publish on 127.0.0.1 only and are removed when the test ends.

## Every statement has an integration test

natsmqtt5 aims to be a conforming MQTT 5.0 server ([CONFORMANCE.md](CONFORMANCE.md)), so a change
is judged against the specification's numbered statements, not against its own unit tests.

Each MUST or SHOULD statement a change touches needs a **test against a real NATS server** that
drives the broker over a real TCP socket and asserts what the specification says a client can
observe:

- Start the embedded `nats-server` from `harness_test.go`, with **JetStream enabled** whenever the
  feature uses it: the offline queue, persistent sessions, the shared backlog, retained messages.
  Use `startNATSWithoutJetStream` for the behaviour of a broker that has none.
- Connect a real client: Paho v5 where an ordinary client is enough, or `rawClient` (this module's
  packet codec) or hand-written bytes where the test must withhold an acknowledgement or judge a
  packet against the specification's byte layout, which a client library's own encoder cannot.
- Assert the fields and bytes the specification names (Session Present, Reason Codes, DUP,
  Packet Identifiers, ordering), not an internal state or a round trip through our own code.
- **Cite the statement id**, `[MQTT-x.y.z-n]`, in the test name or the comment above it, with the
  statement's wording read from the specification, never from memory. If a test writes into the
  session record or the queue because the broker cannot be driven there through its interface, say
  what it stands in for.
- Where a test shows a real defect, fix it in the same pull request when it is small. Otherwise put
  a precise entry on [ROADMAP.md](ROADMAP.md) and `t.Skip` the test with that entry's name; a red
  test is never left in.

Unit tests are welcome for the parts (the codec, the topic mapping, a bound, a decoder), but they do
not satisfy this rule. A statement covered only by unit tests is a **gap**: say so in the pull request
and list it on the roadmap (the "Statements covered only by unit tests" entry).

In the pull request description, give a table of statement id to the integration test that proves it.
When a change touches a row of `conformance/mqtt5-statements.tsv`, update the row and the summary in
[CONFORMANCE.md](CONFORMANCE.md).

## Scheduled runs nobody is watching

The weekly `Vulnerability scan` (`.github/workflows/vulncheck.yml`, Mondays 05:41 UTC) is the only thing that
turns red when a vulnerability is published against code that has not changed. The workflows hold
`contents: read` only, so one cannot open an issue on failure, and nothing in this repository pushes the
result to anyone. Whoever runs the maintenance pass reads it: list
`/repos/taumatix/natsmqtt5/actions/runs?event=schedule` (the Actions API; the App has no `checks:read`) and
treat a latest `Vulnerability scan` run that is not `success` as a finding, not as noise. A failure that names
a standard-library symbol clears when the runner's Go does; one that names a dependency is a bump.

## Other expectations

- Anything bound to an external reference (the specification, `nats.go`, `nats-server`) is pinned in
  [UPSTREAM.md](UPSTREAM.md); update it when you move a pin.
- Tests that take long are gated behind an environment variable and say which one in the skip message
  (`NATSMQTT5_SLOW_TESTS=1`). The `slow` CI job sets it and fails if the named test skips.
- Never commit a credential, token or key.
