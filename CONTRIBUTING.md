# Contributing

Fixes and features go through a pull request with CI green. `go build ./...`, `go vet ./...`,
`gofmt -l .` (empty) and `go test -race ./...` must pass locally first. Public API changes are
additive within a major version ([CHANGELOG.md](CHANGELOG.md)); a user-visible change adds a line
under `## [Unreleased]`.

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

## Other expectations

- Anything bound to an external reference (the specification, `nats.go`, `nats-server`) is pinned in
  [UPSTREAM.md](UPSTREAM.md); update it when you move a pin.
- Tests that take long are gated behind an environment variable and say which one in the skip message
  (`NATSMQTT5_SLOW_TESTS=1`).
- Never commit a credential, token or key.
