# Upstream

This broker implements a published specification and runs against a server neither of which it
controls. This file says which versions it was built and checked against.

```yaml
- name: mqtt-specification
  kind: literal
  value: "MQTT Version 5.0, OASIS Standard, 2019-03-07"
  checked: 2026-09-19
  note: the normative reference; source clauses are cited inline as MQTT-5.0 §x.y.z

- name: nats-server
  kind: github-release
  repo: nats-io/nats-server
  tag: v2.14.5
  checked: 2026-09-19
  hold: "issue #11 — v2.15.0 needs Go 1.26, this module's floor is 1.25, dependency is test-only"
  note: held deliberately; the gap is still reported, it just does not raise the alarm

- name: paho-golang
  kind: github-release
  repo: eclipse-paho/paho.golang
  tag: v0.23.0
  checked: 2026-09-19
  note: the third-party MQTT client the end-to-end tests drive the broker with
```

## What each pin means

**The MQTT 5.0 specification** is the contract this broker exists to honour, and it does not
move — MQTT 5.0 is a finished OASIS standard. Clauses are cited inline in the source
(`MQTT-5.0 §3.8.2.1.2` and so on) so a reader can check a behaviour against the text rather than
against an assertion here. Where the broker deliberately does not conform, `ROADMAP.md` says so
and why.

**`nats-server` is held at v2.14.5 on purpose, not by neglect.** v2.15.0 raises its Go
requirement to 1.26, and this module's floor is Go 1.25; taking the bump would drop users on
1.25 to buy a test-only dependency upgrade. The dependency is test-only — the broker speaks to
whatever NATS server it is pointed at — so the trade is not worth making unilaterally. Tracked as
issue #11; it stays pinned until that is decided.

**`paho.golang` is the independent client** the end-to-end tests use. That independence is the
point: a test that drives the broker with its own encoder proves self-consistency, not
conformance. A third-party client that has never seen this code is the thing that can catch a
wire-format mistake.

**`compose.yaml` runs the `nats:2-alpine` image**, a floating tag that resolves to whatever 2.x
is current — deliberately different from the go.mod pin, so the compose stack exercises the
broker against a *newer* server than the test suite compiles in. A break there is an early
warning, not a build failure.

## How this file is kept honest

`checked` is bumped on every maintenance pass whether or not anything moved — an unrefreshed
date cannot be told apart from an unchecked one. Drift is reported by:

    /Users/taumatix/bootstrap/bin/check-upstream-drift.py <checkout>

It reports `nats-server` as **HELD** rather than DRIFTED, because the pin carries a `hold:` with
the reason. The gap is still measured and still printed — the hold suppresses the alarm, not the
measurement. That distinction matters: a checker that cries drift on a decision already taken is
one whose exit code everybody learns to ignore, and then it stops catching the real thing.

Remove the `hold:` the moment the reason stops being true, so the alarm comes back on.
