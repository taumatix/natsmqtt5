# Upstream

This broker implements a published specification and runs against a server neither of which it
controls. This file says which versions it was built and checked against.

```yaml
- name: mqtt-specification
  kind: literal
  value: "MQTT Version 5.0, OASIS Standard, 2019-03-07"
  checked: 2026-10-05
  note: >-
    the normative reference; source clauses are cited inline as MQTT-5.0 §x.y.z.
    Re-read at docs.oasis-open.org on 2026-09-24: the document's own status line
    still says "OASIS Standard, 07 March 2019", its "Latest version" link still
    resolves to itself, and no errata document exists (3.1.1 has one; 5.0 does not).
    Re-checked 2026-09-27 at oasis-open.org/standard/mqtt-v5-0-os: still the
    approved standard, with no successor listed.

- name: nats-server
  kind: github-release
  repo: nats-io/nats-server
  tag: v2.15.0
  checked: 2026-10-07
  note: >-
    test-only: the embedded server the end-to-end tests run against. Held at v2.14.5
    from 2026-09-21 until 2026-10-07 because v2.15.0 declares go 1.26.0; the floor
    was raised to Go 1.26 on 2026-10-07 (issue #11), which released the hold.

- name: nats-go
  kind: github-release
  repo: nats-io/nats.go
  tag: v1.54.0
  checked: 2026-10-07
  note: >-
    the NATS client the broker is built on; in the shipped build, not just the tests.
    Held at v1.53.1 from 2026-09-21 until 2026-10-07 because v1.54.0 declares
    go 1.26.0. Taking it with the Go 1.26 floor (issue #11) also moved
    golang.org/x/crypto from v0.55.0 to v0.57.0, past the x/crypto/ssh DoS
    advisories GO-2026-6354 and GO-2026-6355 that the hold had kept in the graph.

- name: paho-golang
  kind: github-release
  repo: eclipse-paho/paho.golang
  tag: v0.23.0
  checked: 2026-10-05
  note: the third-party MQTT client the end-to-end tests drive the broker with
```

## What each pin means

**The MQTT 5.0 specification** is the contract this broker exists to honour, and it does not
move — MQTT 5.0 is a finished OASIS standard. Clauses are cited inline in the source
(`MQTT-5.0 §3.8.2.1.2` and so on) so a reader can check a behaviour against the text rather than
against an assertion here. Where the broker deliberately does not conform, `ROADMAP.md` says so
and why.

**`nats-server` and `nats.go` track their latest releases, and the floor is Go 1.26.** From
2026-09-21 to 2026-10-07 both were held one release back (v2.14.5 and v1.53.1), because
nats-server v2.15.0 and nats.go v1.54.0 declare `go 1.26.0` and this module's floor was Go 1.25.
The hold had three costs. Users ran a NATS client one minor version behind; `nats.go` is in the
shipped build. It kept `golang.org/x/crypto` at v0.55.0, below the fix for two `x/crypto/ssh` DoS
advisories ([GO-2026-6354][g54], [GO-2026-6355][g55]). `govulncheck` showed those as not called
here, but they could not be patched from a Go 1.25 floor. And Go 1.25 itself left upstream
security support on 2026-08-19, when Go 1.27.0 shipped. On 2026-10-07 the floor was raised to
Go 1.26 (issue #11). That released both holds and moved `x/crypto` to v0.57.0. `govulncheck` now
reports no advisory affecting this module, and no fixable advisory anywhere in its graph.

`nats-server` is test-only: the broker speaks to whatever NATS server it is pointed at. `nats.go`
is the client the broker is built on.

[g54]: https://pkg.go.dev/vuln/GO-2026-6354
[g55]: https://pkg.go.dev/vuln/GO-2026-6355

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
date cannot be told apart from an unchecked one, and it moves only as far as that pass actually
verified. All four pins read 2026-09-24: the three `github-release` ones because the drift checker
queried them that day, and `mqtt-specification` because the OASIS document was re-opened and its
status line read, which is what had been missing when it lagged its neighbours by two days.
Drift is reported by:

    /Users/taumatix/bootstrap/bin/check-upstream-drift.py <checkout>

A pin may carry a `hold:` with a reason, and the checker then reports it as **HELD** rather than
DRIFTED. The gap is still measured and still printed: the hold suppresses the alarm, not the
measurement. That matters because a checker that cries drift on a decision already taken is one
whose exit code everybody learns to ignore, and then it stops catching the real thing. No pin
here is held today; the NATS holds were removed on 2026-10-07 when their reason went away.

Remove a `hold:` the moment the reason stops being true, so the alarm comes back on.
