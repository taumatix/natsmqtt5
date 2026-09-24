# Upstream

This broker implements a published specification and runs against a server neither of which it
controls. This file says which versions it was built and checked against.

```yaml
- name: mqtt-specification
  kind: literal
  value: "MQTT Version 5.0, OASIS Standard, 2019-03-07"
  checked: 2026-09-24
  note: >-
    the normative reference; source clauses are cited inline as MQTT-5.0 §x.y.z.
    Re-read at docs.oasis-open.org on 2026-09-24: the document's own status line
    still says "OASIS Standard, 07 March 2019", its "Latest version" link still
    resolves to itself, and no errata document exists (3.1.1 has one; 5.0 does not).

- name: nats-server
  kind: github-release
  repo: nats-io/nats-server
  tag: v2.14.5
  checked: 2026-09-24
  hold: "issue #11 — v2.15.0 needs Go 1.26, this module's floor is 1.25, dependency is test-only"
  note: held deliberately; the gap is still reported, it just does not raise the alarm

- name: nats-go
  kind: github-release
  repo: nats-io/nats.go
  tag: v1.53.1
  checked: 2026-09-24
  hold: >-
    issue #11 — v1.54.0 declares go 1.26.0, and taking it rewrites this module's
    directive from 1.25.0 to 1.26.0 (probed, not assumed). Unlike nats-server this
    one is a RUNTIME dependency, so the hold now costs users a real version — and
    since 2026-09-21 it also pins golang.org/x/crypto at v0.55.0, whose two
    x/crypto/ssh DoS advisories are fixed only in v0.56.0, which needs Go 1.26 too.
    Re-probed 2026-09-24: taking nats.go@v1.54.0 now drags x/crypto to v0.57.0 by
    itself, so the runtime bump and the security patch are one move, not two.
  note: >-
    the NATS client the broker is built on; in the shipped build, not just the tests.
    Added as a pin on 2026-09-21 when it hit the same Go 1.26 floor as nats-server.

- name: paho-golang
  kind: github-release
  repo: eclipse-paho/paho.golang
  tag: v0.23.0
  checked: 2026-09-24
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

**As of 2026-09-21 the same wall now blocks `nats.go`, and that changes the cost of the hold.**
`nats.go` v1.54.0 declares `go 1.26.0`. Running `go get github.com/nats-io/nats.go@v1.54.0` in a
throwaway copy of this module rewrote the directive `go 1.25.0 => go 1.26.0`, exactly as
nats-server's bump did — so the choice is the same choice, but the dependency is not: **`nats.go`
is linked into the shipped library**, not confined to the tests. Issue #11's reasoning for holding
("the update buys us nothing a user can observe") no longer covers this half. Holding now means
users run a NATS client one minor version behind, and raising the floor still drops everyone on
Go 1.25. The decision is recorded on issue #11 rather than taken in a maintenance pass, because
raising a language floor is a compatibility break.

**Later the same day the hold acquired a second cost, and this one is a security patch.**
`govulncheck ./...` reports three advisories in required-but-uncalled modules, all in
`golang.org/x/crypto` v0.55.0; two of them — [GO-2026-6354][g54] and [GO-2026-6355][g55],
`x/crypto/ssh` connection deadlocks reachable by a malicious peer — are **fixed in v0.56.0**.
`x/crypto` v0.56.0 declares `go 1.26.0`, and `go get golang.org/x/crypto@v0.56.0` in a throwaway
copy printed `go: upgraded go 1.25.0 => 1.26.0`. So there is no patched `x/crypto` reachable from a
Go 1.25 floor: the whole `golang.org/x` ecosystem has moved past it.

Two qualifiers keep this honest, and they cut in opposite directions. It is **not exploitable
here** — `govulncheck` puts the affected symbols (`Dial`, `NewClientConn`, `NewServerConn`) outside
this module's call graph, and nothing in the broker speaks SSH; `x/crypto` arrives transitively
through `nats-server`. But it is **unpatchable**, which is the part that does not improve on its
own: every future `x/crypto` advisory lands the same way for as long as the floor stays at 1.25.

The fact issue #11 asked for is now available too. Go's release policy is that "each major Go
release is supported until there are two newer major releases"; Go 1.25.0 shipped 2025-08-12, 1.26.0
on 2026-02-10 and 1.27.0 on 2026-08-19 (go.dev/doc/devel/release, read 2026-09-21). Two newer major
releases exist, so **Go 1.25 has itself been out of upstream security support since 2026-08-19**.
The floor is protecting a toolchain that stopped receiving security fixes a month ago. That is an
argument, not a decision — it still belongs to issue #11.

[g54]: https://pkg.go.dev/vuln/GO-2026-6354
[g55]: https://pkg.go.dev/vuln/GO-2026-6355

**2026-09-24: the two bumps turned out to be one bump.** Re-probed in a throwaway copy rather
than re-read off the earlier note. `go get github.com/nats-io/nats.go@v1.54.0` now pulls
`golang.org/x/crypto v0.55.0 => v0.57.0` along with it — so taking the runtime client forward and
taking the `x/crypto` patch are the same action, not two separate reasons to raise the floor. The
cost side is unchanged: every path prints `go: upgraded go 1.25.0 => 1.26.0`.

Sixteen modules now have a newer version available and `go get` rewrites the directive to `1.26.0`
for each of the ones tried. `x/crypto` itself has moved on again, to v0.57.0. Nothing here is a
new decision — issue #11 still owns it — but the number to weigh has grown, and the toolchain this
host builds with is Go 1.27.1, two majors past the floor.

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

It reports `nats-server` as **HELD** rather than DRIFTED, because the pin carries a `hold:` with
the reason. The gap is still measured and still printed — the hold suppresses the alarm, not the
measurement. That distinction matters: a checker that cries drift on a decision already taken is
one whose exit code everybody learns to ignore, and then it stops catching the real thing.

Remove the `hold:` the moment the reason stops being true, so the alarm comes back on.
