# Eclipse Paho interoperability suite, MQTT 5

An external check on the statement-by-statement review in `CONFORMANCE.md`: the MQTT 5 client
tests of `eclipse/paho.mqtt.testing` driven at natsmqtt5 over a real socket. Since 2026-11-02 CI
runs it on every pull request and fails on any difference from the expected results.

- Last measured: 2026-11-02, natsmqtt5 at `876b02b` (v1.2.1 in preparation), with a local
  nats-server (JetStream on). First run by hand on 2026-11-01 against v1.0.1: the same results.
- Suite: `https://github.com/eclipse/paho.mqtt.testing` at `fdab9d2b1ce125385135e5d9f282a0ad315268cc`,
  `interoperability/client_test5.py` (27 tests, plain Python 3, no dependencies)

## How CI enforces it

The `paho` job in `.github/workflows/ci.yml` runs `conformance/run-paho.sh`, which builds
nats-server and `cmd/natsmqtt5` from the checkout, fetches the suite at the pinned commit, runs it
unmodified against a fresh broker in three configurations, and hands each run's output to
`conformance/paho-compare.py`. The comparer reads which tests the final `FAIL:`/`ERROR:` block
names (the progress lines are interleaved with the suite's own logging and are not parsed) and
compares every test in every configuration with `conformance/paho-expected.tsv`. **Any difference
fails the job, in either direction:** a test that fails and is expected to pass is a regression; a
test that passes and is expected to fail means the file is stale, so update it (and this page).

| Configuration | Broker started with | Passes |
|---|---|---|
| `default` | nothing extra | 21 of 27, 22 with `test_flow_control2` |
| `dollar` | `-restrict-dollar-topics=false` | 22 of 27, 23 with it |
| `keepalive` | `-server-keep-alive 60` | 20 of 27, 21 with it |

Across the three, 24 of 27 tests pass somewhere (`test_flow_control2` included). The only tests that fail in all three are the
three that cannot run against a broker configured like this one (below).

One suite test, `test_flow_control2`, is marked `FLAKY` in the expectations and either result is
accepted. It sends 65536 QoS 2 PUBLISHes and expects DISCONNECT 0x93 (Receive Maximum exceeded);
the broker sends it and closes, but the suite's client keeps writing (a PUBREL) to the closed
socket, and the reset can reach it before it reads the DISCONNECT (`BrokenPipeError` or
`ConnectionResetError`). It passed about two runs in three on macOS and failed every time on the
Linux CI runner. The broker does send the DISCONNECT; whether it should drain the connection before
closing, so the reason code survives, is under "Left over". The counts below leave that test out
of the passes, so they are the floor.

## Reproduce

```sh
conformance/run-paho.sh            # about nine minutes: three runs of about three
conformance/run-paho.sh --write    # print a fresh expectations table, reasons blank
```

or by hand, one configuration:

```sh
git clone https://github.com/eclipse/paho.mqtt.testing /tmp/paho && git -C /tmp/paho checkout fdab9d2
go build -o /tmp/nats-server github.com/nats-io/nats-server/v2 && go build -o /tmp/natsmqtt5 ./cmd/natsmqtt5
/tmp/nats-server -js -a 127.0.0.1 -p 24222 -sd /tmp/js &
/tmp/natsmqtt5 -nats nats://127.0.0.1:24222 -listen 127.0.0.1:21883 &
cd /tmp/paho/interoperability && python3 client_test5.py --port 21883 -v
```

The suite's option parsing has quirks: `-h` is read as `--help` (substring test), the
`--port=N` form crashes, and `--hostname` is not removed from `sys.argv` before unittest sees it.
Use `--port N` with a space, and leave the host at its default `localhost`. The suite is run
unmodified.

## Result

Measured 2026-11-02. None of the failures is a broker bug.

| Test | default | dollar | keepalive | Class | Reason |
|---|---|---|---|---|---|
| test_dollar_topics | FAIL | PASS | FAIL | suite assumption | The broker refuses a publish to a `$` topic (PUBACK 0x90 with a Reason String, `-restrict-dollar-topics`, MQTT 5 section 4.7.2 SHOULD). The suite's client asserts a PUBACK has 2 to 4 bytes and cannot parse it; the test itself says it is to be omitted where such publishes are refused. |
| test_server_keep_alive | FAIL | FAIL | PASS | suite assumption (config) | Expects Server Keep Alive 60 in the CONNACK, which the broker sends only when started with `-server-keep-alive`. |
| test_will_message, test_keepalive | PASS | PASS | FAIL | consequence of the option | `-server-keep-alive 60` overrides the 2 second keep alive these tests rely on, so the broker waits 90 seconds for what they wait seconds for. This is why `-server-keep-alive` has a run of its own. |
| test_flow_control2 | FLAKY | FLAKY | FLAKY | suite race | See above. |
| test_subscribe_failure | FAIL | FAIL | FAIL | unrunnable | Expects a SUBACK 0x80 for `test/nosubscribe`, a filter the reference broker is configured to refuse. natsmqtt5 grants it. The command line has no per-filter deny list (an `Authorizer` in the library could do it); the suite's `-n` option is parsed and never used. |
| test_server_topic_alias | FAIL | FAIL | FAIL | unrunnable | The suite's topics are shorter than 8 bytes; the broker aliases only names of 8 bytes or more (`minAliasedTopic`, `outalias.go`), since a shorter name gains nothing. Aliases are optional for a server. The alias path itself works: with a 19-byte topic the broker sends the alias on the first PUBLISH and an empty name on the next. |
| test_subscribe_identifiers | FAIL | FAIL | FAIL | unrunnable | Two overlapping subscriptions (`TopicA`, `TopicA/#`) get two PUBLISHes, one per subscription, each with its own identifier. MQTT 5 section 3.3.4 allows either one copy carrying every identifier or one copy per subscription, each carrying its own [MQTT-3.3.4-3]; the suite only accepts the first. `CONFORMANCE.md` records 3.3.4-4 as vacuous for this reason. |

The 22 tests that pass by default: basic, retained message, will message, will delay, zero-length client id,
assigned client id, offline message queueing, overlapping subscriptions, keep alive, redelivery on
reconnect, unsubscribe, session expiry, user properties, payload format, publication expiry,
subscribe options, request/response, client topic alias, maximum packet size, flow control 1 and 2,
shared subscriptions.

## Left over

No `ROADMAP.md` entry: the run found no broker bug. The `test_flow_control2` race above is the suite's, but a broker that closes with unread packets in its receive buffer can turn a DISCONNECT into a reset the client never reads (a lingering close: shut down the write side, drain reads briefly, then close); whether to do that after every protocol-error DISCONNECT is open, and would let the test be expected to PASS. Not run: the MQTT 3.1.1 suite
(`client_test.py`), the bridge and TLS suites.
