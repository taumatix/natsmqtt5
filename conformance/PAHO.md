# Eclipse Paho interoperability suite, MQTT 5

An external check on the statement-by-statement review in `CONFORMANCE.md`: the MQTT 5 client
tests of `eclipse/paho.mqtt.testing` driven at natsmqtt5 over a real socket.

- Date: 2026-11-01
- Broker: natsmqtt5 v1.0.1 (`758afd7`), with a local nats-server (JetStream on)
- Suite: `https://github.com/eclipse/paho.mqtt.testing` at `fdab9d2b1ce125385135e5d9f282a0ad315268cc`,
  `interoperability/client_test5.py` (27 tests, plain Python 3, no dependencies)

## Reproduce

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

Default flags: **22 passed, 5 failed** of 27. None of the 5 is a broker bug. Two pass when the
broker is started with the option that matches what the test assumes.

| Test | Default | Class | Reason |
|---|---|---|---|
| test_dollar_topics | FAIL | suite assumption | The broker refuses a publish to a `$` topic (PUBACK 0x90 with a Reason String, `-restrict-dollar-topics`, MQTT 5 section 4.7.2 SHOULD). The suite's client asserts a PUBACK has 2 to 4 bytes and cannot parse it; the test itself says it is to be omitted where such publishes are refused. Passes with `-restrict-dollar-topics=false`. |
| test_server_keep_alive | FAIL | suite assumption (config) | Expects Server Keep Alive 60 in the CONNACK. The broker sends it only when started with `-server-keep-alive`. Passes with `-server-keep-alive 60`. |
| test_subscribe_failure | FAIL | suite assumption (config) | Expects a SUBACK 0x80 for `test/nosubscribe`, a filter the reference broker is configured to refuse. natsmqtt5 grants it. The command line has no per-filter deny list (an `Authorizer` in the library could do it); the suite's `-n` option is parsed and never used. |
| test_server_topic_alias | FAIL | suite assumption (design) | The suite's topics are `TopicA` and shorter-than-8-byte names; the broker aliases only names of 8 bytes or more (`minAliasedTopic`, `outalias.go`), since a shorter name gains nothing. Aliases are optional for a server. The alias path itself works: with a 19-byte topic the broker sends the alias on the first PUBLISH and an empty name on the next. |
| test_subscribe_identifiers | FAIL | suite assumption | Two overlapping subscriptions (`TopicA`, `TopicA/#`) get two PUBLISHes, one per subscription, each with its own identifier. MQTT 5 section 3.3.4 allows either one copy carrying every identifier or one copy per subscription, each carrying its own [MQTT-3.3.4-3]; the suite only accepts the first. `CONFORMANCE.md` records 3.3.4-4 as vacuous for this reason. |

The 22 passing tests: basic, retained message, will message, will delay, zero-length client id,
assigned client id, offline message queueing, overlapping subscriptions, keep alive, redelivery on
reconnect, unsubscribe, session expiry, user properties, payload format, publication expiry,
subscribe options, request/response, client topic alias, maximum packet size, flow control 1 and 2,
shared subscriptions.

## Left over

No `ROADMAP.md` entry: the run found no broker bug. Not run: the MQTT 3.1.1 suite
(`client_test.py`), the bridge and TLS suites.
