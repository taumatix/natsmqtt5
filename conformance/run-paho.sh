#!/usr/bin/env bash
# Runs the Eclipse Paho MQTT 5 interoperability suite against natsmqtt5 and compares each test's
# result with conformance/paho-expected.tsv (see conformance/PAHO.md). Exits non-zero on ANY
# difference, a new failure or an expected failure that now passes.
#
# Three runs, each against a fresh nats-server and broker: "default", "dollar" (started with
# -restrict-dollar-topics=false, which test_dollar_topics assumes) and "keepalive" (started with
# -server-keep-alive 60, which test_server_keep_alive assumes). Needs go, git and python3; no
# container runtime. About three minutes per run.
set -euo pipefail
export PYTHONDONTWRITEBYTECODE=1

PAHO_SHA=fdab9d2b1ce125385135e5d9f282a0ad315268cc
root=$(cd "$(dirname "$0")/.." && pwd)
work=${WORK_DIR:-$(mktemp -d)}
paho=${PAHO_DIR:-$work/paho}
mkdir -p "$work"
pids=()
trap 'kill "${pids[@]}" 2>/dev/null || true' EXIT

if [ ! -d "$paho/.git" ]; then
  git clone --quiet https://github.com/eclipse/paho.mqtt.testing "$paho"
fi
git -C "$paho" checkout --quiet "$PAHO_SHA"

(cd "$root" && go build -o "$work/nats-server" github.com/nats-io/nats-server/v2 && go build -o "$work/natsmqtt5" ./cmd/natsmqtt5)

wait_tcp() {
  for _ in $(seq 100); do
    (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null && return 0
    sleep 0.2
  done
  echo "nothing listening on $1" >&2
  return 1
}

# run NAME NATS_PORT MQTT_PORT BROKER_FLAGS...
run() {
  local name=$1 nport=$2 mport=$3
  shift 3
  rm -rf "$work/js-$name"
  "$work/nats-server" -js -a 127.0.0.1 -p "$nport" -sd "$work/js-$name" >"$work/nats-$name.log" 2>&1 &
  local npid=$!
  pids+=("$npid")
  wait_tcp "$nport"
  "$work/natsmqtt5" -nats "nats://127.0.0.1:$nport" -listen "127.0.0.1:$mport" "$@" >"$work/broker-$name.log" 2>&1 &
  local bpid=$!
  pids+=("$bpid")
  wait_tcp "$mport"
  # The suite is run unmodified, from its own directory. Its option parsing wants "--port N".
  # Its exit status says only that something failed; the comparison reads the per-test results.
  (cd "$paho/interoperability" && python3 client_test5.py --port "$mport" -v >"$work/paho-$name.txt" 2>&1) || true
  kill "$bpid" "$npid" 2>/dev/null || true
  wait "$bpid" "$npid" 2>/dev/null || true
}

run default 24222 21883
run dollar 24223 21884 -restrict-dollar-topics=false
run keepalive 24224 21885 -server-keep-alive 60

python3 "$root/conformance/paho-compare.py" "$@" "$root/conformance/paho-expected.tsv" \
  "$paho/interoperability/client_test5.py" \
  "default=$work/paho-default.txt" "dollar=$work/paho-dollar.txt" "keepalive=$work/paho-keepalive.txt"
