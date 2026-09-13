#!/bin/sh
# Run the named tests and fail unless every one of them reports PASS.
#
# The smoke tests skip unless NATSMQTT5_SMOKE_ADDR names a running broker, which
# is what keeps them out of the ordinary suite — and which means a green job
# proves nothing unless something checks they actually ran. That check used to
# be a human reading the log.
#
#	smoke.sh TestSmoke TestSmokeSessionMovesBetweenBrokers

set -eu

if [ "$#" -eq 0 ]; then
	echo "usage: smoke.sh <test name>..." >&2
	exit 2
fi

pattern=""
for name in "$@"; do
	if [ -z "$pattern" ]; then
		pattern="$name"
	else
		pattern="$pattern|$name"
	fi
done

out=$(mktemp)
trap 'rm -f "$out"' EXIT

status=0
go test -run "^(${pattern})\$" -count=1 -v . >"$out" 2>&1 || status=1
cat "$out"

for name in "$@"; do
	if ! grep -q -- "--- PASS: $name" "$out"; then
		echo "smoke test $name did not pass: it skipped, failed, or does not exist" >&2
		status=1
	fi
done

exit "$status"
