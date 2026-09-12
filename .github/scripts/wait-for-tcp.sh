#!/bin/sh
# Wait until a TCP port accepts connections, so a CI step does not race a
# container that is still starting.
#
#	wait-for-tcp.sh 127.0.0.1 1883 [timeout-seconds]

set -eu

host=$1
port=$2
deadline=$(($(date +%s) + ${3:-60}))

while ! nc -z "$host" "$port" 2>/dev/null; do
	if [ "$(date +%s)" -ge "$deadline" ]; then
		echo "timed out waiting for $host:$port" >&2
		exit 1
	fi
	sleep 0.5
done

echo "$host:$port is accepting connections"
