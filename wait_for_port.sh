#!/bin/bash
# Takes 3 args: host, port, timeout
#
host="${1:-localhost}"
port="${2:-8888}"
timeout="${3:-60}"

echo "Waiting for ${host} on port ${port}"
timeout "${timeout}" sh -c 'while ! nc -q0 -w1 -z '"${host}"' '"${port}"' </dev/null >/dev/null 2>&1; do sleep 1; done' &&
	echo "Connection was established for ${host} on port ${port}"
