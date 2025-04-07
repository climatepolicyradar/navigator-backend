#!/bin/bash

set -euo pipefail

VESPA_CERT="${VESPA_CERT-}"
VESPA_KEY="${VESPA_KEY-}"
VESPA_SECRETS_LOCATION="${VESPA_SECRETS_LOCATION:-/secrets}"

# base64 decode key/cert supplied in env vars
if [[ -n ${VESPA_CERT} ]]; then
	echo "${VESPA_CERT}" | openssl base64 -A -d >"${VESPA_SECRETS_LOCATION}/cert.pem"
else
	echo "No Vespa certificate supplied, skipping file creation"
fi

if [[ -n ${VESPA_KEY} ]]; then
	echo "${VESPA_KEY}" | openssl base64 -A -d >"${VESPA_SECRETS_LOCATION}/key.pem"
else
	echo "No Vespa key supplied, skipping file creation"
fi

echo "Starting backend app"
exec uvicorn app.main:app --host 0.0.0.0 --port 8000
