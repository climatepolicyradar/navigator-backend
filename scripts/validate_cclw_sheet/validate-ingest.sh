#!/bin/bash
#
# Posts the request to validate the ingest.
#
# 
set -eou pipefail

CSV_FILE=$1
USER=${SUPERUSER_EMAIL:-user@navigator.com}
PASSWORD=${SUPERUSER_PASSWORD:-password}
 
# ---------- Functions ----------

wait_for_server() {
    printf 'Waiting for server.'
    until $(curl --output /dev/null --silent  ${TEST_HOST}/health); do
        printf '.'
        sleep 1
    done
    echo
}

get_token() {
    curl -s \
    -H "Content-Type: application/x-www-form-urlencoded" \
    -d "username=${USER}&password=${PASSWORD}" \
        ${TEST_HOST}/api/tokens | \
        jq ".access_token" | tr -d '"'
}

validate_csv() {
    TOKEN=$(get_token)
    curl -s \
        -H "Authorization: Bearer ${TOKEN}" \
        -F "law_policy_csv=@${CSV_FILE}" \
        ${TEST_HOST}/api/v1/admin/bulk-ingest/validate/cclw
}

echo "Validating as ${USER}"
wait_for_server

echo
echo "👉👉👉  Validate CSV"
validate_csv > validation.json
ls -lh validation.json
cat validation.json

echo
echo "👉👉👉  Detailed Output"
cat validation.json | jq " (.errors)" | jq ".[] | (.details)"

echo
echo "👉👉👉  Summary"
cat validation.json | jq " (.message)"

# Ensure return code only succeeds when no failures.
grep ", 0 Fail" validation.json
