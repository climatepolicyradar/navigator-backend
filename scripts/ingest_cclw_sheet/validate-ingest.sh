#!/bin/bash

export CSV_FILE=$1

# ---------- Functions ----------

get_token() {
    curl -s \
    -H "Content-Type: application/x-www-form-urlencoded" \
    -d "username=$SUPERUSER_EMAIL&password=$SUPERUSER_PASSWORD" \
        $API_HOST/api/tokens | \
        jq ".access_token" | tr -d '"'
}

validate_csv() {
    TOKEN=$(get_token)
    curl -s \
        -H "Authorization: Bearer ${TOKEN}" \
        -F "law_policy_csv=@${CSV_FILE}" \
        $API_HOST/api/v1/admin/bulk-ingest/validate/cclw/law-policy
}

echo "👉👉👉  Validate CSV"
validate_csv

