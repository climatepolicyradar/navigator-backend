#!/bin/bash
#
# Posts the request to validate the ingest.
#
# 
export CSV_FILE=$1

# ---------- Functions ----------

get_token() {
    curl -s \
    -H "Content-Type: application/x-www-form-urlencoded" \
    -d "username=$SUPERUSER_EMAIL&password=$SUPERUSER_PASSWORD" \
        $TEST_HOST/api/tokens | \
        jq ".access_token" | tr -d '"'
}

validate_csv() {
    TOKEN=$(get_token)
    curl -s \
        -H "Authorization: Bearer ${TOKEN}" \
        -F "law_policy_csv=@${CSV_FILE}" \
        $TEST_HOST/api/v1/admin/bulk-ingest/validate/cclw/law-policy | jq
}

echo
echo "👉👉👉  Validate CSV"
validate_csv > validation.json
ls -lh validation.json

echo
echo "👉👉👉  Detailed Output"
cat validation.json | jq " (.errors)" | jq ".[] | (.details)"

echo
echo "👉👉👉  Summary"
cat validation.json | jq " (.message)"
