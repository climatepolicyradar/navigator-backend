#!/bin/bash

API_HOST=http://localhost:8888

do_browse_local() {
    curl "$API_HOST/api/v1/searches?group_documents=true" \
        -s \
        -X POST \
        -H 'Accept: application/json' \
        -H 'Content-Type: application/json'\
        --data-raw '{"query_string":"","exact_match":true,"keyword_filters":{},"sort_field":null,"sort_order":"desc","limit":100,"offset":0}' 
}

do_browse_local | jq | grep time