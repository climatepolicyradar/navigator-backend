#!/bin/bash

CCLW_URLS=$(psql -t -c "select count(id) from physical_document \
    where (source_url like '%climate-laws%');" )

MISSING_CDN_OBJS=$(psql -t -c "select count(id) from physical_document \
    where (source_url like '%climate-laws%') and (cdn_object is null);" )

echo "--------------------------------------------------------------------------------"
echo "Starting checking database: ${PGHOST}:${PGPORT}/${PGDATABASE}"
echo "--------------------------------------------------------------------------------"

echo "Found ${CCLW_URLS} CCLW source urls."
echo "Found ${MISSING_CDN_OBJS} documents with no CDN object (missing_cdn_objects.csv)"

psql --csv  --output missing_cdn_objects.csv -c " \
  select id, source_url \
    from physical_document \
    where (source_url like '%climate-laws%') and (cdn_object is null);"

echo "Complete."
echo "--------------------------------------------------------------------------------"
