#!/bin/bash

rm_existing() {
  [ -f $1 ] && rm $1 || echo "$1 Clean"
}

rm_existing cdn_urls_input.txt 
rm_existing cdn_urls_missing.csv  # Contains missing CDN objects from CCLW
rm_existing cdn_urls_results.txt


echo "--------------------------------------------------------------------------------"
echo "Starting checking database: ${PGHOST}:${PGPORT}/${PGDATABASE}"
echo "--------------------------------------------------------------------------------"

# Get the script input from the database  
psql -At --output cdn_urls_input.txt -c "select source_url,id,md5_sum from physical_document \
    where (source_url like '%climate-laws%');"
CCLW_URLS=$(cat cdn_urls_input.txt | wc -l)
echo "Written ${CCLW_URLS} CCLW source urls (cdn_urls_input.txt)"

# Record anything missing from the database
MISSING_CDN_OBJS=$(psql -t -c "select count(id) from physical_document \
    where (source_url like '%climate-laws%') and (cdn_object is null);" )

psql --csv  --output cdn_urls_missing.csv -c " \
  select id, source_url \
    from physical_document \
    where (source_url like '%climate-laws%') and (cdn_object is null);"

echo "Written ${MISSING_CDN_OBJS} documents with no CDN object (cdn_urls_missing.csv)"

echo "--------------------------------------------------------------------------------"
echo "Checking all Physical Documents exist with the correct MD5 ..."
echo
echo "Results written to cdn_urls_results.txt"
echo
echo
for line in $(cat cdn_urls_input.txt)
do
  url=$(echo $line | cut -d '|' -f1)
  id=$(echo $line | cut -d '|' -f2)
  md5=$(echo $line | cut -d '|' -f3)

  echo -n "${id}, ${md5}, "  >> cdn_urls_results.txt

  short="${url:0:24}…${url: -30}"
  echo -ne "${id}  ${short}... checking url                                   \r"
  found=$(curl -I -s $url | grep "HTTP/2 302" >/dev/null && echo "found" || echo "missing")
  if [ ${found} == "missing" ]
  then
    echo "Missing url: ${url}"  >> cdn_urls_results.txt
    continue
  fi

  # Now check MD5 
  if [ "x${md5}" == "x" ]
  then
    echo "Missing md5 skipping" >> cdn_urls_results.txt
    continue
  fi

  # Now check MD5 against download
  tmp=/tmp/tmp-cclw-${id}
  echo -ne "${id}  ${short}... checking md5                                   \r"
  # if the file has not already been downloaded
  if [ ! -f ${tmp} ]
  then
    curl -kLs ${url} --output ${tmp}
  fi

  md5_found=$(md5sum ${tmp} | cut -d ' ' -f1)
  if [ ! ${md5} == ${md5_found} ]
  then
    echo "Mismatch md5: ${md5_found}"  >> cdn_urls_results.txt
  else
    echo "OK"  >> cdn_urls_results.txt
  fi
done

echo "Complete."
echo "--------------------------------------------------------------------------------"
