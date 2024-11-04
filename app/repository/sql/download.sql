WITH
deduplicated_family_slugs as (
  SELECT
  distinct ON (slug.family_import_id)
  slug.family_import_id, slug.created, slug.name
  FROM (
    SELECT
    slug.family_import_id as "family_import_id",
    count(*) as count
    FROM slug
    WHERE slug.family_import_id is not null
    group by slug.family_import_id
    having count(*) > 1
  ) duplicates
  left join slug
  on duplicates.family_import_id = slug.family_import_id
  order by slug.family_import_id desc, slug.created desc, slug.ctid desc
),
unique_family_slugs as (
  SELECT
  distinct ON (slug.family_import_id)
  slug.family_import_id, slug.created, slug.name
  FROM (
    SELECT
    slug.family_import_id as "family_import_id",
    count(*) as count
    FROM slug
    WHERE slug.family_import_id is not null
    group by slug.family_import_id
    having count(*) = 1
  ) non_duplicates
  left join slug
  on non_duplicates.family_import_id = slug.family_import_id
  order by slug.family_import_id desc, slug.created desc, slug.ctid desc
  ), most_recent_family_slugs as (
  SELECT
  deduplicated_family_slugs.family_import_id as "family_import_id",
  deduplicated_family_slugs.created as "created",
  deduplicated_family_slugs.name as "name"
  FROM deduplicated_family_slugs
  UNION ALL
  SELECT
  unique_family_slugs.family_import_id as "family_import_id",
  unique_family_slugs.created as "created",
  unique_family_slugs.name as "name"
  FROM unique_family_slugs
  order by family_import_id desc, created desc
  ), deduplicated_doc_slugs as (
  SELECT
  distinct ON (slug.family_document_import_id)
  slug.family_document_import_id,
  slug.created,
  slug.name
  FROM (
    SELECT
    slug.family_document_import_id as "family_document_import_id",
    count(*) as count
    FROM slug
    WHERE slug.family_document_import_id is not null
    group by slug.family_document_import_id
    having count(*) >  1
  ) duplicates
  left join slug
  on duplicates.family_document_import_id = slug.family_document_import_id
  order by
  slug.family_document_import_id desc, slug.created desc, slug.ctid desc
),
unique_doc_slugs as (
  SELECT
  distinct ON (slug.family_document_import_id)
  slug.family_document_import_id,
  slug.created,
  slug.name
  FROM (
    SELECT
    slug.family_document_import_id as "family_document_import_id",
    count(*) as count
    FROM slug
    WHERE slug.family_document_import_id is not null
    group by slug.family_document_import_id
    having count(*) = 1
  ) non_duplicates
  left join slug
  on non_duplicates.family_document_import_id = slug.family_document_import_id
  order by
  slug.family_document_import_id desc, slug.created desc, slug.ctid desc
  ), most_recent_doc_slugs as (
  SELECT
  deduplicated_doc_slugs.family_document_import_id
  as "family_document_import_id",
  deduplicated_doc_slugs.created,
  deduplicated_doc_slugs.name
  FROM deduplicated_doc_slugs
  UNION ALL
  SELECT
  unique_doc_slugs.family_document_import_id as "family_document_import_id",
  unique_doc_slugs.created,
  unique_doc_slugs.name
  FROM unique_doc_slugs
  order by family_document_import_id desc, created desc
  ), event_dates as (
  SELECT
      family_event.family_import_id AS family_import_id,
      CASE
          WHEN COUNT(*) FILTER (
              WHERE family_event.event_type_name =
              (family_event.valid_metadata->'datetime_event_name'->>0)
          ) > 0 THEN
              MIN(CASE
                  WHEN family_event.event_type_name =
                  (family_event.valid_metadata->'datetime_event_name'->>0)
                  THEN family_event.date::TIMESTAMPTZ
              END)
          ELSE
              MIN(family_event.date::TIMESTAMPTZ)
      END AS published_date,
      max(family_event.date::date) last_changed
  FROM
      family_event
  GROUP BY
      family_import_id
)
SELECT
ds.name as "Document ID",
p.title as "Document Title",
fs.name as "Family ID",
f.title as "Family Title",
f.description as "Family Summary",
n1.collection_titles as "Collection Title(s)",
n1.collection_descriptions as "Collection Description(s)",
INITCAP(d.valid_metadata::json#>>'{
  role,0}') as
"Document Role",
d.variant_name as "Document Variant",
p.source_url as "Document Content URL",
INITCAP(d.valid_metadata::json#>>'{
  type,0}') as
"Document Type",
CASE
  WHEN f.family_category = 'UNFCCC' THEN 'UNFCCC'
  ELSE INITCAP(f.family_category::TEXT)
END "Category",
array_to_string(ARRAY(
    SELECT jsonb_array_elements_text(fm.value->'framework')), ';')
as "Framework",
n2.language as "Language",
o.name as "Source",
fg.geo_isos as "Geography ISOs",
fg.geo_display_values as "Geographies",
array_to_string(ARRAY(
    SELECT jsonb_array_elements_text(fm.value->'topic')), ';')
as "Topic/Response",
array_to_string(ARRAY(
    SELECT jsonb_array_elements_text(fm.value->'hazard')), ';')
as "Hazard",
array_to_string(ARRAY(
    SELECT jsonb_array_elements_text(fm.value->'sector')), ';')
as "Sector",
array_to_string(ARRAY(
    SELECT jsonb_array_elements_text(fm.value->'keyword')), ';')
as "Keyword",
array_to_string(ARRAY(
    SELECT jsonb_array_elements_text(fm.value->'instrument')), ';')
as "Instrument",
array_to_string(ARRAY(
    SELECT jsonb_array_elements_text(fm.value->'author')), ';')
as "Author",
array_to_string(ARRAY(
    SELECT jsonb_array_elements_text(fm.value->'author_type')), ';')
as "Author Type",
fp.published_date as "First event in timeline",
fp.last_changed as "Last event in timeline",
n3.event_type_names as "Full timeline of events (types)",
n3.event_dates as "Full timeline of events (dates)",
d.created::date as "Date Added to System",
f.last_modified::date as "Last ModIFied on System",
d.import_id as "Internal Document ID",
f.import_id as "Internal Family ID",
n1.collection_import_ids as "Internal Collection ID(s)"
FROM physical_document p
JOIN family_document d
ON p.id = d.physical_document_id
JOIN family f
ON d.family_import_id = f.import_id
FULL JOIN (
  SELECT
  family_geography.family_import_id as "family_import_id",
  string_agg(geography.value, ';') AS geo_isos,
  string_agg(geography.display_value, ';') AS geo_display_values
  FROM
  geography
  INNER JOIN family_geography
  ON geography.id = family_geography.geography_id
  GROUP BY family_geography.family_import_id
) fg ON fg.family_import_id=f.import_id
join family_corpus fc
on f.import_id = fc.family_import_id
join corpus c
on fc.corpus_import_id = c.import_id
join organisation o
on c.organisation_id = o.id
join family_metadata fm
on fm.family_import_id = f.import_id
FULL JOIN (
  SELECT
  collection_family.family_import_id as "family_import_id",
  string_agg(collection.import_id, ';') AS collection_import_ids,
  string_agg(collection.title, ';') AS collection_titles,
  string_agg(collection.description, ';') AS collection_descriptions
  FROM
  collection
  INNER JOIN collection_family
  ON collection_family.collection_import_id = collection.import_id
  GROUP BY collection_family.family_import_id
) n1 ON n1.family_import_id=f.import_id
left JOIN (
  SELECT
  p.id as "id",
  string_agg(l.name, ';' ORDER BY l.name) AS language
  FROM physical_document p
  left join physical_document_language pdl
  on pdl.document_id = p.id
  left join language l
  on l.id = pdl.language_id
  GROUP  BY p.id
) n2 ON n2.id=d.physical_document_id
FULL JOIN (
  SELECT
  family_event.family_import_id,
  string_agg(family_event.import_id, ';') AS event_import_ids,
  string_agg(family_event.title, ';') AS event_titles,
  string_agg(family_event.event_type_name, ';') AS event_type_names,
  string_agg(family_event.date::date::text, ';') AS event_dates
  FROM family_event
  INNER JOIN  family ON family.import_id = family_event.family_import_id
  GROUP BY family_event.family_import_id
) n3 ON n3.family_import_id=f.import_id
LEFT JOIN most_recent_doc_slugs ds
on ds.family_document_import_id = d.import_id
LEFT JOIN most_recent_family_slugs fs on fs.family_import_id = f.import_id
LEFT JOIN event_dates fp on fp.family_import_id = f.import_id
WHERE d.last_modified < '{ingest_cycle_start}' AND fc.corpus_import_id in ({allowed_corpora_ids})
ORDER BY d.last_modified desc, d.created desc, d.ctid desc, n1.family_import_id
