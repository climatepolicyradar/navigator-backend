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
  family_event.family_import_id as family_import_id,
  min(CASE
  WHEN family_event.event_type_name='Passed/Approved' then
    family_event.date::date
  ELSE family_event.date::date
end) published_date,
max(family_event.date::date) last_changed
FROM family_event
group by family_import_id
)

SELECT
  f.title as "family_title",
  p.title as "physical_document_title",
  f.description as "family_description",
  f.family_category as "family_category",
  fp.published_date as "family_published_date",
  d.import_id as "family_document_import_id",
  ds.name as "family_document_slug",
  f.import_id as "family_import_id",
  fs.name as "family_slug",
  p.source_url as "physical_document_source_url",
  d.valid_metadata::json#>>'{type,0}' as "family_document_type",
  o.name as "organisation_name",
  geos.geographies as "geographies",
  c.import_id as "corpus_import_id",
  c.corpus_type_name as "corpus_type_name",
  langs.languages AS "languages",
  fm.value as "family_metadata",
  d.valid_metadata as "family_document_metadata"
FROM physical_document p
JOIN family_document d ON p.id = d.physical_document_id
JOIN family f ON d.family_import_id = f.import_id
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
join family_corpus fc on f.import_id = fc.family_import_id
join corpus c on fc.corpus_import_id = c.import_id
join organisation o on c.organisation_id = o.id
join family_metadata fm on fm.family_import_id = f.import_id
LEFT OUTER JOIN (
    SELECT family_document.import_id AS family_document_import_id,
           json_agg(distinct(language.name)) AS languages
    FROM family_document
    JOIN physical_document_language ON physical_document_language.document_id = family_document.physical_document_id
    JOIN language ON language.id = physical_document_language.language_id
    GROUP BY family_document.import_id
) AS langs ON langs.family_document_import_id = d.import_id
LEFT OUTER JOIN (
	SELECT
		family_geography.family_import_id AS family_import_id,
		json_agg(distinct(geography.value)) AS geographies
	FROM family_geography
	JOIN geography ON geography.id = family_geography.geography_id
	GROUP BY family_geography.family_import_id
) AS geos ON geos.family_import_id = f.import_id
LEFT JOIN most_recent_doc_slugs ds on ds.family_document_import_id = d.import_id
LEFT JOIN most_recent_family_slugs fs on fs.family_import_id = f.import_id
LEFT JOIN event_dates fp on fp.family_import_id = f.import_id
WHERE 
  d.document_status != 'DELETED' AND fg.family_import_id = f.import_id
ORDER BY
  d.last_modified desc, d.created desc, d.ctid desc, f.import_id
