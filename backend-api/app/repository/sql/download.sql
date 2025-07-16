WITH deduplicated_family_slugs AS (
        SELECT DISTINCT
            ON (slug.family_import_id) slug.family_import_id,
            slug.created,
            slug.name
        FROM
            (
                SELECT
                    slug.family_import_id,
                    COUNT(*) AS count
                FROM
                    slug
                WHERE
                    slug.family_import_id IS NOT NULL
                GROUP BY
                    slug.family_import_id
                HAVING
                    COUNT(*) > 1
            ) AS duplicates
        LEFT JOIN slug ON duplicates.family_import_id = slug.family_import_id
        ORDER BY
            slug.family_import_id DESC,
            slug.created DESC,
            slug.ctid DESC
    ),

unique_family_slugs AS (
        SELECT DISTINCT
            ON (slug.family_import_id) slug.family_import_id,
            slug.created,
            slug.name
        FROM
            (
                SELECT
                    slug.family_import_id,
                    COUNT(*) AS count
                FROM
                    slug
                WHERE
                    slug.family_import_id IS NOT NULL
                GROUP BY
                    slug.family_import_id
                HAVING
                    COUNT(*) = 1
            ) AS non_duplicates
        LEFT JOIN
            slug
            ON non_duplicates.family_import_id = slug.family_import_id
        ORDER BY
            slug.family_import_id DESC,
            slug.created DESC,
            slug.ctid DESC
    ),

most_recent_family_slugs AS (
        SELECT
            deduplicated_family_slugs.family_import_id,
            deduplicated_family_slugs.created,
            deduplicated_family_slugs.name
        FROM
            deduplicated_family_slugs
        UNION ALL
        SELECT
            unique_family_slugs.family_import_id,
            unique_family_slugs.created,
            unique_family_slugs.name
        FROM
            unique_family_slugs
        ORDER BY
            family_import_id DESC,
            created DESC
    ),

deduplicated_doc_slugs AS (
        SELECT DISTINCT
            ON (slug.family_document_import_id) slug.family_document_import_id,
            slug.created,
            slug.name
        FROM
            (
                SELECT
                    slug.family_document_import_id,
                    COUNT(*) AS count
                FROM
                    slug
                WHERE
                    slug.family_document_import_id IS NOT NULL
                GROUP BY
                    slug.family_document_import_id
                HAVING
                    COUNT(*) > 1
            ) AS duplicates
        LEFT JOIN
            slug
            ON
                duplicates.family_document_import_id
                = slug.family_document_import_id
        ORDER BY
            slug.family_document_import_id DESC,
            slug.created DESC,
            slug.ctid DESC
    ),

unique_doc_slugs AS (
        SELECT DISTINCT
            ON (slug.family_document_import_id) slug.family_document_import_id,
            slug.created,
            slug.name
        FROM
            (
                SELECT
                    slug.family_document_import_id,
                    COUNT(*) AS count
                FROM
                    slug
                WHERE
                    slug.family_document_import_id IS NOT NULL
                GROUP BY
                    slug.family_document_import_id
                HAVING
                    COUNT(*) = 1
            ) AS non_duplicates
        LEFT JOIN
            slug
            ON
                non_duplicates.family_document_import_id
                = slug.family_document_import_id
        ORDER BY
            slug.family_document_import_id DESC,
            slug.created DESC,
            slug.ctid DESC
    ),

most_recent_doc_slugs AS (
        SELECT
            deduplicated_doc_slugs.family_document_import_id,
            deduplicated_doc_slugs.created,
            deduplicated_doc_slugs.name
        FROM
            deduplicated_doc_slugs
        UNION ALL
        SELECT
            unique_doc_slugs.family_document_import_id,
            unique_doc_slugs.created,
            unique_doc_slugs.name
        FROM
            unique_doc_slugs
        ORDER BY
            family_document_import_id DESC,
            created DESC
    ),

event_dates AS (
        SELECT
            family_event.family_import_id,
            CASE
                WHEN COUNT(*) FILTER (
                    WHERE
                        family_event.event_type_name = (
                            family_event.valid_metadata
                            -> 'datetime_event_name'
                            ->> 0
                        )
                ) > 0 THEN MIN(
                    CASE
                        WHEN family_event.event_type_name = (
                            family_event.valid_metadata
                            -> 'datetime_event_name'
                            ->> 0
                        ) THEN family_event.date::TIMESTAMPTZ
                    END
                )
                ELSE MIN(family_event.date::TIMESTAMPTZ)
            END AS published_date,
            MAX(family_event.date::DATE) AS last_changed
        FROM
            family_event
        GROUP BY
            family_event.family_import_id
    ),

fg AS (
        SELECT
            family_geography.family_import_id,
            STRING_AGG(geography.value, ';') AS geo_isos,
            STRING_AGG(geography.display_value, ';') AS geo_display_values
        FROM
            geography
            INNER JOIN
                family_geography
                ON geography.id = family_geography.geography_id
        GROUP BY
            family_geography.family_import_id
    ),

n1 AS (
        SELECT
            collection_family.family_import_id,
            STRING_AGG(collection.import_id, ';') AS collection_import_ids,
            STRING_AGG(collection.title, ';') AS collection_titles,
            STRING_AGG(collection.description, ';') AS collection_descriptions
        FROM
            collection
            INNER JOIN
                collection_family
                ON collection.import_id = collection_family.collection_import_id
        GROUP BY
            collection_family.family_import_id
    )

SELECT
    ds.name AS "Document ID",
    p.title AS "Document Title",
    fs.name AS "Family ID",
    f.title AS "Family Title",
    f.description AS "Family Summary",
    n1.collection_titles AS "Collection Title(s)",
    n1.collection_descriptions AS "Collection Description(s)",
    d.variant_name AS "Document Variant",
    p.source_url AS "Document Content URL",
    language_agg.display_name AS "Language",
    o.name AS "Source",
    fg.geo_isos AS "Geography ISOs",
    fg.geo_display_values AS "Geographies",
    fp.published_date AS "First event in timeline",
    fp.last_changed AS "Last event in timeline",
    n3.event_type_names AS "Full timeline of events (types)",
    n3.event_dates AS "Full timeline of events (dates)",
    d.created::DATE AS "Date Added to System",
    f.last_modified::DATE AS "Last Modified on System",
    d.import_id AS "Internal Document ID",
    f.import_id AS "Internal Family ID",
    fc.corpus_import_id AS "Internal Corpus ID",
    n1.collection_import_ids AS "Internal Collection ID(s)",
    INITCAP(d.valid_metadata::JSON #>> '{
  role,0}') AS "Document Role",
    INITCAP(d.valid_metadata::JSON #>> '{
  type,0}') AS "Document Type",
    CASE
        WHEN f.family_category = 'UNFCCC' THEN 'UNFCCC'
        WHEN f.family_category = 'MCF' THEN 'MCF'
        ELSE INITCAP(f.family_category::TEXT)
    END AS "Category",
    ARRAY_TO_STRING(
        ARRAY(
            SELECT
                JSONB_ARRAY_ELEMENTS_TEXT(fm.value -> 'framework')
        ),
        ';'
    ) AS "Framework",
    ARRAY_TO_STRING(
        ARRAY(
            SELECT
                JSONB_ARRAY_ELEMENTS_TEXT(fm.value -> 'topic')
        ),
        ';'
    ) AS "Topic/Response",
    ARRAY_TO_STRING(
        ARRAY(
            SELECT
                JSONB_ARRAY_ELEMENTS_TEXT(fm.value -> 'hazard')
        ),
        ';'
    ) AS "Hazard",
    ARRAY_TO_STRING(
        ARRAY(
            SELECT
                JSONB_ARRAY_ELEMENTS_TEXT(fm.value -> 'sector')
        ),
        ';'
    ) AS "Sector",
    ARRAY_TO_STRING(
        ARRAY(
            SELECT
                JSONB_ARRAY_ELEMENTS_TEXT(fm.value -> 'keyword')
        ),
        ';'
    ) AS "Keyword",
    ARRAY_TO_STRING(
        ARRAY(
            SELECT
                JSONB_ARRAY_ELEMENTS_TEXT(fm.value -> 'instrument')
        ),
        ';'
    ) AS "Instrument",
    ARRAY_TO_STRING(
        ARRAY(
            SELECT
                JSONB_ARRAY_ELEMENTS_TEXT(fm.value -> 'author')
        ),
        ';'
    ) AS "Author",
    ARRAY_TO_STRING(
        ARRAY(
            SELECT
                JSONB_ARRAY_ELEMENTS_TEXT(fm.value -> 'author_type')
        ),
        ';'
    ) AS "Author Type"
FROM
    physical_document AS p
    INNER JOIN family_document AS d ON p.id = d.physical_document_id
    INNER JOIN family AS f ON d.family_import_id = f.import_id
    FULL JOIN fg ON f.import_id = fg.family_import_id
    INNER JOIN family_corpus AS fc ON f.import_id = fc.family_import_id
    INNER JOIN corpus AS c ON fc.corpus_import_id = c.import_id
    INNER JOIN organisation AS o ON c.organisation_id = o.id
    INNER JOIN family_metadata AS fm ON f.import_id = fm.family_import_id
    FULL JOIN n1 ON f.import_id = n1.family_import_id
    LEFT JOIN (
        SELECT
            p.id,
            STRING_AGG(
                l.name,
                ';'
                ORDER BY
                    l.name
            ) AS display_name
        FROM
            physical_document AS p
            LEFT JOIN
                physical_document_language AS pdl
                ON p.id = pdl.document_id
            LEFT JOIN language AS l ON pdl.language_id = l.id
        GROUP BY
            p.id
    ) AS language_agg ON d.physical_document_id = language_agg.id
    FULL JOIN (
        SELECT
            family_event.family_import_id,
            STRING_AGG(family_event.import_id, ';') AS event_import_ids,
            STRING_AGG(family_event.title, ';') AS event_titles,
            STRING_AGG(family_event.event_type_name, ';') AS event_type_names,
            STRING_AGG(family_event.date::DATE::TEXT, ';') AS event_dates
        FROM
            family_event
            INNER JOIN
                family
                ON family_event.family_import_id = family.import_id
        GROUP BY
            family_event.family_import_id
    ) AS n3 ON f.import_id = n3.family_import_id
    LEFT JOIN
        most_recent_doc_slugs AS ds
        ON d.import_id = ds.family_document_import_id
    LEFT JOIN
        most_recent_family_slugs AS fs
        ON f.import_id = fs.family_import_id
    LEFT JOIN event_dates AS fp ON f.import_id = fp.family_import_id
WHERE
    d.last_modified < :ingest_cycle_start
    AND fc.corpus_import_id = ANY(:allowed_corpora_ids)
    AND d.document_status = 'PUBLISHED'
ORDER BY
    d.last_modified DESC,
    d.created DESC,
    d.ctid DESC,
    n1.family_import_id ASC
