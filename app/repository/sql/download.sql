WITH
    deduplicated_family_slugs AS (
        SELECT DISTINCT
            ON (slug.family_import_id) slug.family_import_id,
            slug.created,
            slug.name
        FROM
            (
                SELECT
                    slug.family_import_id AS "family_import_id",
                    COUNT(*) AS COUNT
                FROM
                    slug
                WHERE
                    slug.family_import_id IS NOT NULL
                GROUP BY
                    slug.family_import_id
                HAVING
                    COUNT(*) > 1
            ) duplicates
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
                    slug.family_import_id AS "family_import_id",
                    COUNT(*) AS COUNT
                FROM
                    slug
                WHERE
                    slug.family_import_id IS NOT NULL
                GROUP BY
                    slug.family_import_id
                HAVING
                    COUNT(*) = 1
            ) non_duplicates
            LEFT JOIN slug ON non_duplicates.family_import_id = slug.family_import_id
        ORDER BY
            slug.family_import_id DESC,
            slug.created DESC,
            slug.ctid DESC
    ),
    most_recent_family_slugs AS (
        SELECT
            deduplicated_family_slugs.family_import_id AS "family_import_id",
            deduplicated_family_slugs.created AS "created",
            deduplicated_family_slugs.name AS "name"
        FROM
            deduplicated_family_slugs
        UNION ALL
        SELECT
            unique_family_slugs.family_import_id AS "family_import_id",
            unique_family_slugs.created AS "created",
            unique_family_slugs.name AS "name"
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
                    slug.family_document_import_id AS "family_document_import_id",
                    COUNT(*) AS COUNT
                FROM
                    slug
                WHERE
                    slug.family_document_import_id IS NOT NULL
                GROUP BY
                    slug.family_document_import_id
                HAVING
                    COUNT(*) > 1
            ) duplicates
            LEFT JOIN slug ON duplicates.family_document_import_id = slug.family_document_import_id
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
                    slug.family_document_import_id AS "family_document_import_id",
                    COUNT(*) AS COUNT
                FROM
                    slug
                WHERE
                    slug.family_document_import_id IS NOT NULL
                GROUP BY
                    slug.family_document_import_id
                HAVING
                    COUNT(*) = 1
            ) non_duplicates
            LEFT JOIN slug ON non_duplicates.family_document_import_id = slug.family_document_import_id
        ORDER BY
            slug.family_document_import_id DESC,
            slug.created DESC,
            slug.ctid DESC
    ),
    most_recent_doc_slugs AS (
        SELECT
            deduplicated_doc_slugs.family_document_import_id AS "family_document_import_id",
            deduplicated_doc_slugs.created,
            deduplicated_doc_slugs.name
        FROM
            deduplicated_doc_slugs
        UNION ALL
        SELECT
            unique_doc_slugs.family_document_import_id AS "family_document_import_id",
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
            family_event.family_import_id AS family_import_id,
            CASE
                WHEN COUNT(*) FILTER (
                    WHERE
                        family_event.event_type_name = (
                            family_event.valid_metadata -> 'datetime_event_name' ->> 0
                        )
                ) > 0 THEN MIN(
                    CASE
                        WHEN family_event.event_type_name = (
                            family_event.valid_metadata -> 'datetime_event_name' ->> 0
                        ) THEN family_event.date::TIMESTAMPTZ
                    END
                )
                ELSE MIN(family_event.date::TIMESTAMPTZ)
            END AS published_date,
            MAX(family_event.date::date) last_changed
        FROM
            family_event
        GROUP BY
            family_import_id
    )
SELECT
    ds.name AS "Document ID",
    p.title AS "Document Title",
    fs.name AS "Family ID",
    f.title AS "Family Title",
    f.description AS "Family Summary",
    n1.collection_titles AS "Collection Title(s)",
    n1.collection_descriptions AS "Collection Description(s)",
    INITCAP(d.valid_metadata::json #>> '{
  role,0}') AS "Document Role",
    d.variant_name AS "Document Variant",
    p.source_url AS "Document Content URL",
    INITCAP(d.valid_metadata::json #>> '{
  type,0}') AS "Document Type",
    CASE
        WHEN f.family_category = 'UNFCCC' THEN 'UNFCCC'
        ELSE INITCAP(f.family_category::TEXT)
    END "Category",
    ARRAY_TO_STRING(
        ARRAY(
            SELECT
                JSONB_ARRAY_ELEMENTS_TEXT(fm.value -> 'framework')
        ),
        ';'
    ) AS "Framework",
    n2.language AS "Language",
    o.name AS "Source",
    fg.geo_isos AS "Geography ISOs",
    fg.geo_display_values AS "Geographies",
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
    ) AS "Author Type",
    fp.published_date AS "First event in timeline",
    fp.last_changed AS "Last event in timeline",
    n3.event_type_names AS "Full timeline of events (types)",
    n3.event_dates AS "Full timeline of events (dates)",
    d.created::date AS "Date Added to System",
    f.last_modified::date AS "Last ModIFied on System",
    d.import_id AS "Internal Document ID",
    f.import_id AS "Internal Family ID",
    n1.collection_import_ids AS "Internal Collection ID(s)"
FROM
    physical_document p
    JOIN family_document d ON p.id = d.physical_document_id
    JOIN FAMILY f ON d.family_import_id = f.import_id
    FULL JOIN (
        SELECT
            family_geography.family_import_id AS "family_import_id",
            STRING_AGG(geography.value, ';') AS geo_isos,
            STRING_AGG(geography.display_value, ';') AS geo_display_values
        FROM
            geography
            INNER JOIN family_geography ON geography.id = family_geography.geography_id
        GROUP BY
            family_geography.family_import_id
    ) fg ON fg.family_import_id = f.import_id
    JOIN family_corpus fc ON f.import_id = fc.family_import_id
    JOIN corpus c ON fc.corpus_import_id = c.import_id
    JOIN organisation o ON c.organisation_id = o.id
    JOIN family_metadata fm ON fm.family_import_id = f.import_id
    FULL JOIN (
        SELECT
            collection_family.family_import_id AS "family_import_id",
            STRING_AGG(collection.import_id, ';') AS collection_import_ids,
            STRING_AGG(collection.title, ';') AS collection_titles,
            STRING_AGG(collection.description, ';') AS collection_descriptions
        FROM
            collection
            INNER JOIN collection_family ON collection_family.collection_import_id = collection.import_id
        GROUP BY
            collection_family.family_import_id
    ) n1 ON n1.family_import_id = f.import_id
    LEFT JOIN (
        SELECT
            p.id AS "id",
            STRING_AGG(
                l.name,
                ';'
                ORDER BY
                    l.name
            ) AS language
        FROM
            physical_document p
            LEFT JOIN physical_document_language pdl ON pdl.document_id = p.id
            LEFT JOIN language l ON l.id = pdl.language_id
        GROUP BY
            p.id
    ) n2 ON n2.id = d.physical_document_id
    FULL JOIN (
        SELECT
            family_event.family_import_id,
            STRING_AGG(family_event.import_id, ';') AS event_import_ids,
            STRING_AGG(family_event.title, ';') AS event_titles,
            STRING_AGG(family_event.event_type_name, ';') AS event_type_names,
            STRING_AGG(family_event.date::date::TEXT, ';') AS event_dates
        FROM
            family_event
            INNER JOIN FAMILY ON FAMILY.import_id = family_event.family_import_id
        GROUP BY
            family_event.family_import_id
    ) n3 ON n3.family_import_id = f.import_id
    LEFT JOIN most_recent_doc_slugs ds ON ds.family_document_import_id = d.import_id
    LEFT JOIN most_recent_family_slugs fs ON fs.family_import_id = f.import_id
    LEFT JOIN event_dates fp ON fp.family_import_id = f.import_id
WHERE
    d.last_modified < ':ingest_cycle_start'
    AND fc.corpus_import_id IN ':allowed_corpora_ids'
ORDER BY
    d.last_modified DESC,
    d.created DESC,
    d.ctid DESC,
    n1.family_import_id
