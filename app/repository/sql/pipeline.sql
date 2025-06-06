WITH deduplicated_family_slugs AS (
    SELECT DISTINCT
    ON (slug.family_import_id)
        slug.family_import_id,
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
    LEFT JOIN
        slug
        ON duplicates.family_import_id = slug.family_import_id
    ORDER BY
        slug.family_import_id DESC,
        slug.created DESC,
        slug.ctid DESC
),

unique_family_slugs AS (
    SELECT DISTINCT
    ON (slug.family_import_id)
        slug.family_import_id,
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
    ON (slug.family_document_import_id)
        slug.family_document_import_id,
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
    ON (slug.family_document_import_id)
        slug.family_document_import_id,
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
            WHEN
                COUNT(*) FILTER (
                    WHERE
                    family_event.event_type_name = (
                        family_event.valid_metadata
                        -> 'datetime_event_name'
                        ->> 0
                    )
                ) > 0
                THEN MIN(
                    CASE
                        WHEN family_event.event_type_name = (
                            family_event.valid_metadata
                            -> 'datetime_event_name'
                            ->> 0
                        ) THEN family_event.date::TIMESTAMPTZ
                    END
                )
            ELSE MIN(family_event.date::TIMESTAMPTZ)
        END AS published_date
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

geos AS (
    SELECT
        family_geography.family_import_id,
        JSON_AGG(DISTINCT geography.value) AS geographies
    FROM
        family_geography
    INNER JOIN geography ON family_geography.geography_id = geography.id
    GROUP BY
        family_geography.family_import_id
)

SELECT
    f.title AS family_title,
    p.title AS physical_document_title,
    f.description AS family_description,
    fp.published_date AS family_published_date,
    d.import_id AS family_document_import_id,
    ds.name AS family_document_slug,
    f.import_id AS family_import_id,
    fs.name AS family_slug,
    p.source_url AS physical_document_source_url,
    o.name AS organisation_name,
    geos.geographies,
    c.import_id AS corpus_import_id,
    c.corpus_type_name,
    langs.languages,
    fm.value AS family_metadata,
    d.valid_metadata AS family_document_metadata,
    CASE
        WHEN
            f.family_category IN ('UNFCCC', 'MCF')
            THEN UPPER(f.family_category::TEXT)
        ELSE INITCAP(f.family_category::TEXT)
    END AS family_category,
    d.valid_metadata::JSON #>> '{type,0}' AS family_document_type
FROM
    physical_document AS p
INNER JOIN family_document AS d ON p.id = d.physical_document_id
INNER JOIN family AS f ON d.family_import_id = f.import_id
FULL JOIN fg ON f.import_id = fg.family_import_id
INNER JOIN family_corpus AS fc ON f.import_id = fc.family_import_id
INNER JOIN corpus AS c ON fc.corpus_import_id = c.import_id
INNER JOIN organisation AS o ON c.organisation_id = o.id
INNER JOIN family_metadata AS fm ON f.import_id = fm.family_import_id
LEFT OUTER JOIN (
    SELECT
        family_document.import_id AS family_document_import_id,
        JSON_AGG(DISTINCT language.name) AS languages
    FROM
        family_document
    INNER JOIN
        physical_document_language
        ON
            family_document.physical_document_id
            = physical_document_language.document_id
    INNER JOIN
        language
        ON physical_document_language.language_id = language.id
    GROUP BY
        family_document.import_id
) AS langs ON d.import_id = langs.family_document_import_id
LEFT OUTER JOIN geos ON f.import_id = geos.family_import_id
LEFT JOIN
    most_recent_doc_slugs AS ds
    ON d.import_id = ds.family_document_import_id
LEFT JOIN
    most_recent_family_slugs AS fs
    ON f.import_id = fs.family_import_id
LEFT JOIN event_dates AS fp ON f.import_id = fp.family_import_id
WHERE
    d.document_status != 'DELETED'
    AND fg.family_import_id = f.import_id
ORDER BY
    d.last_modified DESC,
    d.created DESC,
    d.ctid DESC,
    f.import_id ASC
