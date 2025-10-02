-- Optimised version with improved performance
WITH
-- Single CTE for all slug deduplication (family and document)
recent_slugs AS (
    SELECT DISTINCT ON (
        COALESCE(family_import_id::TEXT, '')
        || '|'
        || COALESCE(family_document_import_id::TEXT, '')
    )
        family_import_id,
        family_document_import_id,
        name,
        created
    FROM slug
    WHERE family_import_id IS NOT NULL OR family_document_import_id IS NOT NULL
    ORDER BY
        COALESCE(family_import_id::TEXT, '')
        || '|'
        || COALESCE(family_document_import_id::TEXT, ''),
        created DESC,
        ctid DESC
),

-- Pre-filter families based on corpus membership and document status
eligible_families AS (
    SELECT DISTINCT f.import_id
    FROM family AS f
    INNER JOIN family_corpus AS fc ON f.import_id = fc.family_import_id
    INNER JOIN family_document AS fd ON f.import_id = fd.family_import_id
    WHERE fc.corpus_import_id = ANY(:allowed_corpora_ids)
      AND fd.document_status = 'PUBLISHED'
      AND fd.last_modified < :ingest_cycle_start
),

-- Combine event aggregations
family_events_agg AS (
    SELECT
        fe.family_import_id,
        -- Event dates calculation (simplified logic)
        CASE
            WHEN COUNT(*) FILTER (
                WHERE fe.event_type_name
                = (fe.valid_metadata -> 'datetime_event_name' ->> 0)
            ) > 0 THEN
                MIN(CASE
                    WHEN
                        fe.event_type_name
                        = (fe.valid_metadata -> 'datetime_event_name' ->> 0)
                    THEN fe.date::TIMESTAMPTZ
                END)
            ELSE MIN(fe.date::TIMESTAMPTZ)
        END AS published_date,
        MAX(fe.date::DATE) AS last_changed,
        -- Event timeline aggregations
        STRING_AGG(fe.import_id, ';') AS event_import_ids,
        STRING_AGG(fe.title, ';') AS event_titles,
        STRING_AGG(fe.event_type_name, ';') AS event_type_names,
        STRING_AGG(fe.date::DATE::TEXT, ';') AS event_dates
    FROM family_event AS fe
    INNER JOIN eligible_families AS ef ON fe.family_import_id = ef.import_id
    GROUP BY fe.family_import_id
),

-- Geography aggregation
family_geo_agg AS (
    SELECT
        fg.family_import_id,
        STRING_AGG(g.value, ';') AS geo_isos,
        STRING_AGG(g.display_value, ';') AS geo_display_values
    FROM family_geography AS fg
    INNER JOIN geography AS g ON fg.geography_id = g.id
    INNER JOIN eligible_families AS ef ON fg.family_import_id = ef.import_id
    GROUP BY fg.family_import_id
),

-- Collection aggregation
family_collections_agg AS (
    SELECT
        cf.family_import_id,
        STRING_AGG(c.import_id, ';') AS collection_import_ids,
        STRING_AGG(c.title, ';') AS collection_titles,
        STRING_AGG(c.description, ';') AS collection_descriptions
    FROM collection_family AS cf
    INNER JOIN collection AS c ON cf.collection_import_id = c.import_id
    INNER JOIN eligible_families AS ef ON cf.family_import_id = ef.import_id
    GROUP BY cf.family_import_id
),

-- Language aggregation
doc_languages AS (
    SELECT
        pd.id,
        STRING_AGG(l.name, ';' ORDER BY l.name) AS display_name
    FROM physical_document AS pd
    LEFT JOIN physical_document_language AS pdl ON pd.id = pdl.document_id
    LEFT JOIN language AS l ON pdl.language_id = l.id
    GROUP BY pd.id
),

-- Pre-compute metadata extractions to avoid repeated JSONB operations
family_metadata_extracted AS (
    SELECT
        fm.family_import_id,
        ARRAY_TO_STRING(
            ARRAY(SELECT JSONB_ARRAY_ELEMENTS_TEXT(fm.value -> 'region')),
            ';'
        ) AS region,
        ARRAY_TO_STRING(
            ARRAY(SELECT JSONB_ARRAY_ELEMENTS_TEXT(fm.value -> 'status')), ';'
        ) AS status,
        ARRAY_TO_STRING(
            ARRAY(SELECT JSONB_ARRAY_ELEMENTS_TEXT(fm.value -> 'project_id')),
            ';'
        ) AS project_id,
        ARRAY_TO_STRING(
            ARRAY(SELECT JSONB_ARRAY_ELEMENTS_TEXT(fm.value -> 'sector')), ';'
        ) AS sector,
        ARRAY_TO_STRING(
            ARRAY(SELECT JSONB_ARRAY_ELEMENTS_TEXT(fm.value -> 'project_url')),
            ';'
        ) AS project_url,
        ARRAY_TO_STRING(
            ARRAY(
                SELECT
                    JSONB_ARRAY_ELEMENTS_TEXT(fm.value -> 'implementing_agency')
            ),
            ';'
        ) AS implementing_agency,
        ARRAY_TO_STRING(
            ARRAY(
                SELECT
                    JSONB_ARRAY_ELEMENTS_TEXT(
                        fm.value -> 'project_value_fund_spend'
                    )
            ),
            ';'
        ) AS project_value_fund_spend,
        ARRAY_TO_STRING(
            ARRAY(
                SELECT
                    JSONB_ARRAY_ELEMENTS_TEXT(
                        fm.value -> 'project_value_co_financing'
                    )
            ),
            ';'
        ) AS project_value_co_financing
    FROM family_metadata AS fm
    INNER JOIN eligible_families AS ef ON fm.family_import_id = ef.import_id
)

-- Main query with optimised joins
SELECT
    rs_doc.name AS "Document ID",
    p.title AS "Document Title",
    rs_fam.name AS "Family ID",
    f.title AS "Family Title",
    f.description AS "Family Summary",
    fca.collection_titles AS "Collection Title(s)",
    fca.collection_descriptions AS "Collection Description(s)",
    d.variant_name AS "Document Variant",
    p.source_url AS "Document Content URL",
    dl.display_name AS "Language",
    o.name AS "Source",
    fme.region AS "Region",
    fga.geo_isos AS "Geography ISOs",
    fga.geo_display_values AS "Geographies",
    fme.status AS "Status",
    fme.implementing_agency AS "Implementing Agency",
    fme.sector AS "Sector",
    fme.project_id AS "External Project ID",
    fme.project_url AS "Project URL",
    fme.project_value_co_financing
        AS "Project Value $ (Co-financing)", -- noqa:disable=RF05
    fme.project_value_fund_spend
        AS "Project Value $ (Fund Spend)", -- noqa:disable=RF05
    fea.published_date AS "First event in timeline",
    fea.last_changed AS "Last event in timeline",
    fea.event_type_names AS "Full timeline of events (types)",
    fea.event_dates AS "Full timeline of events (dates)",
    d.created::DATE AS "Date Added to System",
    f.last_modified::DATE AS "Last Modified on System",
    d.import_id AS "Internal Document ID",
    f.import_id AS "Internal Family ID",
    fc.corpus_import_id AS "Internal Corpus ID",
    fca.collection_import_ids AS "Internal Collection ID(s)",
    CONCAT(
        :url_base, '/document/', rs_fam.name
    ) AS "Family URL",
    CONCAT(
        :url_base, '/documents/', rs_doc.name
    ) AS "Document URL",
    INITCAP(d.valid_metadata::JSON #>> '{role,0}') AS "Document Role",
    INITCAP(d.valid_metadata::JSON #>> '{type,0}') AS "Document Type",
    CASE
        WHEN f.family_category = 'UNFCCC' THEN 'UNFCCC'
        WHEN f.family_category = 'MCF' THEN 'MCF'
        ELSE INITCAP(f.family_category::TEXT)
    END AS "Category"
FROM family_document AS d
INNER JOIN physical_document AS p ON d.physical_document_id = p.id
INNER JOIN family AS f ON d.family_import_id = f.import_id
INNER JOIN family_corpus AS fc ON f.import_id = fc.family_import_id
INNER JOIN corpus AS c ON fc.corpus_import_id = c.import_id
INNER JOIN organisation AS o ON c.organisation_id = o.id
INNER JOIN
    family_metadata_extracted AS fme
    ON f.import_id = fme.family_import_id
LEFT JOIN family_collections_agg AS fca ON f.import_id = fca.family_import_id
LEFT JOIN family_geo_agg AS fga ON f.import_id = fga.family_import_id
LEFT JOIN family_events_agg AS fea ON f.import_id = fea.family_import_id
LEFT JOIN doc_languages AS dl ON p.id = dl.id
LEFT JOIN
    recent_slugs AS rs_doc
    ON d.import_id = rs_doc.family_document_import_id
LEFT JOIN recent_slugs AS rs_fam ON f.import_id = rs_fam.family_import_id
WHERE d.last_modified < :ingest_cycle_start
  AND fc.corpus_import_id = ANY(:allowed_corpora_ids)
  AND d.document_status = 'PUBLISHED'
ORDER BY d.last_modified DESC, d.created DESC, d.ctid DESC, f.import_id ASC;
