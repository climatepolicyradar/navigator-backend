WITH recent_slugs AS (
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

eligible_families AS (
    SELECT DISTINCT f.import_id
    FROM family AS f
    INNER JOIN family_corpus AS fc ON f.import_id = fc.family_import_id
    INNER JOIN family_document AS fd ON f.import_id = fd.family_import_id
    WHERE fc.corpus_import_id = ANY(:allowed_corpora_ids)
      AND fd.document_status = 'PUBLISHED'
      AND fd.last_modified < :ingest_cycle_start
),

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

doc_languages AS (
    SELECT
        pd.id,
        STRING_AGG(l.name, ';' ORDER BY l.name) AS display_name
    FROM physical_document AS pd
    LEFT JOIN physical_document_language AS pdl ON pd.id = pdl.document_id
    LEFT JOIN language AS l ON pdl.language_id = l.id
    GROUP BY pd.id
),

family_metadata_extracted AS (
    SELECT
        fm.family_import_id,
        ARRAY_TO_STRING(
            ARRAY(SELECT JSONB_ARRAY_ELEMENTS_TEXT(
                    fm.value -> 'original_case_name'
                )),
            ';'
        ) AS original_case_name,
        ARRAY_TO_STRING(
            ARRAY(SELECT JSONB_ARRAY_ELEMENTS_TEXT(
                fm.value -> 'case_number'
                )),
            ';'
        ) AS case_number,
        ARRAY_TO_STRING(
            ARRAY(SELECT JSONB_ARRAY_ELEMENTS_TEXT(
                fm.value -> 'status'
                )),
            ';'
        ) AS status,
        ARRAY_TO_STRING(
            ARRAY(SELECT JSONB_ARRAY_ELEMENTS_TEXT(
                fm.value -> 'court_number'
                )),
            ';'
        ) AS court_number,
        ARRAY_TO_STRING(
            ARRAY(SELECT JSONB_ARRAY_ELEMENTS_TEXT(
                fm.value -> 'core_object'
                )),
            ';'
        ) AS core_object,
        ARRAY_TO_STRING(
            ARRAY(SELECT JSONB_ARRAY_ELEMENTS_TEXT(
                fm.value -> 'concept_preferred_label'
                )),
            ';'
        ) AS concept_preferred_label
    FROM family_metadata AS fm
    INNER JOIN eligible_families AS ef ON fm.family_import_id = ef.import_id
),

concept_labels AS (
    SELECT
        fm.family_import_id,
        JSONB_ARRAY_ELEMENTS_TEXT(
            fm.value -> 'concept_preferred_label'
        ) AS concept_label
    FROM family_metadata AS fm
    INNER JOIN eligible_families AS ef ON fm.family_import_id = ef.import_id
    WHERE fm.value ? 'concept_preferred_label'
),

concept_labels_parsed AS (
    SELECT
        fm.family_import_id,
        ARRAY_TO_STRING(
            ARRAY(SELECT
                CASE
                    WHEN concept_label LIKE 'jurisdiction/%'
                    THEN SPLIT_PART(concept_label, '/', 2)
                END
            ),
            ';'
        ) AS jurisdictions,
        ARRAY_TO_STRING(
            ARRAY(SELECT
                CASE
                    WHEN concept_label LIKE 'category/%'
                    THEN SPLIT_PART(concept_label, '/', 2)
                END
            ),
            ';'
        ) AS case_categories,
        ARRAY_TO_STRING(
            ARRAY(SELECT
                CASE
                    WHEN concept_label LIKE 'principal_law/%'
                    THEN SPLIT_PART(concept_label, '/', 2)
                END
            ),
            ';'
        ) AS principal_laws
    FROM family_metadata AS fm
    INNER JOIN eligible_families AS ef ON fm.family_import_id = ef.import_id
    CROSS JOIN concept_labels
    GROUP BY fm.family_import_id
)

SELECT
    rs_doc.name AS "Document ID",
    p.title AS "Document Title",
    rs_fam.name AS "Case ID",

    f.title AS "Case Name",
    fme.original_case_name AS "Non-English Case Name",
    f.description AS "Case Summary",
    EXTRACT(
        YEAR FROM fea.published_date
    )::TEXT AS "Case Filing Year for Action",
    fca.collection_import_ids AS "Bundle ID(s)",
    fca.collection_titles AS "Bundle Name(s)",

    fea.published_date::DATE AS "Document Filing Date",
    p.source_url AS "Document Content URL",
    -- Generate semicolon-separated URLs for each bundle ID
    d.variant_name AS "Document Variant",

    dl.display_name AS "Language(s)",
    -- Document Type comes from event type, not doc metadata
    fme.case_number AS "Case Number",
    -- Document Summary from event metadata (first event description)
    fme.status AS "Status",
    clp.jurisdictions AS "Jurisdictions",
    clp.case_categories AS "Case Categories",
    clp.principal_laws AS "Principal Laws",
    -- At Issue logic: collection description for USA, core_object for non-USA
    fme.court_number AS "Court Number",
    fga.geo_isos AS "Geography ISOs",
    fga.geo_display_values AS "Geographies",
    fea.published_date AS "First event in timeline",
    fea.last_changed AS "Last event in timeline",
    fea.event_type_names AS "Full timeline of events (types)",
    fea.event_dates AS "Full timeline of events (dates)",

    d.created::DATE AS "Date Added to System",
    f.last_modified::DATE AS "Last Modified on System",

    d.import_id AS "Internal Document ID",
    f.import_id AS "Internal Case ID",
    fc.corpus_import_id AS "Internal Corpus ID",
    fca.collection_import_ids AS "Internal Bundle ID(s)",
    CONCAT(
        'https://app.climatepolicyradar.org/documents/', rs_doc.name
    ) AS "Document URL",
    CONCAT(
        'https://app.climatepolicyradar.org/document/', rs_fam.name
    ) AS "Case URL",

    CASE
        WHEN fca.collection_import_ids IS NOT NULL THEN
            'https://app.climatepolicyradar.org/collection/' || REPLACE(
                fca.collection_import_ids, ';',
                ';https://app.climatepolicyradar.org/collection/'
            )
        ELSE ''
    END AS "Bundle URL(s)",
    CASE
        WHEN fea.event_import_ids IS NOT NULL THEN
            (SELECT fe.valid_metadata -> 'event_type' ->> 0
             FROM family_event AS fe
             WHERE fe.import_id = SPLIT_PART(fea.event_import_ids, ';', 1)
             LIMIT 1)
        ELSE ''
    END AS "Document Type",
    CASE
        WHEN fea.event_import_ids IS NOT NULL THEN
            (SELECT fe.valid_metadata -> 'description' ->> 0
             FROM family_event AS fe
             WHERE fe.import_id = SPLIT_PART(fea.event_import_ids, ';', 1)
             LIMIT 1)
        ELSE ''
    END AS "Document Summary",
    CASE
        WHEN
            fga.geo_isos LIKE '%USA%' AND fga.geo_isos NOT LIKE '%;%'
            THEN fca.collection_descriptions
        ELSE fme.core_object
    END AS "At Issue"
FROM family_document AS d
INNER JOIN physical_document AS p ON d.physical_document_id = p.id
INNER JOIN family AS f ON d.family_import_id = f.import_id
INNER JOIN family_corpus AS fc ON f.import_id = fc.family_import_id
INNER JOIN corpus AS c ON fc.corpus_import_id = c.import_id
INNER JOIN organisation AS o ON c.organisation_id = o.id
INNER JOIN
    family_metadata_extracted AS fme
    ON f.import_id = fme.family_import_id
LEFT JOIN concept_labels_parsed AS clp ON f.import_id = clp.family_import_id
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
