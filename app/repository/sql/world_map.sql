-- Filter to published families first
WITH published_families AS (
    SELECT f.import_id, f.family_category
    FROM family AS f
    WHERE EXISTS (
        SELECT 1
        FROM family_document AS fd
        WHERE fd.family_import_id = f.import_id
        AND fd.document_status = 'PUBLISHED'
    )
),

geography_counts AS (
    SELECT
        g.id AS geography_id,
        g.display_value,
        g.slug,
        g.value,
        pf.family_category,
        COUNT(*) AS records_count
    FROM geography AS g
    INNER JOIN family_geography AS fg ON g.id = fg.geography_id
    INNER JOIN published_families AS pf ON fg.family_import_id = pf.import_id
    INNER JOIN family_corpus AS fc ON pf.import_id = fc.family_import_id
    WHERE fc.corpus_import_id = ANY(:allowed_corpora_ids)
    GROUP BY g.id, g.display_value, g.slug, g.value, pf.family_category
),

category_matrix AS (
    SELECT DISTINCT
        g.id AS geography_id,
        g.display_value,
        g.slug,
        g.value,
        f.family_category
    FROM geography AS g
    CROSS JOIN f
),

f AS (SELECT DISTINCT family_category FROM family)

SELECT
    cm.display_value,
    cm.slug,
    cm.value,
    JSONB_OBJECT_AGG(
        cm.family_category,
        COALESCE(gc.records_count, 0)
    ) AS counts
FROM category_matrix AS cm
LEFT JOIN geography_counts AS gc ON
    cm.geography_id = gc.geography_id
    AND cm.family_category = gc.family_category
GROUP BY cm.display_value, cm.slug, cm.value
ORDER BY cm.display_value;
