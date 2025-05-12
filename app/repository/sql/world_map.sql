-- noqa: disable=all
-- Filter to published families first
WITH published_families AS (
    SELECT f.import_id, f.family_category 
    FROM family f
    WHERE EXISTS (
        SELECT 1 
        FROM family_document fd 
        WHERE fd.family_import_id = f.import_id 
        AND fd.document_status = 'PUBLISHED'
    )
),
-- And count how many families are in each geography, filtered by allowed corpuses
geography_counts AS (
    SELECT 
        g.id AS geography_id,
        g.display_value,
        g.slug,
        g.value,
        pf.family_category,
        COUNT(*) AS records_count
    FROM geography g
    JOIN family_geography fg ON g.id = fg.geography_id
    JOIN published_families pf ON fg.family_import_id = pf.import_id
    JOIN family_corpus fc ON pf.import_id = fc.family_import_id
    WHERE fc.corpus_import_id =  ANY(:allowed_corpora_ids)
    GROUP BY g.id, g.display_value, g.slug, g.value, pf.family_category
),
-- This builds a grid where each row is a unique geography/family_category combination
-- This ensures even when a geography has no families in a category, it will still be included
category_matrix AS (
    SELECT DISTINCT
        g.id AS geography_id,
        g.display_value,
        g.slug,
        g.value,
        f.family_category
    FROM geography g
    CROSS JOIN (SELECT DISTINCT family_category FROM family) f
)
--Now, we have filtered families that are published;
-- from that we build a count of how many families are in each geography/family_category combination
-- and we have a grid of all possible geography/family_category combinations for fast lookup
-- SO if we left join the geog counts with the matrix, 
-- we get an output that matches the old world map query without all the extra cruft
SELECT 
    cm.display_value,
    cm.slug,
    cm.value,
    JSONB_OBJECT_AGG(
        cm.family_category,
        COALESCE(gc.records_count, 0)
    ) AS counts
FROM category_matrix cm
LEFT JOIN geography_counts gc ON 
    cm.geography_id = gc.geography_id AND 
    cm.family_category = gc.family_category
GROUP BY cm.display_value, cm.slug, cm.value
ORDER BY cm.display_value;
