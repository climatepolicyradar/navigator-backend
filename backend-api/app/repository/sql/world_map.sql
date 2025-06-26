WITH counts AS (
    SELECT
      family.family_category,
      family_geography.geography_id,
      COUNT(*) AS records_count
    FROM
      family
      INNER JOIN
          family_corpus
          ON family.import_id = family_corpus.family_import_id
      INNER JOIN corpus ON family_corpus.corpus_import_id = corpus.import_id
      INNER JOIN
          family_geography
          ON family.import_id = family_geography.family_import_id
    WHERE
      family_corpus.corpus_import_id = ANY(:allowed_corpora_ids)
      AND CASE
        WHEN (
          NOT (
            EXISTS (
              SELECT
                1
              FROM
                family_document
              WHERE
                family.import_id = family_document.family_import_id
            )
          )
        ) THEN 'Created'
        WHEN (
            (
              SELECT
                COUNT(family_document.document_status) AS count_1
              FROM
                family_document
              WHERE
                family_document.family_import_id = family.import_id
                AND family_document.document_status = 'PUBLISHED'
            ) > 0
          ) THEN 'Published'
        WHEN (
              (
                SELECT
                  COUNT(family_document.document_status) AS count_2
                FROM
                  family_document
                WHERE
                  family_document.family_import_id = family.import_id
                  AND family_document.document_status = 'CREATED'
              ) > 0
            ) THEN 'Created'
        ELSE 'Deleted'
      END = 'Published'
    GROUP BY
      family.family_category,
      family_geography.geography_id
  )

SELECT
  geo_family_combinations.display_value,
  geo_family_combinations.slug,
  geo_family_combinations.value,
  JSONB_OBJECT_AGG(
    geo_family_combinations.family_category,
    COALESCE(counts.records_count, 0)
  ) AS counts
FROM
  (
    SELECT
      geography.id AS geography_id,
      geography.display_value,
      geography.slug,
      geography.value,
      anon_1.family_category
    FROM
      geography,
      (
        SELECT DISTINCT
          family.family_category
        FROM
          family
      ) AS anon_1
  ) AS geo_family_combinations
  LEFT OUTER JOIN
      counts
      ON geo_family_combinations.geography_id = counts.geography_id
  AND geo_family_combinations.family_category = counts.family_category
GROUP BY
  geo_family_combinations.display_value,
  geo_family_combinations.slug,
  geo_family_combinations.value
ORDER BY
  geo_family_combinations.display_value;
