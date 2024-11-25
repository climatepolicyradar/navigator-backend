SELECT
  geo_family_combinations.display_value AS display_value,
  geo_family_combinations.slug AS slug,
  geo_family_combinations.value AS value,
  jsonb_object_agg (
    geo_family_combinations.family_category,
    coalesce(counts.records_count, 0)
  ) AS counts
FROM
  (
    SELECT
      geography.id AS geography_id,
      geography.display_value AS display_value,
      geography.slug AS slug,
      geography.value AS value,
      anon_1.family_category AS family_category
    FROM
      geography,
      (
        SELECT DISTINCT
          family.family_category AS family_category
        FROM
          family
      ) AS anon_1
  ) AS geo_family_combinations
  LEFT OUTER JOIN (
    SELECT
      family.family_category AS family_category,
      family_geography.geography_id AS geography_id,
      count(*) AS records_count
    FROM
      family
      JOIN family_geography ON family.import_id = family_geography.family_import_id
    WHERE
      CASE
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
        ELSE CASE
          WHEN (
            (
              SELECT
                count(family_document.document_status) AS count_1
              FROM
                family_document
              WHERE
                family_document.family_import_id = family.import_id
                AND family_document.document_status = 'PUBLISHED'
            ) > 0
          ) THEN 'Published'
          ELSE CASE
            WHEN (
              (
                SELECT
                  count(family_document.document_status) AS count_2
                FROM
                  family_document
                WHERE
                  family_document.family_import_id = family.import_id
                  AND family_document.document_status = 'CREATED'
              ) > 0
            ) THEN 'Created'
            ELSE 'Deleted'
          END
        END
      END = 'Published'
    GROUP BY
      family.family_category,
      family_geography.geography_id
  ) AS counts ON geo_family_combinations.geography_id = counts.geography_id
  AND geo_family_combinations.family_category = counts.family_category
GROUP BY
  geo_family_combinations.display_value,
  geo_family_combinations.slug,
  geo_family_combinations.value
ORDER BY
  geo_family_combinations.display_value;
