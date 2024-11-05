SELECT
  slug.family_document_import_id,
  slug.family_import_id
FROM
  slug
  LEFT JOIN family ON family.import_id = slug.family_import_id
  LEFT JOIN family_corpus ON family_corpus.family_import_id = family.import_id
  LEFT JOIN corpus ON corpus.import_id = family_corpus.corpus_import_id
WHERE
  slug.name = '{slug_name}'
  AND corpus.import_id IN ({allowed_corpora_ids})
UNION
SELECT
  slug.family_document_import_id,
  slug.family_import_id
FROM
  slug
  LEFT JOIN family_document ON family_document.import_id = slug.family_document_import_id
  LEFT JOIN family ON family.import_id = family_document.family_import_id
  LEFT JOIN family_corpus ON family_corpus.family_import_id = family.import_id
  LEFT JOIN corpus ON corpus.import_id = family_corpus.corpus_import_id
WHERE
  slug.name = '{slug_name}'
  AND corpus.import_id IN ({allowed_corpora_ids});
