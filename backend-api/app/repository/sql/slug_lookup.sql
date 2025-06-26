-- First query for family document slugs
SELECT DISTINCT
    slug.family_document_import_id,
    slug.family_import_id
FROM slug
    INNER JOIN family_document
        ON slug.family_document_import_id = family_document.import_id
    INNER JOIN family
        ON family_document.family_import_id = family.import_id
    INNER JOIN family_corpus
        ON family.import_id = family_corpus.family_import_id
    INNER JOIN corpus
        ON family_corpus.corpus_import_id = corpus.import_id
WHERE
    slug.name = :slug_name
    AND corpus.import_id = ANY(:allowed_corpora_ids)

UNION

-- Second query for family slugs
SELECT DISTINCT
    NULL AS family_document_import_id,
    slug.family_import_id
FROM slug
    INNER JOIN family
        ON slug.family_import_id = family.import_id
    INNER JOIN family_corpus
        ON family.import_id = family_corpus.family_import_id
    INNER JOIN corpus
        ON family_corpus.corpus_import_id = corpus.import_id
WHERE
    slug.name = :slug_name
    AND corpus.import_id = ANY(:allowed_corpora_ids)
