SELECT DISTINCT
    slug.collection_import_id
FROM slug
    INNER JOIN collection_family
        ON slug.collection_import_id = collection_family.collection_import_id
    INNER JOIN family
        ON collection_family.family_import_id = family.import_id
    INNER JOIN family_corpus
        ON family.import_id = family_corpus.family_import_id
    INNER JOIN corpus
        ON family_corpus.corpus_import_id = corpus.import_id
WHERE
    slug.name = :slug_name
    AND corpus.import_id = ANY(:allowed_corpora_ids)
