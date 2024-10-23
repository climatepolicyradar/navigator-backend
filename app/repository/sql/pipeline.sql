WITH deduplicated_family_slugs AS (   SELECT
        DISTINCT
            ON (slug.family_import_id)   slug.family_import_id,
            slug.created,
            slug.name
    FROM
        (     SELECT
            slug.family_import_id AS "family_import_id",
            Count(*) AS count
        FROM
            slug
        WHERE
            slug.family_import_id IS NOT NULL
        GROUP BY
            slug.family_import_id
        HAVING
            Count(*) > 1   ) duplicates
    left join
        slug
            ON duplicates.family_import_id = slug.family_import_id
    ORDER BY
        slug.family_import_id DESC,
        slug.created DESC,
        slug.ctid DESC ),
        unique_family_slugs AS (   SELECT
            DISTINCT
                ON (slug.family_import_id)   slug.family_import_id,
                slug.created,
                slug.name
        FROM
            (     SELECT
                slug.family_import_id AS "family_import_id",
                Count(*) AS count
            FROM
                slug
            WHERE
                slug.family_import_id IS NOT NULL
            GROUP BY
                slug.family_import_id
            HAVING
                Count(*) = 1   ) non_duplicates
        left join
            slug
                ON non_duplicates.family_import_id = slug.family_import_id
        ORDER BY
            slug.family_import_id DESC,
            slug.created DESC,
            slug.ctid DESC   ),
            most_recent_family_slugs AS (   SELECT
                deduplicated_family_slugs.family_import_id AS "family_import_id",
                deduplicated_family_slugs.created AS "created",
                deduplicated_family_slugs.name AS "name"
            FROM
                deduplicated_family_slugs
            UNION
            ALL   SELECT
                unique_family_slugs.family_import_id AS "family_import_id",
                unique_family_slugs.created AS "created",
                unique_family_slugs.name AS "name"
            FROM
                unique_family_slugs
            ORDER BY
                family_import_id DESC,
                created DESC   ), deduplicated_doc_slugs AS (   SELECT
                DISTINCT
                    ON (slug.family_document_import_id)   slug.family_document_import_id,
                    slug.created,
                    slug.name
            FROM
                (     SELECT
                    slug.family_document_import_id AS "family_document_import_id",
                    Count(*) AS count
                FROM
                    slug
                WHERE
                    slug.family_document_import_id IS NOT NULL
                GROUP BY
                    slug.family_document_import_id
                HAVING
                    Count(*) >  1   ) duplicates
            left join
                slug
                    ON duplicates.family_document_import_id = slug.family_document_import_id
            ORDER BY
                slug.family_document_import_id DESC,
                slug.created DESC,
                slug.ctid DESC ),
                unique_doc_slugs AS (   SELECT
                    DISTINCT
                        ON (slug.family_document_import_id)   slug.family_document_import_id,
                        slug.created,
                        slug.name
                FROM
                    (     SELECT
                        slug.family_document_import_id AS "family_document_import_id",
                        Count(*) AS count
                    FROM
                        slug
                    WHERE
                        slug.family_document_import_id IS NOT NULL
                    GROUP BY
                        slug.family_document_import_id
                    HAVING
                        Count(*) = 1   ) non_duplicates
                left join
                    slug
                        ON non_duplicates.family_document_import_id = slug.family_document_import_id
                ORDER BY
                    slug.family_document_import_id DESC,
                    slug.created DESC,
                    slug.ctid DESC   ),
                    most_recent_doc_slugs AS (
                        SELECT
                            deduplicated_doc_slugs.family_document_import_id   AS "family_document_import_id",
                            deduplicated_doc_slugs.created,
                            deduplicated_doc_slugs.name
                        FROM
                            deduplicated_doc_slugs
                        UNION
                        ALL   SELECT
                            unique_doc_slugs.family_document_import_id AS "family_document_import_id",
                            unique_doc_slugs.created,
                            unique_doc_slugs.name
                        FROM
                            unique_doc_slugs
                        ORDER BY
                            family_document_import_id DESC,
                            created DESC
                    ), event_dates AS (
                        SELECT
                            family_event.family_import_id AS family_import_id,
                            CASE
                                WHEN COUNT(*) FILTER (WHERE family_event.event_type_name = 'Passed/Approved') > 0 THEN
                                    MIN(CASE
                                        WHEN family_event.event_type_name = 'Passed/Approved' THEN family_event.date::TIMESTAMPTZ
                                    END)
                                ELSE
                                    MIN(family_event.date::TIMESTAMPTZ)
                            END AS published_date
                        FROM
                            family_event
                        GROUP BY
                            family_import_id
                    )  SELECT
                        f.title AS "family_title",
                        p.title AS "physical_document_title",
                        f.description AS "family_description",
                        CASE
                            WHEN f.family_category IN ('UNFCCC',
                            'MCF') THEN Upper(f.family_category::text)
                            ELSE Initcap(f.family_category::text)
                        END "family_category",
                        fp.published_date AS "family_published_date",
                        d.import_id AS "family_document_import_id",
                        ds.name AS "family_document_slug",
                        f.import_id AS "family_import_id",
                        fs.name AS "family_slug",
                        p.source_url AS "physical_document_source_url",
                        d.valid_metadata::json#>>'{type,0}' AS "family_document_type",
                        o.name AS "organisation_name",
                        geos.geographies AS "geographies",
                        c.import_id AS "corpus_import_id",
                        c.corpus_type_name AS "corpus_type_name",
                        langs.languages AS "languages",
                        fm.value AS "family_metadata",
                        d.valid_metadata AS "family_document_metadata"
                    FROM
                        physical_document p
                    join
                        family_document d
                            ON p.id = d.physical_document_id
                    join
                        family f
                            ON d.family_import_id = f.import_id full
                    join
                        (
                            SELECT
                                family_geography.family_import_id AS "family_import_id",
                                string_agg(geography.value,
                                ';') AS geo_isos,
                                string_agg(geography.display_value,
                                ';') AS geo_display_values
                            FROM
                                geography
                            inner join
                                family_geography
                                    ON geography.id = family_geography.geography_id
                            GROUP BY
                                family_geography.family_import_id
                        ) fg
                            ON fg.family_import_id=f.import_id
                    join
                        family_corpus fc
                            ON f.import_id = fc.family_import_id
                    join
                        corpus c
                            ON fc.corpus_import_id = c.import_id
                    join
                        organisation o
                            ON c.organisation_id = o.id
                    join
                        family_metadata fm
                            ON fm.family_import_id = f.import_id
                    left outer join
                        (
                            SELECT
                                family_document.import_id AS family_document_import_id,
                                json_agg(DISTINCT(LANGUAGE.name)) AS languages
                            FROM
                                family_document
                            join
                                physical_document_language
                                    ON physical_document_language.document_id = family_document.physical_document_id
                            join
                                LANGUAGE
                                    ON LANGUAGE.id = physical_document_language.language_id
                            GROUP BY
                                family_document.import_id
                        ) AS langs
                            ON langs.family_document_import_id = d.import_id
                    left outer join
                        (
                            SELECT
                                family_geography.family_import_id AS family_import_id,
                                json_agg(DISTINCT(geography.value)) AS geographies
                            FROM
                                family_geography
                            join
                                geography
                                    ON geography.id = family_geography.geography_id
                            GROUP BY
                                family_geography.family_import_id
                        ) AS geos
                            ON geos.family_import_id = f.import_id
                    left join
                        most_recent_doc_slugs ds
                            ON ds.family_document_import_id = d.import_id
                    left join
                        most_recent_family_slugs fs
                            ON fs.family_import_id = f.import_id
                    left join
                        event_dates fp
                            ON fp.family_import_id = f.import_id
                    WHERE
                        d.document_status != 'DELETED'
                        AND fg.family_import_id = f.import_id
                    ORDER BY
                        d.last_modified DESC,
                        d.created DESC,
                        d.ctid DESC,
                        f.import_id
