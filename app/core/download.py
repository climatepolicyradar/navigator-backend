"""Functions to support browsing the RDS document structure"""

import zipfile
from io import BytesIO, StringIO
from logging import getLogger
from typing import Optional

import pandas as pd
from fastapi import Depends

from app.db.session import get_db

_LOGGER = getLogger(__name__)


def create_query(ingest_cycle_start: str) -> str:
    """Browse RDS"""

    query = (
        "WITH "
        "deduplicated_family_slugs as ("
        "select "
        "distinct ON (slug.family_import_id) "
        "slug.family_import_id, slug.created, slug.name "
        "from ("
        "SELECT "
        'slug.family_import_id as "family_import_id", '
        "count(*) as count "
        "from slug "
        "where slug.family_import_id is not null "
        "group by slug.family_import_id "
        "having count(*) > 1"
        ") duplicates "
        "left join slug "
        "on duplicates.family_import_id = slug.family_import_id "
        "order by slug.family_import_id desc, slug.created desc, slug.ctid desc "
        "), "
        "unique_family_slugs as ("
        "select "
        "distinct ON (slug.family_import_id) "
        "slug.family_import_id, slug.created, slug.name "
        "from ("
        "SELECT "
        'slug.family_import_id as "family_import_id", '
        "count(*) as count "
        "from slug "
        "where slug.family_import_id is not null "
        "group by slug.family_import_id "
        "having count(*) = 1"
        ") non_duplicates "
        "left join slug "
        "on non_duplicates.family_import_id = slug.family_import_id "
        "order by slug.family_import_id desc, slug.created desc, slug.ctid desc "
        "), most_recent_family_slugs as ("
        "select "
        'deduplicated_family_slugs.family_import_id as "family_import_id", '
        'deduplicated_family_slugs.created as "created", '
        'deduplicated_family_slugs.name as "name" '
        "from deduplicated_family_slugs "
        "UNION ALL "
        "select "
        'unique_family_slugs.family_import_id as "family_import_id", '
        'unique_family_slugs.created as "created", '
        'unique_family_slugs.name as "name" '
        "from unique_family_slugs "
        "order by family_import_id desc, created desc "
        "), deduplicated_doc_slugs as ("
        "select "
        "distinct ON (slug.family_document_import_id) "
        "slug.family_document_import_id, "
        "slug.created, "
        "slug.name "
        "from ("
        "SELECT "
        'slug.family_document_import_id as "family_document_import_id", '
        "count(*) as count "
        "from slug "
        "where slug.family_document_import_id is not null "
        "group by slug.family_document_import_id "
        "having count(*) >  1"
        ") duplicates "
        "left join slug "
        "on duplicates.family_document_import_id = slug.family_document_import_id "
        "order by "
        "slug.family_document_import_id desc, slug.created desc, slug.ctid desc"
        "), "
        "unique_doc_slugs as ("
        "select "
        "distinct ON (slug.family_document_import_id) "
        "slug.family_document_import_id, "
        "slug.created, "
        "slug.name "
        "from ("
        "SELECT "
        'slug.family_document_import_id as "family_document_import_id", '
        "count(*) as count "
        "from slug "
        "where slug.family_document_import_id is not null "
        "group by slug.family_document_import_id "
        "having count(*) = 1"
        ") non_duplicates "
        "left join slug "
        "on non_duplicates.family_document_import_id = slug.family_document_import_id "
        "order by  "
        "slug.family_document_import_id desc, slug.created desc, slug.ctid desc"
        "), most_recent_doc_slugs as ("
        "select "
        "deduplicated_doc_slugs.family_document_import_id "
        'as "family_document_import_id", '
        "deduplicated_doc_slugs.created, "
        "deduplicated_doc_slugs.name "
        "from deduplicated_doc_slugs "
        "UNION ALL "
        "select "
        'unique_doc_slugs.family_document_import_id as "family_document_import_id", '
        "unique_doc_slugs.created, "
        "unique_doc_slugs.name "
        "from unique_doc_slugs "
        "order by family_document_import_id desc, created desc"
        "), event_dates as ("
        "select "
        "family_event.family_import_id as family_import_id, "
        "min(case "
        "when family_event.event_type_name='Passed/Approved' then "
        "family_event.date::date "
        "else family_event.date::date "
        "end) published_date, "
        "max(family_event.date::date) last_changed "
        "from family_event "
        "group by family_import_id "
        ") "
        "SELECT "
        'ds.name as "Document ID", '
        'p.title as "Document Title", '
        'fs.name as "Family ID", '
        'f.title as "Family Title", '
        'f.description as "Family Summary", '
        'n1.collection_titles as "Collection Title(s)", '
        'n1.collection_descriptions as "Collection Description(s)", '
        'INITCAP(d.document_role::TEXT) as "Document Role", '
        'd.variant_name as "Document Variant", '
        'p.source_url as "Document Content URL", '
        'd.document_type as "Document Type", '
        "CASE "
        "WHEN f.family_category = 'UNFCCC' THEN 'UNFCCC' "
        "ELSE INITCAP(f.family_category::TEXT) "
        'END "Category", '
        "array_to_string(ARRAY("
        "SELECT jsonb_array_elements_text(fm.value->'framework')), ';') "
        'as "Framework", '
        'n2.language as "Language", '
        'o.name as "Source", '
        'g.value as "Geography ISO", '
        'g.display_value as "Geography", '
        "array_to_string(ARRAY("
        "SELECT jsonb_array_elements_text(fm.value->'topic')), ';') "
        'as "Topic/Response", '
        "array_to_string(ARRAY("
        "SELECT jsonb_array_elements_text(fm.value->'hazard')), ';') "
        'as "Hazard", '
        "array_to_string(ARRAY("
        "SELECT jsonb_array_elements_text(fm.value->'sector')), ';') "
        'as "Sector", '
        "array_to_string(ARRAY("
        "SELECT jsonb_array_elements_text(fm.value->'keyword')), ';') "
        'as "Keyword", '
        "array_to_string(ARRAY("
        "SELECT jsonb_array_elements_text(fm.value->'instrument')), ';') "
        'as "Instrument", '
        "array_to_string(ARRAY("
        "SELECT jsonb_array_elements_text(fm.value->'author')), ';') "
        'as "Author", '
        "array_to_string(ARRAY("
        "SELECT jsonb_array_elements_text(fm.value->'author_type')), ';') "
        'as "Author Type", '
        'fp.published_date as "First event in timeline", '
        'fp.last_changed as "Last event in timeline", '
        'n3.event_type_names as "Full timeline of events (types)", '
        'n3.event_dates as "Full timeline of events (dates)", '
        'd.created::date as "Date Added to System", '
        'f.last_modified::date as "Last Modified on System", '
        'd.import_id as "Internal Document ID", '
        'f.import_id as "Internal Family ID", '
        'n1.collection_import_ids as "Internal Collection ID(s)" '
        "FROM physical_document p "
        "JOIN family_document d "
        "    ON p.id = d.physical_document_id "
        "JOIN family f "
        "    ON d.family_import_id = f.import_id "
        "inner join geography g "
        "    on g.id = f.geography_id "
        "join family_corpus fc "
        "    on f.import_id = fc.family_import_id "
        "join corpus c "
        "    on fc.corpus_import_id = c.import_id "
        "join organisation o "
        "    on c.organisation_id = o.id "
        "join family_metadata fm "
        "    on fm.family_import_id = f.import_id "
        "FULL JOIN ("
        "    SELECT "
        '        collection_family.family_import_id as "family_import_id", '
        "string_agg(collection.import_id, ';') AS collection_import_ids, "
        "string_agg(collection.title, ';') AS collection_titles, "
        "string_agg(collection.description, ';') AS collection_descriptions "
        "FROM "
        "    collection "
        "INNER JOIN collection_family "
        "ON collection_family.collection_import_id = collection.import_id "
        "GROUP BY collection_family.family_import_id "
        ") n1 ON n1.family_import_id=f.import_id "
        "left JOIN ("
        "    SELECT "
        '        p.id as "id", '
        "string_agg(l.name, ';' ORDER BY l.name) AS language "
        "FROM physical_document p "
        "    left join physical_document_language pdl "
        "        on pdl.document_id = p.id "
        "    left join language l "
        "        on l.id = pdl.language_id "
        "    GROUP  BY p.id "
        ") n2 ON n2.id=d.physical_document_id "
        "FULL JOIN ("
        "    SELECT "
        "        family_event.family_import_id, "
        "string_agg(family_event.import_id, ';') AS event_import_ids, "
        "string_agg(family_event.title, ';') AS event_titles, "
        "string_agg(family_event.event_type_name, ';') AS event_type_names, "
        "string_agg(family_event.date::date::text, ';') AS event_dates "
        "FROM family_event "
        "    INNER JOIN  family ON family.import_id = family_event.family_import_id "
        "    GROUP BY family_event.family_import_id "
        ") n3 ON n3.family_import_id=f.import_id "
        "LEFT JOIN most_recent_doc_slugs ds "
        "on ds.family_document_import_id = d.import_id "
        "LEFT JOIN most_recent_family_slugs fs on fs.family_import_id = f.import_id "
        "LEFT JOIN event_dates fp on fp.family_import_id = f.import_id "
        "WHERE d.last_modified < '{}' "
        "ORDER BY "
        "d.last_modified desc, d.created desc, d.ctid desc, n1.family_import_id"
    ).format(ingest_cycle_start)
    return query


def get_whole_database_dump(ingest_cycle_start: str, db=Depends(get_db)):
    query = create_query(ingest_cycle_start)
    with db.connection() as conn:
        df = pd.read_sql_query(query, conn)
        return df


def replace_slug_with_qualified_url(
    df: pd.DataFrame,
    public_app_url: str,
    url_cols: Optional[list[str]] = None,
) -> pd.DataFrame:
    """
    Use the slug to create a fully qualified URL to the entity.

    This functionality won't be included in the MVP for the data dump,
    but will likely be included in future revisions.
    """
    if url_cols is None:
        url_cols = ["Family Slug", "Document Slug"]

    url_base = f"{public_app_url}/documents/"

    for col in url_cols:
        df[col] = url_base + df[col].astype(str)

    df.columns = df.columns.str.replace("Slug", "URL")
    return df


def convert_dump_to_csv(df: pd.DataFrame):
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, sep=",", index=False, encoding="utf-8")
    return csv_buffer


def generate_data_dump_as_csv(ingest_cycle_start: str, db=Depends(get_db)):
    df = get_whole_database_dump(ingest_cycle_start, db)
    csv = convert_dump_to_csv(df)
    csv.seek(0)
    return csv


def generate_data_dump_readme(ingest_cycle_start: str):
    file_buffer = StringIO(
        "Thank you for downloading the full document dataset from Climate Policy Radar "
        "and Climate Change Laws of the World!"
        "\n\n"
        "For more information including our data dictionary, methodology and "
        "information about how to cite us, visit "
        "\n"
        "https://climatepolicyradar.notion.site/Readme-for-document-data-download-f2d55b7e238941b59559b9b1c4cc52c5"
        "\n\n"
        "View our terms of use at https://app.climatepolicyradar.org/terms-of-use"
        "\n\n"
        f"Date data last updated: {ingest_cycle_start}"
    )
    file_buffer.seek(0)
    return file_buffer


def create_data_download_zip_archive(ingest_cycle_start: str, db=Depends(get_db)):
    readme_buffer = generate_data_dump_readme(ingest_cycle_start)
    csv_buffer = generate_data_dump_as_csv(ingest_cycle_start, db)

    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
        for file_name, data in [
            ("README.txt", readme_buffer),
            (f"Document_Data_Download-{ingest_cycle_start}.csv", csv_buffer),
        ]:
            zip_file.writestr(file_name, data.getvalue())

    return zip_buffer
