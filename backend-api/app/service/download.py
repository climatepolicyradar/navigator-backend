"""Functions to support browsing the RDS document structure"""

import zipfile
from io import BytesIO, StringIO
from logging import getLogger
from typing import Optional

import pandas as pd
from app.clients.db.session import get_db
from app.repository.download import get_whole_database_dump
from fastapi import Depends

_LOGGER = getLogger(__name__)


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


def generate_data_dump_as_csv(
    ingest_cycle_start: str, allowed_corpora_ids: list[str], db=Depends(get_db)
):
    df = get_whole_database_dump(ingest_cycle_start, allowed_corpora_ids, db)
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


def create_data_download_zip_archive(
    ingest_cycle_start: str, allowed_corpora_ids: list[str], db=Depends(get_db)
):
    readme_buffer = generate_data_dump_readme(ingest_cycle_start)

    csv_buffer = generate_data_dump_as_csv(ingest_cycle_start, allowed_corpora_ids, db)

    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
        for file_name, data in [
            ("README.txt", readme_buffer),
            (f"Document_Data_Download-{ingest_cycle_start}.csv", csv_buffer),
        ]:
            zip_file.writestr(file_name, data.getvalue())

    return zip_buffer
