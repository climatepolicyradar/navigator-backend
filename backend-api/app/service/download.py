"""Functions to support browsing the RDS document structure"""

import csv
import zipfile
from collections import defaultdict
from io import BytesIO, StringIO
from logging import getLogger
from typing import Any, Mapping, Optional, Sequence, cast

import pandas as pd
from db_client.models.dfce import (
    Collection,
    CollectionFamily,
    DocumentStatus,
    Family,
    FamilyDocument,
    FamilyEvent,
    FamilyMetadata,
    Slug,
)
from db_client.models.dfce.family import Corpus, FamilyCorpus
from db_client.models.organisation import Organisation
from fastapi import Depends
from sqlalchemy.orm import Session

from app.clients.db.session import get_db
from app.errors import ValidationError
from app.models.search import SearchResponse, SearchResponseFamily
from app.repository.download import get_whole_database_dump
from app.repository.lookups import (
    doc_type_from_family_document_metadata,  # TODO: update this to use geographies api endpoint when refactoring geographies to use iso codes
)
from app.service.util import to_cdn_url
from app.telemetry import observe

_LOGGER = getLogger(__name__)

_CSV_SEARCH_RESPONSE_COLUMNS = [
    "Collection Name",
    "Collection Summary",
    "Family Name",
    "Family Summary",
    "Family URL",
    "Family Publication Date",
    "Geographies",
    "Document Title",
    "Document URL",
    "Document Content URL",
    "Document Type",
    "Document Content Matches Search Phrase",
    "Category",
    "Languages",
    "Source",
]

_CCC_CSV_SEARCH_RESPONSE_COLUMNS = [
    "Bundle ID",
    "Bundle Name",
    "Bundle URL",
    "Case ID",
    "Case Name",
    "Non-English Case Name",
    "Case URL",
    "At Issue",
    "Case Summary",
    "Case Number",
    "Case Filing Year for Action",
    "Status",
    "Jurisdictions",
    "Case Categories",
    "Principal Laws",
    "Court Number",
    "Document Title",
    "Document URL",
    "Document Content URL",
    "Document Type",
    "Document Filing Date",
    "Document Summary",
    "Geographies",
    "Document Content Matches Search Phrase",
]


def _get_extra_csv_info(
    db: Session,
    families: Sequence[SearchResponseFamily],
) -> Mapping[str, Any]:
    all_family_slugs = [f.family_slug for f in families]

    slug_and_family_metadata = (
        db.query(Slug, FamilyMetadata)
        .filter(Slug.name.in_(all_family_slugs))
        .join(FamilyMetadata, FamilyMetadata.family_import_id == Slug.family_import_id)
        .all()
    )
    slug_and_organisation = (
        db.query(Slug, Organisation)
        .filter(Slug.name.in_(all_family_slugs))
        .join(FamilyCorpus, FamilyCorpus.family_import_id == Slug.family_import_id)
        .join(Corpus, Corpus.import_id == FamilyCorpus.corpus_import_id)
        .join(Organisation, Organisation.id == Corpus.organisation_id)
    )
    slug_and_family_document = (
        db.query(Slug, FamilyDocument)
        .filter(Slug.name.in_(all_family_slugs))
        .join(Family, Family.import_id == Slug.family_import_id)
        .join(FamilyDocument, Family.import_id == FamilyDocument.family_import_id)
        .filter(FamilyDocument.document_status == DocumentStatus.PUBLISHED)
        .all()
    )
    # For now there is max one collection per family
    slug_and_collection = (
        db.query(Slug, Collection)
        .filter(Slug.name.in_(all_family_slugs))
        .join(
            CollectionFamily, CollectionFamily.family_import_id == Slug.family_import_id
        )
        .join(Collection, Collection.import_id == CollectionFamily.collection_import_id)
        .all()
    )
    family_slug_to_documents = defaultdict(list)
    all_document_import_ids = []
    for slug, document in slug_and_family_document:
        family_slug_to_documents[slug.name].append(document)
        all_document_import_ids.append(document.import_id)

    # Fetch document events
    document_events = _get_document_events(db, all_document_import_ids)

    extra_csv_info = {
        "metadata": {
            slug.name: meta.value for (slug, meta) in slug_and_family_metadata
        },
        "source": {slug.name: org.name for (slug, org) in slug_and_organisation},
        "documents": family_slug_to_documents,
        "collection": {
            slug.name: collection for (slug, collection) in slug_and_collection
        },
        "document_events": document_events,
    }

    return extra_csv_info


def parse_concept_labels(concept_labels: Sequence[str], prefix: str) -> str:
    """Extracts and joins concept labels with a given prefix."""
    return ";".join(
        label.split("/", 1)[1]
        for label in concept_labels
        if label.startswith(prefix + "/")
    )


def _get_document_events(
    db: Session, document_import_ids: Sequence[str]
) -> Mapping[str, list[FamilyEvent]]:
    """
    Fetch events associated with documents (not family events).

    :param Session db: database session
    :param Sequence[str] document_import_ids: list of document import IDs
    :return Mapping[str, list[FamilyEvent]]: mapping of document import ID to list of events
    """
    if not document_import_ids:
        return {}

    # Query for events that are associated with documents (not families)
    document_events = (
        db.query(FamilyEvent)
        .filter(
            FamilyEvent.family_document_import_id.in_(document_import_ids),
            FamilyEvent.family_document_import_id.isnot(None),
        )
        .order_by(FamilyEvent.date.asc())  # Order by date ascending for earliest first
        .all()
    )

    # Group events by document import ID
    document_events_map = defaultdict(list)
    for event in document_events:
        if event.family_document_import_id:
            document_events_map[event.family_document_import_id].append(event)

    return dict(document_events_map)


@observe("process_result_into_csv")
def process_result_into_csv(
    db: Session,
    search_response: SearchResponse,
    base_url: Optional[str],
    is_browse: bool,
    theme: Optional[str] = None,
) -> str:
    """
    Process a search/browse result into a CSV file for download.

    :param Session db: database session for supplementary queries
    :param SearchResponse search_response: the search result to process
    :param bool is_browse: a flag indicating whether this is a search/browse result
    :param Optional[str] theme: the theme to determine CSV column format
    :return str: the search result represented as CSV
    """
    # Check if theme is CCC (case insensitive)
    is_ccc_theme = theme and theme.upper() == "CCC"

    extra_required_info = _get_extra_csv_info(db, search_response.families)
    all_matching_document_slugs = {
        d.document_slug
        for f in search_response.families
        for d in f.family_documents
        if d.document_passage_matches
    }

    if base_url is None:
        raise ValidationError("Error creating CSV")

    scheme = "http" if "localhost" in base_url else "https"
    url_base = f"{scheme}://{base_url}"
    rows = []

    for family in search_response.families:
        _LOGGER.debug(f"Family: {family}")
        family_metadata = extra_required_info["metadata"].get(family.family_slug, {})
        if not family_metadata:
            _LOGGER.error(f"Failed to find metadata for '{family.family_slug}'")
        family_source = extra_required_info["source"].get(family.family_slug, "")
        if not family_source:
            _LOGGER.error(f"Failed to identify organisation for '{family.family_slug}'")

        family_geos = ";".join(
            [cast(str, geo) for geo in family.family_geographies]
            if family is not None
            else []
        )

        collection = extra_required_info["collection"].get(family.family_slug)

        family_documents: Sequence[FamilyDocument] = extra_required_info["documents"][
            family.family_slug
        ]
        if family_documents:
            for document in family_documents:
                _LOGGER.info(f"Document: {document}")
                physical_document = document.physical_document

                if physical_document is None:
                    document_content = ""
                    document_title = ""
                else:
                    document_content = (
                        to_cdn_url(cast(str, physical_document.cdn_object))
                        or physical_document.source_url
                        or ""
                    )
                    document_title = physical_document.title

                if is_browse:
                    document_match = "n/a"
                else:
                    if physical_document is None:
                        document_match = "No"
                    else:
                        document_match = (
                            "Yes"
                            if bool(
                                {slug.name for slug in document.slugs}
                                & all_matching_document_slugs
                            )
                            else "No"
                        )

                document_languages = ";".join(
                    [
                        cast(str, language.name)
                        for language in physical_document.languages
                    ]
                    if physical_document is not None
                    else []
                )

                if is_ccc_theme:
                    # CCC theme columns
                    row = _create_ccc_csv_row(
                        family,
                        document,
                        collection,
                        family_metadata,
                        family_geos,
                        document_title,
                        document_content,
                        document_match,
                        url_base,
                        extra_required_info["document_events"],
                    )
                else:
                    row = _create_standard_csv_row(
                        family,
                        document,
                        collection,
                        family_metadata,
                        family_source,
                        family_geos,
                        document_title,
                        document_content,
                        document_match,
                        url_base,
                        document_languages,
                    )

                rows.append(row)
        else:
            # Always write a row, even if the Family contains no documents
            if is_ccc_theme:
                # CCC theme columns
                row = _create_ccc_csv_row(
                    family,
                    None,
                    collection,
                    family_metadata,
                    family_geos,
                    "",  # Document title
                    "",  # Document content
                    "n/a",
                    url_base,
                    extra_required_info["document_events"],
                )
            else:
                row = _create_standard_csv_row(
                    family,
                    None,
                    collection,
                    family_metadata,
                    family_source,
                    family_geos,
                    "",  # Document title
                    "",  # Document content
                    "n/a",
                    url_base,
                    "",  # Document languages
                )

            rows.append(row)

    csv_result_io = StringIO("")

    if is_ccc_theme:
        csv_fieldnames = _CCC_CSV_SEARCH_RESPONSE_COLUMNS
    else:
        csv_fieldnames = _CSV_SEARCH_RESPONSE_COLUMNS

    writer = csv.DictWriter(
        csv_result_io, fieldnames=csv_fieldnames, extrasaction="ignore"
    )
    writer.writeheader()
    for row in rows:
        writer.writerow(row)

    csv_result_io.seek(0)
    return csv_result_io.read()


def _create_ccc_csv_row(
    family: SearchResponseFamily,
    document: Optional[FamilyDocument],
    collection: Optional[Collection],
    family_metadata: dict,
    family_geos: str,
    document_title: str,
    document_content: str,
    document_match: str,
    url_base: str,
    document_events: Optional[Mapping[str, list[FamilyEvent]]] = None,
) -> dict:
    """Create a CSV row for CCC theme format."""

    # Extract CCC-specific metadata
    case_number = ";".join(family_metadata.get("case_number", []))
    concept_labels = family_metadata.get("concept_preferred_label", [])
    case_categories = parse_concept_labels(concept_labels, "category")
    principal_laws = parse_concept_labels(concept_labels, "principal_law")
    jurisdictions = parse_concept_labels(concept_labels, "jurisdiction")
    status = ";".join(family_metadata.get("status", []))
    court_number = ";".join(family_metadata.get("court_number", []))
    non_english_case_name = ";".join(family_metadata.get("original_case_name", []))

    # This currently assumes that a family can only be associated with a single
    # collection - multi collection support to come.
    collection_id = collection.import_id if collection else ""
    collection_name = collection.title if collection else ""
    collection_url = f"{url_base}/collection/{collection_id}" if collection_id else ""

    # Get family dates
    family_date = family.family_date or ""
    if family_date:
        try:
            # Extract year from ISO date string
            year = family_date.split("-")[0]
        except (AttributeError, IndexError):
            year = ""
    else:
        year = ""

    # For USA, we need to get the collection description & for non-USA, we need to get
    # the core_object labels for the at issue value. If the family is associated with
    # multiple different countries including the US it is classed as global.
    #
    # We are assuming in this block of code that a case is only associated with a single
    # country, even if it is associated with multiple subdivisions within that country
    # for now.
    core_object = family_metadata.get("core_object", [])
    is_usa = "USA" in family_geos and all(
        geo == "USA" or geo.startswith("US-") for geo in family_geos
    )
    if is_usa and collection is not None:
        at_issue = collection.description
    elif not is_usa and len(core_object) > 0:
        at_issue = ";".join([label for label in core_object])
    else:
        at_issue = ""

    # Get document event data
    document_filing_date = ""
    document_summary = ""
    document_type = ""
    if document and document_events:
        doc_events = document_events.get(str(document.import_id), [])
        if doc_events:
            # Get the earliest event (first in the list since we ordered by date asc)
            earliest_event = doc_events[0]
            document_filing_date = earliest_event.date.isoformat()
            document_summary = ""
            document_type = ""
            if earliest_event.valid_metadata:
                description = earliest_event.valid_metadata.get("description")
                if (
                    description
                    and isinstance(description, list)
                    and len(description) > 0
                ):
                    document_summary = description[0]

                # Get document type - which is not the same as the document_type field
                # in our database for litigation document. Instead this is the type of
                # the event associated with the document.
                event_type = earliest_event.valid_metadata.get("event_type")
                if event_type and isinstance(event_type, list) and len(event_type) > 0:
                    document_type = event_type[0]

    # Another silly US vs non US piece of logic for the document title. US documents
    # have document titles but some non US documents don't. Where that is the case we
    # use the family name as the document title.
    if not is_usa:
        document_title = f"{family.family_name} - {document_type}"
    else:
        document_title = document_title

    return {
        "Bundle ID": collection_id,
        "Bundle Name": collection_name,
        "Bundle URL": collection_url,
        "Case ID": family.family_slug,
        "Case Name": family.family_name,
        "Non-English Case Name": non_english_case_name,
        "Case URL": f"{url_base}/document/{family.family_slug}",
        "At Issue": at_issue,
        "Case Summary": family.family_description,
        "Case Number": case_number,
        "Case Filing Year for Action": year,
        "Status": status,
        "Jurisdictions": jurisdictions,
        "Case Categories": case_categories,
        "Principal Laws": principal_laws,
        "Court Number": court_number,
        "Document Title": document_title,
        "Document Filing Date": document_filing_date,
        "Document Summary": document_summary,
        "Document URL": (
            f"{url_base}/documents/{document.slugs[-1].name}"
            if document and document.slugs
            else ""
        ),
        "Document Content URL": document_content,
        "Document Type": document_type,
        "Geographies": family_geos,
        "Document Content Matches Search Phrase": document_match,
    }


def _create_standard_csv_row(
    family: SearchResponseFamily,
    document: Optional[FamilyDocument],
    collection: Optional[Collection],
    family_metadata: dict,
    family_source: str,
    family_geos: str,
    document_title: str,
    document_content: str,
    document_match: str,
    url_base: str,
    document_languages: str,
) -> dict:
    """Create a CSV row for standard theme format."""

    collection_name = collection.title if collection else ""
    collection_summary = collection.description if collection else ""

    # Process metadata for standard CSV format
    metadata: dict[str, str] = defaultdict(str)
    for k in family_metadata:
        metadata[k.title()] = ";".join(family_metadata.get(k, []))

    return {
        "Collection Name": collection_name,
        "Collection Summary": collection_summary,
        "Family Name": family.family_name,
        "Family Summary": family.family_description,
        "Family Publication Date": family.family_date,
        "Family URL": f"{url_base}/document/{family.family_slug}",
        "Document Title": document_title,
        "Document URL": (
            f"{url_base}/documents/{document.slugs[-1].name}"
            if document and document.slugs
            else ""
        ),
        "Document Content URL": document_content,
        "Document Type": (
            doc_type_from_family_document_metadata(document) if document else ""
        ),
        "Document Content Matches Search Phrase": document_match,
        "Geographies": family_geos,
        "Category": family.family_category,
        "Languages": document_languages,
        "Source": family_source,
        **metadata,
    }


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


def generate_data_dump_readme(ingest_cycle_start: str, theme: Optional[str] = None):
    partner_name = ""
    match theme:
        case "cclw":
            partner_name = " and Climate Change Laws of the World"
        case "mcf":
            partner_name = (
                " and the Multilateral Climate Funds' Climate Project Explorer"
            )
        case "ccc":
            partner_name = " and the Sabin Center's Climate Litigation Database"
        case _:
            partner_name = ""

    file_buffer = StringIO(
        "Thank you for downloading the full document dataset from Climate Policy Radar"
        f"{partner_name}!"
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
    ingest_cycle_start: str,
    allowed_corpora_ids: list[str],
    db=Depends(get_db),
    theme: Optional[str] = None,
):
    readme_buffer = generate_data_dump_readme(ingest_cycle_start, theme)

    csv_buffer = generate_data_dump_as_csv(ingest_cycle_start, allowed_corpora_ids, db)

    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
        for file_name, data in [
            ("README.txt", readme_buffer),
            (f"Document_Data_Download-{ingest_cycle_start}.csv", csv_buffer),
        ]:
            zip_file.writestr(file_name, data.getvalue())

    return zip_buffer
