import csv
import itertools
import logging
from collections import defaultdict
from io import StringIO
from typing import Any, Mapping, Optional, Sequence, cast

from cpr_data_access.embedding import Embedder
from cpr_data_access.models.search import Document as DataAccessResponseDocument
from cpr_data_access.models.search import Family as DataAccessResponseFamily
from cpr_data_access.models.search import Passage as DataAccessResponsePassage
from cpr_data_access.models.search import SearchResponse as DataAccessSearchResponse
from cpr_data_access.models.search import Filters as DataAccessKeywordFilters
from cpr_data_access.models.search import filter_fields
from sqlalchemy.orm import Session

from app.api.api_v1.schemas.search import (
    FilterField,
    BackendFilterValues,
    SearchRequestBody,
    SearchResponse,
    SearchResponseDocumentPassage,
    SearchResponseFamily,
    SearchResponseFamilyDocument,
)
from app.core.config import (
    INDEX_ENCODER_CACHE_FOLDER,
    PUBLIC_APP_URL,
)
from app.core.lookups import get_countries_for_region, get_countries_for_slugs
from app.core.util import to_cdn_url
from db_client.models.organisation import Organisation
from db_client.models.dfce import (
    Collection,
    CollectionFamily,
    Family,
    FamilyDocument,
    FamilyMetadata,
    FamilyOrganisation,
    Slug,
)
from db_client.models.dfce.family import DocumentStatus, FamilyStatus

_LOGGER = logging.getLogger(__name__)

ENCODER = Embedder(cache_folder=INDEX_ENCODER_CACHE_FOLDER)

_CSV_SEARCH_RESPONSE_COLUMNS = [
    "Collection Name",
    "Collection Summary",
    "Family Name",
    "Family Summary",
    "Family URL",
    "Family Publication Date",
    "Geography",
    "Document Title",
    "Document URL",
    "Document Content URL",
    "Document Type",
    "Document Content Matches Search Phrase",
    "Category",
    "Languages",
    "Source",
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
        .join(
            FamilyOrganisation,
            FamilyOrganisation.family_import_id == Slug.family_import_id,
        )
        .join(Organisation, Organisation.id == FamilyOrganisation.organisation_id)
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
    for slug, document in slug_and_family_document:
        family_slug_to_documents[slug.name].append(document)
    extra_csv_info = {
        "metadata": {
            slug.name: meta.value for (slug, meta) in slug_and_family_metadata
        },
        "source": {slug.name: org.name for (slug, org) in slug_and_organisation},
        "documents": family_slug_to_documents,
        "collection": {
            slug.name: collection for (slug, collection) in slug_and_collection
        },
    }

    return extra_csv_info


def process_result_into_csv(
    db: Session,
    search_response: SearchResponse,
    is_browse: bool,
) -> str:
    """
    Process a search/browse result into a CSV file for download.

    :param Session db: database session for supplementary queries
    :param SearchResponse search_response: the search result to process
    :param bool is_browse: a flag indicating whether this is a search/browse result
    :return str: the search result represented as CSV
    """
    extra_required_info = _get_extra_csv_info(db, search_response.families)
    all_matching_document_slugs = {
        d.document_slug
        for f in search_response.families
        for d in f.family_documents
        if d.document_passage_matches
    }

    url_base = f"{PUBLIC_APP_URL}/documents"
    metadata_keys = {}
    rows = []
    for family in search_response.families:
        _LOGGER.debug(f"Family: {family}")
        family_metadata = extra_required_info["metadata"].get(family.family_slug, {})
        if not family_metadata:
            _LOGGER.error(f"Failed to find metadata for '{family.family_slug}'")
        family_source = extra_required_info["source"].get(family.family_slug, "")
        if not family_source:
            _LOGGER.error(f"Failed to identify organisation for '{family.family_slug}'")

        if family_source not in metadata_keys:
            metadata_keys[family_source] = list(
                [key.title() for key in family_metadata.keys()]
            )
        metadata: dict[str, str] = defaultdict(str)
        for k in family_metadata:
            metadata[k.title()] = ";".join(family_metadata.get(k, []))

        collection_name = ""
        collection_summary = ""
        collection = extra_required_info["collection"].get(family.family_slug)
        if collection is not None:
            collection_name = collection.title
            collection_summary = collection.description

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
                row = {
                    "Collection Name": collection_name,
                    "Collection Summary": collection_summary,
                    "Family Name": family.family_name,
                    "Family Summary": family.family_description,
                    "Family Publication Date": family.family_date,
                    "Family URL": f"{url_base}/{family.family_slug}",
                    "Document Title": document_title,
                    "Document URL": f"{url_base}/{document.slugs[-1].name}",
                    "Document Content URL": document_content,
                    "Document Type": document.document_type,
                    "Document Content Matches Search Phrase": document_match,
                    "Geography": family.family_geography,
                    "Category": family.family_category,
                    "Languages": document_languages,
                    "Source": family_source,
                    **metadata,
                }
                rows.append(row)
        else:
            # Always write a row, even if the Family contains no documents
            row = {
                "Collection Name": collection_name,
                "Collection Summary": collection_summary,
                "Family Name": family.family_name,
                "Family Summary": family.family_description,
                "Family Publication Date": family.family_date,
                "Family URL": f"{url_base}/{family.family_slug}",
                "Document Title": "",
                "Document URL": "",
                "Document Content URL": "",
                "Document Type": "",
                "Document Content Matches Search Phrase": "n/a",
                "Geography": family.family_geography,
                "Category": family.family_category,
                "Languages": "",
                "Source": family_source,
                **metadata,
            }
            rows.append(row)

    csv_result_io = StringIO("")
    csv_fieldnames = list(
        itertools.chain(_CSV_SEARCH_RESPONSE_COLUMNS, *metadata_keys.values())
    )
    writer = csv.DictWriter(
        csv_result_io,
        fieldnames=csv_fieldnames,
    )
    writer.writeheader()
    for row in rows:
        writer.writerow(row)

    csv_result_io.seek(0)
    return csv_result_io.read()


def _convert_filter_field(filter_field: str) -> Optional[str]:
    if filter_field == FilterField.CATEGORY:
        return filter_fields["category"]
    if filter_field == FilterField.COUNTRY:
        return filter_fields["geography"]
    if filter_field == FilterField.REGION:
        return filter_fields["geography"]
    if filter_field == FilterField.LANGUAGE:
        return filter_fields["language"]
    if filter_field == FilterField.SOURCE:
        return filter_fields["source"]


def _convert_filters(
    db: Session,
    keyword_filters: Optional[Mapping[BackendFilterValues, Sequence[str]]],
) -> Optional[Mapping[str, Sequence[str]]]:
    if keyword_filters is None:
        return None
    new_keyword_filters = {}
    regions = []
    countries = []
    for field, values in keyword_filters.items():
        new_field = _convert_filter_field(field)
        if field == FilterField.REGION:
            for region in values:
                regions.extend(
                    [country.value for country in get_countries_for_region(db, region)]
                )
        elif field == FilterField.COUNTRY:
            countries.extend(
                [country.value for country in get_countries_for_slugs(db, values)]
            )
        else:
            new_values = values
            new_keyword_filters[new_field] = new_values

    # Regions and countries filters should only include the overlap
    geo_field = filter_fields["geography"]
    if regions and countries:
        values = list(set(countries).intersection(regions))
        if values:
            new_keyword_filters[geo_field] = values
    elif regions:
        new_keyword_filters[geo_field] = regions
    elif countries:
        new_keyword_filters[geo_field] = countries

    if len(new_keyword_filters) > 0:
        return new_keyword_filters
    else:
        return None


def _process_vespa_search_response_families(
    db: Session,
    vespa_families: Sequence[DataAccessResponseFamily],
    limit: int,
    offset: int,
) -> Sequence[SearchResponseFamily]:
    """
    Process a list of data access results into a list of SearchResponse Families

    Note: this function requires that results from the data access library are grouped
          by family_import_id.
    """
    vespa_families_to_process = vespa_families[offset : limit + offset]
    all_response_family_ids = [vf.id for vf in vespa_families_to_process]

    family_and_family_metadata: Sequence[tuple[Family, FamilyMetadata]] = (
        db.query(Family, FamilyMetadata)
        .filter(Family.import_id.in_(all_response_family_ids))
        .join(FamilyMetadata, FamilyMetadata.family_import_id == Family.import_id)
        .all()
    )  # type: ignore
    db_family_lookup: Mapping[str, tuple[Family, FamilyMetadata]] = {
        str(family.import_id): (family, family_metadata)
        for (family, family_metadata) in family_and_family_metadata
    }
    db_family_document_lookup: Mapping[str, FamilyDocument] = {
        str(fd.import_id): fd
        for (fam, _) in family_and_family_metadata
        for fd in fam.family_documents
    }

    response_families = []
    response_family = None

    for vespa_family in vespa_families_to_process:
        db_family_tuple = db_family_lookup.get(vespa_family.id)
        if db_family_tuple is None:
            _LOGGER.error(f"Could not locate family with import id '{vespa_family.id}'")
            continue
        if db_family_tuple[0].family_status != FamilyStatus.PUBLISHED:
            _LOGGER.debug(
                f"Skipping unpublished family with id '{vespa_family.id}' "
                "in search results"
            )
            continue
        db_family = db_family_tuple[0]
        db_family_metadata = db_family_tuple[1]

        response_family_lookup = {}
        response_document_lookup = {}

        for hit in vespa_family.hits:
            family_import_id = hit.family_import_id
            if family_import_id is None:
                _LOGGER.error("Skipping hit with empty family import id")
                continue

            # Check for all required family/document fields in the hit
            if (
                hit.family_slug is None
                or hit.document_slug is None
                or hit.family_name is None
                or hit.family_category is None
                or hit.family_source is None
                or hit.family_geography is None
            ):
                _LOGGER.error(
                    "Skipping hit with empty required family info for import "
                    f"id: {family_import_id}"
                )
                continue

            response_family = response_family_lookup.get(family_import_id)
            # All hits contain required family info to create response
            if response_family is None:
                response_family = SearchResponseFamily(
                    family_slug=hit.family_slug,
                    family_name=hit.family_name,
                    family_description=hit.family_description or "",
                    family_category=hit.family_category,
                    family_date=(
                        db_family.published_date.isoformat()
                        if db_family.published_date is not None
                        else ""
                    ),
                    family_last_updated_date=(
                        db_family.last_updated_date.isoformat()
                        if db_family.last_updated_date is not None
                        else ""
                    ),
                    family_source=hit.family_source,
                    family_description_match=False,
                    family_title_match=False,
                    total_passage_hits=vespa_family.total_passage_hits,
                    continuation_token=vespa_family.continuation_token,
                    prev_continuation_token=vespa_family.prev_continuation_token,
                    family_documents=[],
                    family_geography=hit.family_geography,
                    family_metadata=cast(dict, db_family_metadata.value),
                )
                response_family_lookup[family_import_id] = response_family

            if isinstance(hit, DataAccessResponseDocument):
                response_family.family_description_match = True
                response_family.family_title_match = True

            elif isinstance(hit, DataAccessResponsePassage):
                document_import_id = hit.document_import_id
                if document_import_id is None:
                    _LOGGER.error("Skipping hit with empty document import id")
                    continue

                response_document = response_document_lookup.get(document_import_id)
                if response_document is None:
                    db_family_document = db_family_document_lookup.get(
                        document_import_id
                    )
                    if db_family_document is None:
                        _LOGGER.error(
                            "Skipping unknown family document with id "
                            f"'{document_import_id}'"
                        )
                        continue

                    response_document = SearchResponseFamilyDocument(
                        document_title=str(db_family_document.physical_document.title),
                        document_slug=hit.document_slug,
                        document_type=str(db_family_document.document_type),
                        document_source_url=hit.document_source_url,
                        document_url=to_cdn_url(hit.document_cdn_object),
                        document_content_type=hit.document_content_type,
                        document_passage_matches=[],
                    )
                    response_document_lookup[document_import_id] = response_document
                    response_family.family_documents.append(response_document)

                response_document.document_passage_matches.append(
                    SearchResponseDocumentPassage(
                        text=hit.text_block,
                        text_block_id=hit.text_block_id,
                        text_block_page=hit.text_block_page,
                        text_block_coords=hit.text_block_coords,
                    )
                )

            else:
                _LOGGER.error(f"Unknown hit type: {type(hit)}")

        response_families.append(response_family)
        response_family = None

    return response_families


def process_vespa_search_response(
    db: Session,
    vespa_search_response: DataAccessSearchResponse,
    limit: int,
    offset: int,
) -> SearchResponse:
    """Process a Vespa search response into a F/E search response"""

    return SearchResponse(
        hits=len(vespa_search_response.families),
        total_family_hits=vespa_search_response.total_family_hits,
        query_time_ms=vespa_search_response.query_time_ms or 0,
        total_time_ms=vespa_search_response.total_time_ms or 0,
        continuation_token=vespa_search_response.continuation_token,
        this_continuation_token=vespa_search_response.this_continuation_token,
        prev_continuation_token=vespa_search_response.prev_continuation_token,
        families=_process_vespa_search_response_families(
            db,
            vespa_search_response.families,
            limit=limit,
            offset=offset,
        ),
    )


def create_vespa_search_params(
    db: Session, search_body: SearchRequestBody
) -> SearchRequestBody:
    """Create Vespa search parameters from a F/E search request body"""
    converted_filters = _convert_filters(db, search_body.keyword_filters)
    if converted_filters:
        search_body.filters = DataAccessKeywordFilters.model_validate(converted_filters)
    else:
        search_body.filters = None
    return search_body
