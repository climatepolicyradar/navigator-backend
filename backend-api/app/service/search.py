import csv
import itertools
import logging
import re
from collections import defaultdict
from enum import Enum
from io import StringIO
from typing import Any, Mapping, Optional, Sequence, Tuple, cast

from cpr_sdk.exceptions import QueryError
from cpr_sdk.models.search import Document as CprSdkResponseDocument
from cpr_sdk.models.search import Family as CprSdkResponseFamily
from cpr_sdk.models.search import Filters as CprSdkKeywordFilters
from cpr_sdk.models.search import Passage as CprSdkResponsePassage
from cpr_sdk.models.search import SearchParameters
from cpr_sdk.models.search import SearchResponse as CprSdkSearchResponse
from cpr_sdk.models.search import filter_fields
from cpr_sdk.search_adaptors import VespaSearchAdapter
from db_client.models.dfce import (
    Collection,
    CollectionFamily,
    Family,
    FamilyDocument,
    FamilyMetadata,
    Slug,
)
from db_client.models.dfce.family import (
    Corpus,
    DocumentStatus,
    FamilyCorpus,
    FamilyStatus,
)
from db_client.models.organisation import Organisation
from sqlalchemy.orm import Session

from app.clients.aws.client import S3Client
from app.clients.aws.s3_document import S3Document
from app.config import CDN_DOMAIN
from app.errors import ValidationError
from app.models.search import (
    BackendFilterValues,
    FilterField,
    SearchRequestBody,
    SearchResponse,
    SearchResponseDocumentPassage,
    SearchResponseFamily,
    SearchResponseFamilyDocument,
)
from app.repository.lookups import (
    get_geographies_as_iso_codes_with_fallback,  # TODO: remove this once frontend is updated to use ISO codes in favour of get_countries_by_iso_codes
)
from app.repository.lookups import (
    validate_subdivision_iso_codes,  # TODO: update this to use geographies api endpoint when refactoring geographies to use iso codes
)
from app.repository.lookups import (
    doc_type_from_family_document_metadata,
    get_countries_for_region,
)
from app.service.util import to_cdn_url
from app.telemetry import observe

_LOGGER = logging.getLogger(__name__)


class SearchType(str, Enum):
    standard = "standard"
    browse = "browse"
    browse_with_concepts = "browse_with_concepts"


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


@observe("process_result_into_csv")
def process_result_into_csv(
    db: Session,
    search_response: SearchResponse,
    base_url: Optional[str],
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

    if base_url is None:
        raise ValidationError("Error creating CSV")

    scheme = "http" if "localhost" in base_url else "https"
    url_base = f"{scheme}://{base_url}"
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

        family_geos = ";".join(
            [cast(str, geo) for geo in family.family_geographies]
            if family is not None
            else []
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
                    "Family URL": f"{url_base}/document/{family.family_slug}",
                    "Document Title": document_title,
                    "Document URL": f"{url_base}/documents/{document.slugs[-1].name}",
                    "Document Content URL": document_content,
                    "Document Type": doc_type_from_family_document_metadata(document),
                    "Document Content Matches Search Phrase": document_match,
                    "Geographies": family_geos,
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
                "Family URL": f"{url_base}/document/{family.family_slug}",
                "Document Title": "",
                "Document URL": "",
                "Document Content URL": "",
                "Document Type": "",
                "Document Content Matches Search Phrase": "n/a",
                "Geographies": family_geos,
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
        csv_result_io, fieldnames=csv_fieldnames, extrasaction="ignore"
    )
    writer.writeheader()
    for row in rows:
        writer.writerow(row)

    csv_result_io.seek(0)
    return csv_result_io.read()


def _parse_text_block_id(text_block_id: Optional[str]) -> Tuple[Optional[int], int]:
    """
    Parse a text block ID into its page and block numbers.

    Supports the following formats:
    - p{page}_b{block}
    - b{block}
    - {block}
    - block_{block}

    :param text_block_id: The text block ID string to parse
    :return: Tuple of (page number, block ID)
    """
    if text_block_id is None:
        return None, 0

    # Try to match the p{page}_b{block} format
    page_block_match = re.match(r"p(\d+)_b(\d+)", text_block_id)
    if page_block_match:
        return int(page_block_match.group(1)), int(page_block_match.group(2))

    # Try to match b{block} format
    block_match = re.match(r"b(\d+)", text_block_id)
    if block_match:
        return None, int(block_match.group(1))

    # Try to match {block} format
    simple_block_match = re.match(r"^\d+$", text_block_id)
    if simple_block_match:
        return None, int(text_block_id)

    # Try to match block_{block} format
    block_prefix_match = re.match(r"block_(\d+)", text_block_id)
    if block_prefix_match:
        return None, int(block_prefix_match.group(1))

    # If no match, treat as block 0
    _LOGGER.warning(
        f"No match for text block ID: {text_block_id}. Defaulting to (0, 0)"
    )
    return None, 0


def _convert_filter_field(filter_field: str) -> Optional[str]:
    if filter_field == FilterField.CATEGORY:
        return filter_fields["category"]
    if filter_field == FilterField.COUNTRY:
        return filter_fields["geographies"]
    if filter_field == FilterField.REGION:
        return filter_fields["geographies"]
    if filter_field == FilterField.SUBDIVISION:
        return filter_fields["geographies"]
    if filter_field == FilterField.LANGUAGE:
        return filter_fields["language"]
    if filter_field == FilterField.SOURCE:
        return filter_fields["source"]


def _convert_filters(
    db: Session,
    keyword_filters: Optional[Mapping[BackendFilterValues, Sequence[str]]],
) -> Optional[Mapping[str, Sequence[str]]]:
    if not keyword_filters:
        return None
    new_keyword_filters = {}
    regions = []
    countries = []
    subdivisions = []
    for field, values in keyword_filters.items():
        if not values:
            continue

        new_field = _convert_filter_field(field)
        if field == FilterField.REGION:
            for region in values:
                regions.extend(
                    [country.value for country in get_countries_for_region(db, region)]
                )
        elif field == FilterField.COUNTRY:
            countries.extend(
                # TODO: remove this once frontend is updated to use ISO codes in favour of get_countries_by_iso_codes
                get_geographies_as_iso_codes_with_fallback(db, values)
            )
        elif field == FilterField.SUBDIVISION:
            subdivisions.extend(validate_subdivision_iso_codes(db, values))

        else:
            new_values = values
            new_keyword_filters[new_field] = new_values

    # Regions and countries filters should only include the overlap
    geo_field = filter_fields["geographies"]
    if subdivisions:
        new_keyword_filters[geo_field] = subdivisions
    elif regions and countries:
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
    vespa_families: Sequence[CprSdkResponseFamily],
    limit: int,
    offset: int,
    sort_within_page: bool,
) -> Sequence[SearchResponseFamily]:
    """
    Process a list of cpr sdk results into a list of SearchResponse Families

    Note: this function requires that results from the cpr sdk library are grouped
          by family_import_id.
    """
    vespa_families_to_process = vespa_families[offset : limit + offset]
    all_response_family_ids = [vf.id for vf in vespa_families_to_process]

    # TODO: Potential disparity between what's in postgres and vespa
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
                or hit.family_geographies is None
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
                    corpus_import_id=hit.corpus_import_id or "",
                    corpus_type_name=hit.corpus_type_name or "",
                    family_description_match=False,
                    family_title_match=False,
                    total_passage_hits=vespa_family.total_passage_hits,
                    continuation_token=vespa_family.continuation_token,
                    prev_continuation_token=vespa_family.prev_continuation_token,
                    family_documents=[],
                    family_geographies=hit.family_geographies,
                    family_metadata=cast(dict, db_family_metadata.value),
                )
                response_family_lookup[family_import_id] = response_family

            if isinstance(hit, CprSdkResponseDocument):
                response_family.family_description_match = True
                response_family.family_title_match = True

            elif isinstance(hit, CprSdkResponsePassage):
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
                        document_type=doc_type_from_family_document_metadata(
                            db_family_document
                        ),
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
                        concepts=hit.concepts,
                    )
                )
                if sort_within_page:
                    response_document.document_passage_matches.sort(
                        key=lambda x: (
                            (
                                x.text_block_page
                                if x.text_block_page is not None
                                else (
                                    _parse_text_block_id(x.text_block_id)[0]
                                    if _parse_text_block_id(x.text_block_id)[0]
                                    is not None
                                    else float("inf")
                                )
                            ),
                            _parse_text_block_id(x.text_block_id)[1],
                        )
                    )

            else:
                _LOGGER.error(f"Unknown hit type: {type(hit)}")

        response_families.append(response_family)
        response_family = None
    return response_families


@observe("process_vespa_search_response")
def process_vespa_search_response(
    db: Session,
    vespa_search_response: CprSdkSearchResponse,
    limit: int,
    offset: int,
    sort_within_page: bool,
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
            sort_within_page=sort_within_page,
        ),
    )


@observe("create_vespa_search_params")
def create_vespa_search_params(
    db: Session, search_body: SearchRequestBody
) -> SearchRequestBody:
    """Create Vespa search parameters from a F/E search request body"""
    converted_filters = _convert_filters(db, search_body.keyword_filters)
    if converted_filters:
        search_body.filters = CprSdkKeywordFilters.model_validate(converted_filters)
    else:
        search_body.filters = None
    return search_body


@observe("identify_search_type")
def identify_search_type(search_body: SearchRequestBody) -> str:
    """Identify the search type from parameters"""
    if not search_body.query_string and not search_body.concept_filters:
        return SearchType.browse
    elif not search_body.query_string and search_body.concept_filters:
        return SearchType.browse_with_concepts
    else:
        return SearchType.standard


@observe("mutate_search_body_for_search_type")
def mutate_search_body_for_search_type(
    search_body: SearchRequestBody,
) -> SearchRequestBody:
    """Mutate the search body in line with the search params"""
    search_type = identify_search_type(search_body=search_body)
    if search_type == SearchType.browse:
        search_body.all_results = True
        search_body.documents_only = True
        search_body.exact_match = False
    elif search_type == SearchType.browse_with_concepts:
        search_body.all_results = True
        search_body.documents_only = False
        search_body.exact_match = False
    return search_body


@observe("make_search_request")
def make_search_request(
    db: Session,
    vespa_search_adapter: VespaSearchAdapter,
    search_body: SearchRequestBody,
) -> SearchResponse:
    """Perform a search request against the Vespa search engine"""

    try:
        search_body = mutate_search_body_for_search_type(search_body=search_body)
        cpr_sdk_search_params = create_vespa_search_params(db, search_body)
        cpr_sdk_search_response = observe("vespa_search")(vespa_search_adapter.search)(
            parameters=cpr_sdk_search_params
        )
        return process_vespa_search_response(
            db,
            cpr_sdk_search_response,
            limit=search_body.page_size,
            offset=search_body.offset,
            sort_within_page=search_body.sort_within_page,
        ).increment_pages()
    except QueryError as e:
        _LOGGER.error(f"make_search_request QueryError: {e}")
        raise ValidationError(e)
    except Exception as e:
        _LOGGER.error(f"make_search_request Exception: {e}")
        raise Exception(e)


@observe("get_family_from_vespa")
def get_family_from_vespa(
    family_id: str,
    db: Session,
    vespa_search_adapter: VespaSearchAdapter,
) -> CprSdkSearchResponse:
    """Get a family from vespa.

    :param str family_id: The id of the family to get.
    :param Session db: Database session to query against.
    :return CprSdkSearchResponse: The family from vespa.
    """
    search_body = SearchParameters(
        family_ids=[family_id], documents_only=True, all_results=True
    )

    _LOGGER.info(
        f"Getting vespa family '{family_id}'",
        extra={"props": {"search_body": search_body.model_dump()}},
    )
    try:
        result = vespa_search_adapter.search(parameters=search_body)
    except QueryError as e:
        raise ValidationError(e)
    return result


@observe("get_document_from_vespa")
def get_document_from_vespa(
    document_id: str,
    db: Session,
    vespa_search_adapter: VespaSearchAdapter,
) -> CprSdkSearchResponse:
    """Get a document from vespa.

    :param str document_id: The id of the document to get.
    :param Session db: Database session to query against.
    :return CprSdkSearchResponse: The document from vespa.
    """
    search_body = SearchParameters(
        document_ids=[document_id], documents_only=True, all_results=True
    )

    _LOGGER.info(
        f"Getting vespa document '{document_id}'",
        extra={"props": {"search_body": search_body.model_dump()}},
    )
    try:
        result = vespa_search_adapter.search(parameters=search_body)
    except QueryError as e:
        raise ValidationError(e)
    return result


@observe("get_s3_doc_url_from_cdn")
def get_s3_doc_url_from_cdn(
    s3_client: S3Client, s3_document: S3Document, data_dump_s3_key: str
) -> Optional[str]:
    redirect_url = None
    if s3_client.document_exists(s3_document):
        _LOGGER.info("Redirecting to CDN data dump location...")
        redirect_url = f"https://{CDN_DOMAIN}/{data_dump_s3_key}"
    return redirect_url
