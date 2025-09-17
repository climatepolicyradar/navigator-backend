import logging
import re
from enum import Enum
from typing import Mapping, Optional, Sequence, Tuple, cast

from cpr_sdk.exceptions import QueryError
from cpr_sdk.models.search import Document as CprSdkResponseDocument
from cpr_sdk.models.search import Family as CprSdkResponseFamily
from cpr_sdk.models.search import Filters as CprSdkKeywordFilters
from cpr_sdk.models.search import Hit as CprSdkResponseHit
from cpr_sdk.models.search import Passage as CprSdkResponsePassage
from cpr_sdk.models.search import SearchParameters
from cpr_sdk.models.search import SearchResponse as CprSdkSearchResponse
from cpr_sdk.models.search import filter_fields
from cpr_sdk.search_adaptors import VespaSearchAdapter
from db_client.models.dfce import Family, FamilyDocument, FamilyMetadata
from db_client.models.dfce.family import FamilyStatus
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


def _vespa_hit_to_search_response_family(
    hit: CprSdkResponseHit,
    vespa_family: CprSdkResponseFamily,
    db_family: Family,
    db_family_metadata: FamilyMetadata,
) -> SearchResponseFamily:
    """Convert a Vespa hit into a SearchResponseFamily"""
    return SearchResponseFamily(
        family_slug=hit.family_slug or "",
        family_name=hit.family_name or "",
        family_description=hit.family_description or "",
        family_category=hit.family_category or "",
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
        family_source=hit.family_source or "",
        corpus_import_id=hit.corpus_import_id or "",
        corpus_type_name=hit.corpus_type_name or "",
        family_description_match=False,
        family_title_match=False,
        total_passage_hits=vespa_family.total_passage_hits,
        continuation_token=vespa_family.continuation_token,
        prev_continuation_token=vespa_family.prev_continuation_token,
        family_documents=[],
        family_geographies=hit.family_geographies or [],
        family_metadata=cast(dict, db_family_metadata.value),
    )


def _vespa_passage_hit_to_search_passage(
    hit: CprSdkResponsePassage,
) -> SearchResponseDocumentPassage:
    """Converts a Vespa hit into a SearchResponseDocumentPassage

    Sorting logic has been moved into this function.

    The sorting logic of passages within a document is as follows:
    1. Find page number -- either from text_block_page or parse text_block_id and extract
    2. If page number is not found, set to inf
    3. Find block number -- parsed from text_block_id
    4. Store both for sort.
    """

    parsed_text_block_id = _parse_text_block_id(hit.text_block_id)

    # If we don't have a page number, add in what we can
    if hit.text_block_page is None:
        if parsed_text_block_id is None or parsed_text_block_id[0] is None:
            hit.text_block_page = 999999
        else:
            hit.text_block_page = parsed_text_block_id[0]

    # Now we can set the sort key, for within-page sorting
    block_id_sort_key = parsed_text_block_id[
        1
    ]  # The _parse_text_block_id function is assumed, in original code, to return this...

    return SearchResponseDocumentPassage(
        text=hit.text_block,
        text_block_id=hit.text_block_id,
        text_block_page=hit.text_block_page,
        text_block_coords=hit.text_block_coords,
        concepts=hit.concepts,
        block_id_sort_key=block_id_sort_key,
    )


def _vespa_passage_hit_to_search_familydocument(
    hit: CprSdkResponsePassage, db_family_document: FamilyDocument
) -> SearchResponseFamilyDocument:
    return SearchResponseFamilyDocument(
        document_title=str(db_family_document.physical_document.title),
        document_slug=hit.document_slug or "",
        document_type=doc_type_from_family_document_metadata(db_family_document),
        document_source_url=hit.document_source_url,
        document_url=to_cdn_url(hit.document_cdn_object),
        document_content_type=hit.document_content_type,
        document_passage_matches=[],
    )


def _hit_is_missing_required_fields(hit: CprSdkResponseHit) -> bool:
    return (
        hit.family_slug is None
        or hit.document_slug is None
        or hit.family_name is None
        or hit.family_category is None
        or hit.family_source is None
        or hit.family_geographies is None
        or hit.family_import_id is None
    )


def _family_is_not_found_or_not_published(
    fam_tuple: Optional[tuple[Family, FamilyMetadata]]
) -> bool:
    return fam_tuple is None or fam_tuple[0].family_status != FamilyStatus.PUBLISHED


def _cached_or_new_family(
    hit: CprSdkResponseHit,
    lookup_table: dict,
    vespa_family: CprSdkResponseFamily,
    db_family: Family,
    db_family_metadata: FamilyMetadata,
) -> SearchResponseFamily:
    response_family = lookup_table.get(hit.family_import_id)
    # All hits contain required family info to create response
    if response_family is None:
        response_family = _vespa_hit_to_search_response_family(
            hit, vespa_family, db_family, db_family_metadata
        )

    if isinstance(hit, CprSdkResponseDocument):
        response_family.family_description_match = True
        response_family.family_title_match = True

    return response_family


def _process_vespa_search_response_families(
    db: Session,
    vespa_families: Sequence[CprSdkResponseFamily],
    limit: int,
    offset: int,
    sort_within_page: bool,
) -> Sequence[SearchResponseFamily]:
    """
    Process a list of cpr sdk results into a list of SearchResponse Families

    We receive a flat list of hits per family. We have to transform each
    family into a SearchResponseFamily, with a list of SearchResponseFamilyDocuments, each
    with a list of SearchResponseDocumentPassages that have been hit. That list may be sorted.

    Only results that have a published family are included in the response.

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

    response_family_lookup: Mapping[str, SearchResponseFamily] = {}
    response_document_lookup: Mapping[str, SearchResponseFamilyDocument] = {}

    for vespa_family in vespa_families_to_process:
        db_family_tuple = db_family_lookup.get(vespa_family.id)

        if _family_is_not_found_or_not_published(db_family_tuple):
            _LOGGER.error(
                f"Skipping unfound or unpublished family with id '{vespa_family.id}' in search results"
            )
            continue

        db_family = db_family_tuple[0]  # type: ignore -- we know it exists from check above
        db_family_metadata = db_family_tuple[1]  # type: ignore -- we know it exists

        for hit in vespa_family.hits:
            if _hit_is_missing_required_fields(hit):
                _LOGGER.error(
                    "Skipping hit with empty required family import_id OR info for import "
                    f"id: {hit.family_import_id}"
                )
                continue

            response_family = _cached_or_new_family(
                hit, response_family_lookup, vespa_family, db_family, db_family_metadata
            )
            response_family_lookup[hit.family_import_id] = response_family  # type: ignore -- we know it exists from check above

            if isinstance(hit, CprSdkResponsePassage):
                if hit.document_import_id is None:
                    _LOGGER.error("Skipping hit with empty document import id")
                    continue

                response_document = response_document_lookup.get(hit.document_import_id)
                if response_document is None:
                    db_family_document = db_family_document_lookup.get(
                        hit.document_import_id
                    )
                    if db_family_document is None:
                        _LOGGER.error(
                            "Skipping unknown family document with id "
                            f"'{hit.document_import_id}'"
                        )
                        continue

                    response_document = _vespa_passage_hit_to_search_familydocument(
                        hit, db_family_document
                    )
                    response_document_lookup[hit.document_import_id] = response_document
                    response_family.family_documents.append(response_document)

                response_document.document_passage_matches.append(
                    _vespa_passage_hit_to_search_passage(hit)
                )
            else:
                _LOGGER.error(f"Unknown hit type: {type(hit)}")

        response_families.append(response_family)
        response_family = None

    # OK NOW lets sort the passages within each document

    if sort_within_page:
        for response_family in response_families:
            for response_document in response_family.family_documents:
                # Updated to use keys from _vespa_passage_hit_to_search_passage
                # So we don't need defensive logic here.
                response_document.document_passage_matches.sort(
                    key=lambda x: (
                        x.text_block_page,
                        x.block_id_sort_key,
                    )
                )

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
