"""
Searches for documents.

All endpoints should perform document searches using the SearchRequestBody as
its input. The individual endpoints will return different responses tailored
for the type of document search being performed.
"""
import json
import logging
from io import BytesIO
from typing import Mapping, Optional, Sequence, cast

from cpr_data_access.search_adaptors import VespaSearchAdapter
from cpr_data_access.models.search import Document as DataAccessResponseDocument
from cpr_data_access.models.search import Family as DataAccessResponseFamily
from cpr_data_access.models.search import FilterField as DataAccessFilterField
from cpr_data_access.models.search import Passage as DataAccessResponsePassage
from cpr_data_access.models.search import SearchRequestBody as DataAccessSearchRequest
from cpr_data_access.models.search import SearchResponse as DataAccessSearchResponse
from cpr_data_access.models.search import SortField as DataAccessSortField
from cpr_data_access.models.search import SortOrder as DataAccessSortOrder
from fastapi import APIRouter, Depends, Request
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from app.api.api_v1.schemas.search import (
    SearchRequestBody,
    SearchResponse,
    SortField,
    SortOrder,
)
from app.core.browse import BrowseArgs, browse_rds_families
from app.core.config import (
    VESPA_SECRETS_LOCATION,
    VESPA_URL,
    VESPA_SEARCH_LIMIT,
    VESPA_SEARCH_MATCHES_PER_DOC,
)
from app.core.lookups import get_countries_for_region, get_country_by_slug
from app.core.search import (
    FilterField,
    OpenSearchConnection,
    OpenSearchConfig,
    OpenSearchQueryConfig,
    SearchResponseFamily,
    SearchResponseFamilyDocument,
    SearchResponseDocumentPassage,
    process_result_into_csv,
)
from app.db.crud.document import DocumentExtraCache
from app.db.session import get_db

_LOGGER = logging.getLogger(__name__)

# Use configured environment for router
_OPENSEARCH_CONFIG = OpenSearchConfig()
_OPENSEARCH_CONNECTION = OpenSearchConnection(opensearch_config=_OPENSEARCH_CONFIG)
_OPENSEARCH_INDEX_CONFIG = OpenSearchQueryConfig()
_DOCUMENT_EXTRA_INFO_CACHE = DocumentExtraCache()

_VESPA_CONNECTION = VespaSearchAdapter(
    instance_url=VESPA_URL,
    cert_directory=VESPA_SECRETS_LOCATION,
)

search_router = APIRouter()


def _convert_sort_field(
    sort_field: Optional[SortField],
) -> Optional[DataAccessSortField]:
    if sort_field is None:
        return None

    if sort_field == SortField.DATE:
        return "date"
    if sort_field == SortField.TITLE:
        return "name"


def _convert_sort_order(sort_order: SortOrder) -> DataAccessSortOrder:
    if sort_order == SortOrder.ASCENDING:
        return "ascending"
    if sort_order == SortOrder.DESCENDING:
        return "descending"


def _convert_filter_field(filter_field: FilterField) -> DataAccessFilterField:
    if filter_field == FilterField.CATEGORY:
        return "category"
    if filter_field == FilterField.COUNTRY:
        return "geography"
    if filter_field == FilterField.REGION:
        return "geography"
    if filter_field == FilterField.LANGUAGE:
        return "language"
    if filter_field == FilterField.SOURCE:
        return "source"


def _convert_filters(
    db: Session,
    keyword_filters: Optional[Mapping[FilterField, Sequence[str]]],
) -> Optional[Mapping[DataAccessFilterField, Sequence[str]]]:
    if keyword_filters is None:
        return None

    new_keyword_filters = {}
    for field, values in keyword_filters.items():
        if field == FilterField.REGION:
            new_values = []
            for region in values:
                new_values.extend(get_countries_for_region(db, region))
        else:
            new_values = values
        new_keyword_filters[_convert_filter_field(field)] = new_values
    return new_keyword_filters


from app.db.models.law_policy import Family, FamilyMetadata, FamilyDocument, FamilyStatus
from app.core.util import to_cdn_url


def _process_vespa_search_response_families(
    db: Session, vespa_families: Sequence[DataAccessResponseFamily]
) -> Sequence[SearchResponseFamily]:
    all_family_ids = [vf.id for vf in vespa_families]

    family_and_family_metadata: Sequence[tuple[Family, FamilyMetadata]] = (
        db.query(Family, FamilyMetadata)
        .filter(Family.import_id.in_(all_family_ids))
        .join(FamilyMetadata, FamilyMetadata.family_import_id == Family.import_id)
        .all()
    )  # type: ignore
    db_family_lookup: Mapping[str, tuple[Family, FamilyMetadata]] = {
        str(family.import_id): (family, family_metadata)
        for (family, family_metadata) in family_and_family_metadata
    }
    db_family_document_lookup: Mapping[str, FamilyDocument] = {
        str(fd.import_id): fd for (fam, _) in family_and_family_metadata
        for fd in fam.family_documents
    }

    response_families = []
    response_family = None

    for vespa_family in vespa_families:
        db_family_tuple = db_family_lookup.get(vespa_family.id)
        if db_family_tuple is None:
            _LOGGER.error(f"Could not locate family with import id '{vespa_family.id}'")
            continue
        if db_family_tuple[0].family_status != FamilyStatus.PUBLISHED:
            _LOGGER.debug(
                f"Skipping unpublished family with id '{vespa_family.id}' "
                "in search results"
            )
        db_family = db_family_tuple[0]
        db_family_metadata = db_family_tuple[1]

        response_family_lookup = {}
        response_document_lookup = {}

        for hit in vespa_family.hits:
            family_import_id = hit.family_import_id
            if family_import_id is None:
                _LOGGER.error("Skipping hit with empty family import id")
                continue

            response_family = response_family_lookup.get(family_import_id)
            # All hits contain required family info to create response
            if response_family is None:
                response_family = SearchResponseFamily(
                    family_slug=hit.family_slug,
                    family_name=hit.family_name,
                    family_description=hit.family_description,
                    family_category=hit.family_category,
                    family_date=db_family.publication_date,
                    family_last_updated_date=db_family.last_updated_date,
                    family_source=hit.family_source,
                    family_description_match=False,
                    family_title_match=False,
                    family_documents=[],
                    family_geography=hit.family_geography,
                    family_metadata=cast(dict, db_family_metadata.value),
                )

            if isinstance(hit, DataAccessResponseDocument):
                response_family.family_description_match = True
                response_family.family_title_match = True

            if isinstance(hit, DataAccessResponsePassage):
                document_import_id = hit.document_import_id
                if document_import_id is None:
                    _LOGGER.error("Skipping hit with empty document import id")
                    continue

                response_document = response_document_lookup.get(document_import_id)
                if response_document is None:
                    db_family_document = db_family_document_lookup.get(document_import_id)
                    if db_family_document is None:
                        _LOGGER.error(f"Skipping unknown family document with id '{document_import_id}'")
                        continue
                    response_document = SearchResponseFamilyDocument(
                        document_title=db_family_document.physical_document.title,
                        document_slug=hit.document_slug,
                        document_type=db_family_document.document_type,
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

        response_families.append(response_family)
        response_family = None

    return response_families


def _process_vespa_search_response(
    db: Session, vespa_search_response: DataAccessSearchResponse
) -> SearchResponse:
    # TODO: implement conversion
    return SearchResponse(
        hits=vespa_search_response.total_hits,
        query_time_ms=vespa_search_response.query_time_ms or 0,
        total_time_ms=vespa_search_response.total_time_ms or 0,
        families=[],
    )


def _search_request(
    db: Session, search_body: SearchRequestBody, use_vespa: bool = False
) -> SearchResponse:
    if search_body.keyword_filters is not None:
        search_body.keyword_filters = process_search_keyword_filters(
            db,
            search_body.keyword_filters,
        )
    is_browse_request = not search_body.query_string
    if is_browse_request:
        # Service browse requests from RDS
        return browse_rds_families(
            db=db,
            req=_get_browse_args_from_search_request_body(search_body),
        )
    else:
        if use_vespa:
            data_access_search_body = DataAccessSearchRequest(
                query_string=search_body.query_string,
                exact_match=search_body.exact_match,
                limit=VESPA_SEARCH_LIMIT,
                max_hits_per_family=VESPA_SEARCH_MATCHES_PER_DOC,
                keyword_filters=_convert_filters(db, search_body.keyword_filters),
                year_range=search_body.year_range,
                sort_by=_convert_sort_field(search_body.sort_field),
                sort_order=_convert_sort_order(search_body.sort_order),
                continuation_token=None,  # TODO: implement pagination?
            )
            data_access_search_response = _VESPA_CONNECTION.search(
                request=data_access_search_body
            )
            return _process_vespa_search_response(db, data_access_search_response)
        else:
            return _OPENSEARCH_CONNECTION.query_families(
                search_request_body=search_body,
                opensearch_internal_config=_OPENSEARCH_INDEX_CONFIG,
                document_extra_info=_DOCUMENT_EXTRA_INFO_CACHE.get_document_extra_info(
                    db
                ),
                preference="default_search_preference",
            )


@search_router.post("/searches")
def search_documents(
    request: Request,
    search_body: SearchRequestBody,
    db=Depends(get_db),
    use_vespa: bool = False,
) -> SearchResponse:
    """Search for documents matching the search criteria."""
    _LOGGER.info(
        "Search request",
        extra={
            "props": {
                "search_request": json.loads(search_body.json()),
            }
        },
    )

    _LOGGER.info(
        "Starting search...",
    )
    return _search_request(db=db, search_body=search_body, use_vespa=use_vespa)


@search_router.post("/searches/download-csv")
def download_search_documents(
    request: Request,
    search_body: SearchRequestBody,
    db=Depends(get_db),
    use_vespa: bool = False,
) -> StreamingResponse:
    """Download a CSV containing details of documents matching the search criteria."""
    _LOGGER.info(
        "Search download request",
        extra={
            "props": {
                "search_request": json.loads(search_body.json()),
            }
        },
    )
    # Always download all results
    search_body.offset = 0
    # TODO: properly configure the limit - for now we override if under 100 or not set
    search_body.limit = max(search_body.limit, 100)
    is_browse = not bool(search_body.query_string)

    _LOGGER.info(
        "Starting search...",
    )
    search_response = _search_request(
        db=db,
        search_body=search_body,
        use_vespa=use_vespa,
    )
    content_str = process_result_into_csv(db, search_response, is_browse=is_browse)

    _LOGGER.debug(f"Downloading search results as CSV: {content_str}")
    return StreamingResponse(
        content=BytesIO(content_str.encode("utf-8")),
        headers={
            "Content-Type": "text/csv",
            "Content-Disposition": "attachment; filename=results.csv",
        },
    )


def _get_browse_args_from_search_request_body(
    search_body: SearchRequestBody,
) -> BrowseArgs:
    keyword_filters = search_body.keyword_filters
    if keyword_filters is None:
        country_codes = None
        categories = None
    else:
        country_codes = keyword_filters.get(FilterField.COUNTRY)
        categories = keyword_filters.get(FilterField.CATEGORY)
    start_year, end_year = search_body.year_range or [None, None]
    return BrowseArgs(
        country_codes=country_codes,
        start_year=start_year,
        end_year=end_year,
        categories=categories,
        sort_field=search_body.sort_field or SortField.DATE,
        sort_order=search_body.sort_order,
        limit=search_body.limit,
        offset=search_body.offset,
    )


def process_search_keyword_filters(
    db: Session,
    request_filters: Mapping[FilterField, Sequence[str]],
) -> Mapping[FilterField, Sequence[str]]:
    filter_map = {}

    for field, values in request_filters.items():
        if field == FilterField.REGION:
            field = FilterField.COUNTRY
            filter_values = []
            for geo_slug in values:
                filter_values.extend(
                    [g.value for g in get_countries_for_region(db, geo_slug)]
                )
        elif field == FilterField.COUNTRY:
            filter_values = [
                country.value
                for geo_slug in values
                if (country := get_country_by_slug(db, geo_slug)) is not None
            ]
        else:
            filter_values = values

        if filter_values:
            values = filter_map.get(field, [])
            values.extend(filter_values)
            # Be consistent in ordering for search
            values = sorted(list(set(values)))
            filter_map[field] = values

    return filter_map
