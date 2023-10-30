from typing import Sequence

import pytest
from cpr_data_access.models.search import (
    Document,
    Family,
    Hit,
    Passage,
    SearchParameters,
    SearchResponse,
    filter_fields,
    sort_fields,
    sort_orders,
)
from sqlalchemy.orm import Session

from app.core.config import VESPA_SEARCH_MATCHES_PER_DOC, VESPA_SEARCH_LIMIT
from app.core.search import (
    FilterField,
    IncludedResults,
    SearchRequestBody,
    SortField,
    SortOrder,
    create_vespa_search_params,
    process_vespa_search_response,
    _convert_filters,
    _convert_sort_field,
    _convert_sort_order,
)
from app.db.models.law_policy import Geography
from tests.core.ingestion.helpers import (
    COLLECTION_IMPORT_ID,
    FAMILY_IMPORT_ID,
    add_a_slug_for_family1_and_flush,
    get_doc_ingest_row_data,
    populate_for_ingest,
)


def db_setup(test_db):
    # Make sure we have geography tables etc populated
    populate_for_ingest(test_db)


TEST_EMPTY_RESULTS = SearchResponse(
    total_hits=0,
    query_time_ms=1,
    total_time_ms=2,
    families=[],
    continuation_token="ZZZ",
)


@pytest.mark.parametrize(
    (
        "query_string,exact_match,year_range,sort_field,sort_order,"
        "keyword_filters,limit,offset,continuation_token"
    ),
    [
        ("hello", True, None, None, SortOrder.ASCENDING, None, 10, 10, None),
        (
            "world",
            True,
            (1940, 1960),
            SortField.TITLE,
            SortOrder.DESCENDING,
            {FilterField.CATEGORY: ["Legislative"], FilterField.REGION: ["europe"]},
            10,
            0,
            "ABC",
        ),
        (
            "hello",
            False,
            (None, 1960),
            SortField.DATE,
            SortOrder.ASCENDING,
            {FilterField.SOURCE: ["UNFCCC"]},
            10,
            0,
            None,
        ),
        (
            "world",
            False,
            (1940, None),
            None,
            SortOrder.DESCENDING,
            {
                FilterField.COUNTRY: ["germany", "France"],
                FilterField.REGION: ["europe"],
            },
            10,
            0,
            "ABC",
        ),
        ("hello", True, None, SortField.TITLE, SortOrder.ASCENDING, None, 10, 0, None),
        (
            "world",
            True,
            (1940, 1960),
            SortField.DATE,
            SortOrder.DESCENDING,
            None,
            100,
            10,
            "ABC",
        ),
        (
            "hello",
            False,
            (None, 1960),
            None,
            SortOrder.ASCENDING,
            {FilterField.LANGUAGE: ["english"]},
            10,
            0,
            None,
        ),
        (
            "world",
            False,
            (1940, None),
            SortField.TITLE,
            SortOrder.DESCENDING,
            None,
            10,
            0,
            "ABC",
        ),
        ("hello", True, None, SortField.DATE, SortOrder.ASCENDING, None, 15, 5, None),
        ("world", True, (1940, 1960), None, SortOrder.DESCENDING, None, 10, 0, "ABC"),
    ],
)
def test_create_vespa_search_params(
    test_db,
    query_string,
    exact_match,
    year_range,
    sort_field,
    sort_order,
    keyword_filters,
    limit,
    offset,
    continuation_token,
):
    db_setup(test_db)

    search_request_body = SearchRequestBody(
        query_string=query_string,
        exact_match=exact_match,
        max_passages_per_doc=10,
        keyword_filters=keyword_filters,
        year_range=year_range,
        sort_field=sort_field,
        sort_order=sort_order,
        include_results=None,
        limit=limit,
        offset=offset,
        continuation_token=continuation_token,
    )

    # First step, just make sure we can create a validated pydantic model
    produced_search_parameters = create_vespa_search_params(
        test_db, search_request_body
    )

    # Test constant values
    assert produced_search_parameters.limit == VESPA_SEARCH_LIMIT
    assert (
        produced_search_parameters.max_hits_per_family == VESPA_SEARCH_MATCHES_PER_DOC
    )

    # Test simple passthrough data first
    assert produced_search_parameters.continuation_token == continuation_token
    assert produced_search_parameters.year_range == year_range
    assert produced_search_parameters.query_string == query_string
    assert produced_search_parameters.exact_match == exact_match

    # Test converted data
    assert produced_search_parameters.keyword_filters == _convert_filters(
        test_db, keyword_filters
    )
    assert produced_search_parameters.sort_by == _convert_sort_field(sort_field)
    assert produced_search_parameters.sort_order == _convert_sort_order(sort_order)


def _get_expected_countries(db: Session, slugs: Sequence[str]) -> Sequence[str]:
    geographies = db.query(Geography).filter(Geography.slug.in_(slugs)).all()

    geo_names = []
    for geo in geographies:
        is_region = geo.parent_id is None
        if is_region:
            countries = db.query(Geography).filter(Geography.parent_id == geo.id).all()
            geo_names.extend(c.value for c in countries)
        else:
            geo_names.append(geo.value)

    return geo_names


@pytest.mark.parametrize(
    "filters",
    [
        None,
        {FilterField.CATEGORY: ["Executive"]},
        {FilterField.LANGUAGE: ["english"]},
        {FilterField.SOURCE: ["CCLW"]},
        {FilterField.REGION: ["europe-central-asia"]},
        {FilterField.COUNTRY: ["france", "germany"]},
        {FilterField.REGION: ["south_america"], FilterField.COUNTRY: ["france"]},
    ],
)
def test__convert_filters(test_db, filters):
    db_setup(test_db)
    converted_filters = _convert_filters(test_db, filters)

    if filters is None:
        assert converted_filters is None

    if filters is not None:
        assert converted_filters is not None

    if converted_filters is not None:
        assert set(converted_filters.keys()).issubset(filter_fields)

        expected_languages = filters.get(FilterField.LANGUAGE)
        expected_categories = filters.get(FilterField.CATEGORY)
        expected_sources = filters.get(FilterField.SOURCE)
        region_slugs = filters.get(FilterField.REGION, [])
        country_slugs = filters.get(FilterField.COUNTRY, [])
        geo_slugs = region_slugs + country_slugs
        if geo_slugs:
            expected_countries = _get_expected_countries(test_db, geo_slugs)
            assert expected_countries
        else:
            expected_countries = None

        assert expected_countries == converted_filters.get("geography")
        assert expected_languages == converted_filters.get("language")
        assert expected_sources == converted_filters.get("source")
        assert expected_categories == converted_filters.get("category")
