from typing import Mapping, Sequence, Union

import pytest
from cpr_sdk.models.search import Filters as CprSdkFilters

from app.core.config import VESPA_SEARCH_LIMIT, VESPA_SEARCH_MATCHES_PER_DOC
from app.core.search import (
    SearchRequestBody,
    _convert_filters,
    create_vespa_search_params,
)


# Make sure we cover a decent number of the potential options
@pytest.mark.search
@pytest.mark.parametrize(
    (
        "query_string,exact_match,year_range,sort_field,sort_order,"
        "keyword_filters,max_passages,page_size,offset,continuation_tokens,"
        "family_ids,document_ids"
    ),
    [
        (
            "hello",
            True,
            None,
            None,
            "asc",
            None,
            10,
            10,
            10,
            None,
            None,
            None,
        ),
        (
            "world",
            True,
            (1940, 1960),
            "title",
            "desc",
            {"categories": ["Legislative"], "regions": ["europe"]},
            10,
            10,
            0,
            ["ABC"],
            None,
            None,
        ),
        (
            "hello",
            False,
            (None, 1960),
            "date",
            "asc",
            {"sources": ["UNFCCC"]},
            20,
            10,
            0,
            None,
            None,
            None,
        ),
        (
            "world",
            False,
            (1940, None),
            None,
            "desc",
            {
                "countries": ["germany", "france"],
                "regions": ["europe"],
            },
            20,
            10,
            0,
            ["ABC"],
            None,
            None,
        ),
        (
            "hello",
            True,
            None,
            "title",
            "asc",
            None,
            10,
            10,
            0,
            None,
            None,
            None,
        ),
        (
            "world",
            True,
            (1940, 1960),
            "date",
            "desc",
            None,
            50,
            100,
            10,
            ["ABC", "ADDD"],
            None,
            None,
        ),
        (
            "hello",
            False,
            (None, 1960),
            None,
            "asc",
            {"languages": ["english"]},
            500,
            10,
            0,
            None,
            None,
            ["CCLW.document.1.0"],
        ),
        (
            "world",
            False,
            (1940, None),
            "title",
            "desc",
            None,
            100,
            10,
            0,
            ["ABC"],
            ["CCLW.executive.1.0"],
            None,
        ),
        (
            "hello",
            True,
            None,
            "date",
            "asc",
            None,
            10,
            15,
            5,
            None,
            ["CCLW.executive.1.0"],
            ["CCLW.document.1.0", "CCLW.document.2.0"],
        ),
        (
            "world",
            True,
            (1940, 1960),
            None,
            "desc",
            None,
            10,
            10,
            0,
            ["ABC"],
            ["CCLW.executive.1.0", "CCLW.executive.2.0"],
            ["CCLW.document.1.0", "CCLW.document.2.0"],
        ),
    ],
)
def test_create_vespa_search_params(
    data_db,
    query_string,
    exact_match,
    year_range,
    sort_field,
    sort_order,
    keyword_filters,
    max_passages,
    page_size,
    offset,
    continuation_tokens,
    family_ids,
    document_ids,
):
    search_request_body = SearchRequestBody(
        query_string=query_string,
        exact_match=exact_match,
        # The SearchParameters model provides allows this field as an alias for
        # max_hits_per_family.
        max_passages_per_doc=max_passages,  # type: ignore
        family_ids=family_ids,
        document_ids=document_ids,
        keyword_filters=keyword_filters,
        year_range=year_range,
        # The SearchParameters model provides allows this field as an alias for
        # sort_by.
        sort_field=sort_field,  # type: ignore
        sort_order=sort_order,
        page_size=page_size,
        offset=offset,
        continuation_tokens=continuation_tokens,
    )

    # First step, just make sure we can create a validated pydantic model
    produced_search_parameters = create_vespa_search_params(
        data_db, search_request_body
    )

    # Test constant values
    assert produced_search_parameters.limit == VESPA_SEARCH_LIMIT
    assert produced_search_parameters.max_hits_per_family == min(
        max_passages, VESPA_SEARCH_MATCHES_PER_DOC
    )

    # Test simple passthrough data first
    assert produced_search_parameters.continuation_tokens == continuation_tokens
    assert produced_search_parameters.year_range == year_range
    assert produced_search_parameters.query_string == query_string
    assert produced_search_parameters.exact_match == exact_match

    # Test converted data
    if keyword_filters:
        converted_keyword_filters: Union[Mapping[str, Sequence[str]], None] = (
            _convert_filters(data_db, keyword_filters)
        )
        assert converted_keyword_filters is not None
        assert produced_search_parameters.filters is not None
        assert produced_search_parameters.filters == CprSdkFilters(
            family_geography=(
                converted_keyword_filters["family_geography"]
                if "family_geography" in converted_keyword_filters.keys()
                else []
            ),
            family_category=(
                converted_keyword_filters["family_category"]
                if "family_category" in converted_keyword_filters.keys()
                else []
            ),
            document_languages=(
                converted_keyword_filters["document_languages"]
                if "document_languages" in converted_keyword_filters.keys()
                else []
            ),
            family_source=(
                converted_keyword_filters["family_source"]
                if "family_source" in converted_keyword_filters.keys()
                else []
            ),
        )
    else:
        assert not produced_search_parameters.keyword_filters
        assert not produced_search_parameters.filters

    assert produced_search_parameters.sort_by == sort_field
    assert produced_search_parameters.sort_order == sort_order


@pytest.mark.search
@pytest.mark.parametrize(
    (
        "exact_match,year_range,sort_field,sort_order,"
        "keyword_filters,max_passages,page_size,offset,continuation_tokens,"
        "family_ids,document_ids"
    ),
    [
        (
            True,
            (1940, 1960),
            None,
            "desc",
            None,
            10,
            10,
            0,
            ["ABC"],
            None,
            None,
        ),
        (
            False,
            (1940, None),
            None,
            "desc",
            {
                "countries": ["germany", "France"],
                "regions": ["europe"],
            },
            20,
            10,
            0,
            ["ABC"],
            ["CCLW.document.1.0", "CCLW.document.2.0"],
            None,
        ),
        (
            False,
            (1940, None),
            None,
            "desc",
            {
                "countries": ["germany", "France"],
                "regions": ["europe"],
            },
            20,
            10,
            0,
            ["ABC"],
            ["CCLW.executive.1.0", "CCLW.executive.2.0"],
            ["CCLW.document.1.0", "CCLW.document.2.0"],
        ),
    ],
)
def test_create_browse_request_params(
    exact_match,
    year_range,
    sort_field,
    sort_order,
    keyword_filters,
    max_passages,
    page_size,
    offset,
    continuation_tokens,
    family_ids,
    document_ids,
):
    SearchRequestBody(
        query_string="",
        exact_match=exact_match,
        # The SearchParameters model provides allows this field as an alias for
        # max_hits_per_family.
        max_passages_per_doc=max_passages,  # type:ignore
        family_ids=family_ids,
        document_ids=document_ids,
        keyword_filters=keyword_filters,
        year_range=year_range,
        # The SearchParameters model provides allows this field as an alias for
        # sort_by.
        sort_field=sort_field,  # type:ignore
        sort_order=sort_order,
        page_size=page_size,
        offset=offset,
        continuation_tokens=continuation_tokens,
    )
