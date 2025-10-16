import random
from dataclasses import dataclass
from datetime import datetime
from typing import Mapping, Sequence, Union

import pytest
from cpr_sdk.models.search import Document as CprSdkDocument
from cpr_sdk.models.search import Family as CprSdkFamily
from cpr_sdk.models.search import Filters as CprSdkFilters
from cpr_sdk.models.search import Hit as CprSdkHit
from cpr_sdk.models.search import MetadataFilter
from cpr_sdk.models.search import Passage as CprSdkPassage
from cpr_sdk.models.search import SearchResponse as CprSdkSearchResponse
from cpr_sdk.models.search import filter_fields
from db_client.models.dfce import (
    DocumentStatus,
    EventStatus,
    Family,
    FamilyCategory,
    FamilyDocument,
    FamilyEvent,
    FamilyGeography,
    FamilyMetadata,
    Geography,
)
from db_client.models.document import PhysicalDocument
from slugify import slugify
from sqlalchemy.orm import Session

from app.service.search import (
    SearchRequestBody,
    _convert_filters,
    create_vespa_search_params,
    make_search_request,
    process_vespa_search_response,
)


@pytest.mark.search
@pytest.mark.parametrize(
    (
        "query_string,exact_match,year_range,sort_field,sort_order,"
        "keyword_filters,max_passages,page_size,offset,continuation_tokens,"
        "family_ids,document_ids,metadata_filters,corpus_type_names,corpus_import_ids"
    ),
    [
        # Tests simple exact match query with pagination offset
        (
            "hello",  # query_string
            True,  # exact_match
            None,  # year_range
            None,  # sort_field
            "asc",  # sort_order
            None,  # keyword_filters
            10,  # max_passages
            10,  # page_size
            10,  # offset
            None,  # continuation_tokens
            None,  # family_ids
            None,  # document_ids
            None,  # metadata_filters
            None,  # corpus_type_names
            None,  # corpus_import_ids
        ),
        # Tests exact match with year range, title sorting, and multiple filters
        (
            "world",  # query_string
            True,  # exact_match
            (1940, 1960),  # year_range
            "title",  # sort_field
            "desc",  # sort_order
            {"categories": ["Legislative"], "regions": ["europe"]},  # keyword_filters
            10,  # max_passages
            10,  # page_size
            0,  # offset
            ["ABC"],  # continuation_tokens
            None,  # family_ids
            None,  # document_ids
            None,  # metadata_filters
            None,  # corpus_type_names
            None,  # corpus_import_ids
        ),
        # Tests semantic search with upper year bound and source filtering
        (
            "hello",  # query_string
            False,  # exact_match
            (None, 1960),  # year_range
            "date",  # sort_field
            "asc",  # sort_order
            {"sources": ["UNFCCC"]},  # keyword_filters
            20,  # max_passages
            10,  # page_size
            0,  # offset
            None,  # continuation_tokens
            None,  # family_ids
            None,  # document_ids
            None,  # metadata_filters
            None,  # corpus_type_names
            None,  # corpus_import_ids
        ),
        # Tests semantic search with lower year bound and country/region filters
        (
            "world",  # query_string
            False,  # exact_match
            (1940, None),  # year_range
            None,  # sort_field
            "desc",  # sort_order
            {"countries": ["DEU", "FRA"], "regions": ["europe"]},  # keyword_filters
            20,  # max_passages
            10,  # page_size
            0,  # offset
            ["ABC"],  # continuation_tokens
            None,  # family_ids
            None,  # document_ids
            None,  # metadata_filters
            None,  # corpus_type_names
            None,  # corpus_import_ids
        ),
        # Tests exact match with title-based sorting in ascending order
        (
            "hello",  # query_string
            True,  # exact_match
            None,  # year_range
            "title",  # sort_field
            "asc",  # sort_order
            None,  # keyword_filters
            10,  # max_passages
            10,  # page_size
            0,  # offset
            None,  # continuation_tokens
            None,  # family_ids
            None,  # document_ids
            None,  # metadata_filters
            None,  # corpus_type_names
            None,  # corpus_import_ids
        ),
        # Tests high passage limit with multiple continuation tokens and large page size
        (
            "world",  # query_string
            True,  # exact_match
            (1940, 1960),  # year_range
            "date",  # sort_field
            "desc",  # sort_order
            None,  # keyword_filters
            50,  # max_passages
            100,  # page_size
            10,  # offset
            ["ABC", "ADDD"],  # continuation_tokens
            None,  # family_ids
            None,  # document_ids
            None,  # metadata_filters
            None,  # corpus_type_names
            None,  # corpus_import_ids
        ),
        # Tests semantic search with language filter and specific document targeting
        (
            "hello",  # query_string
            False,  # exact_match
            (None, 1960),  # year_range
            None,  # sort_field
            "asc",  # sort_order
            {"languages": ["english"]},  # keyword_filters
            500,  # max_passages
            10,  # page_size
            0,  # offset
            None,  # continuation_tokens
            None,  # family_ids
            ["CCLW.document.1.0"],  # document_ids
            None,  # metadata_filters
            None,  # corpus_type_names
            None,  # corpus_import_ids
        ),
        # Tests semantic search with specific family targeting and title-based sorting
        (
            "world",  # query_string
            False,  # exact_match
            (1940, None),  # year_range
            "title",  # sort_field
            "desc",  # sort_order
            None,  # keyword_filters
            100,  # max_passages
            10,  # page_size
            0,  # offset
            ["ABC"],  # continuation_tokens
            ["CCLW.executive.1.0"],  # family_ids
            None,  # document_ids
            None,  # metadata_filters
            None,  # corpus_type_names
            None,  # corpus_import_ids
        ),
        # Tests exact match with both family and document filtering, date sorting
        (
            "hello",  # query_string
            True,  # exact_match
            None,  # year_range
            "date",  # sort_field
            "asc",  # sort_order
            None,  # keyword_filters
            10,  # max_passages
            15,  # page_size
            5,  # offset
            None,  # continuation_tokens
            ["CCLW.executive.1.0"],  # family_ids
            ["CCLW.document.1.0", "CCLW.document.2.0"],  # document_ids
            None,  # metadata_filters
            None,  # corpus_type_names
            None,  # corpus_import_ids
        ),
        # Tests exact match with multiple families/documents and continuation tokens
        (
            "world",  # query_string
            True,  # exact_match
            (1940, 1960),  # year_range
            None,  # sort_field
            "desc",  # sort_order
            None,  # keyword_filters
            10,  # max_passages
            10,  # page_size
            0,  # offset
            ["ABC"],  # continuation_tokens
            ["CCLW.executive.1.0", "CCLW.executive.2.0"],  # family_ids
            ["CCLW.document.1.0", "CCLW.document.2.0"],  # document_ids
            None,  # metadata_filters
            None,  # corpus_type_names
            None,  # corpus_import_ids
        ),
        # Tests exact match with metadata filters for sector and topic
        (
            "world",  # query_string
            True,  # exact_match
            None,  # year_range
            None,  # sort_field
            "desc",  # sort_order
            None,  # keyword_filters
            10,  # max_passages
            10,  # page_size
            0,  # offset
            None,  # continuation_tokens
            None,  # family_ids
            None,  # document_ids
            [
                {"name": "family.sector", "value": "Price"},
                {"name": "family.topic", "value": "Mitigation"},
            ],  # metadata_filters
            None,  # corpus_type_names
            None,  # corpus_import_ids
        ),
        # Tests exact match with corpus type name filtering
        (
            "world",  # query_string
            True,  # exact_match
            None,  # year_range
            None,  # sort_field
            "desc",  # sort_order
            None,  # keyword_filters
            10,  # max_passages
            10,  # page_size
            0,  # offset
            None,  # continuation_tokens
            None,  # family_ids
            None,  # document_ids
            None,  # metadata_filters
            ["UNFCCC Submissions", "Laws and Policies"],  # corpus_type_names
            None,  # corpus_import_ids
        ),
        # Tests exact match with specific corpus import ID filtering
        (
            "world",  # query_string
            True,  # exact_match
            None,  # year_range
            None,  # sort_field
            "desc",  # sort_order
            None,  # keyword_filters
            10,  # max_passages
            10,  # page_size
            0,  # offset
            None,  # continuation_tokens
            None,  # family_ids
            None,  # document_ids
            None,  # metadata_filters
            None,  # corpus_type_names
            ["CCLW.corpus.1.0", "CCLW.corpus.2.0"],  # corpus_import_ids
        ),
        # Tests basic semantic search to validate sorting functionality
        (
            "test_sorting",  # query_string
            False,  # exact_match
            None,  # year_range
            None,  # sort_field
            "asc",  # sort_order
            None,  # keyword_filters
            10,  # max_passages
            10,  # page_size
            0,  # offset
            None,  # continuation_tokens
            None,  # family_ids
            None,  # document_ids
            None,  # metadata_filters
            None,  # corpus_type_names
            None,  # corpus_import_ids
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
    metadata_filters,
    corpus_type_names,
    corpus_import_ids,
):
    search_request_body = SearchRequestBody(
        query_string=query_string,
        exact_match=exact_match,
        max_passages_per_doc=max_passages,  # type: ignore
        family_ids=family_ids,
        document_ids=document_ids,
        keyword_filters=keyword_filters,
        year_range=year_range,
        sort_field=sort_field,  # type: ignore
        sort_order=sort_order,
        page_size=page_size,
        offset=offset,
        continuation_tokens=continuation_tokens,
        corpus_type_names=corpus_type_names,
        corpus_import_ids=corpus_import_ids,
        metadata=(
            [MetadataFilter.model_validate(mdata) for mdata in metadata_filters]
            if metadata_filters is not None
            else []
        ),
    )

    # First step, just make sure we can create a validated pydantic model
    produced_search_parameters = create_vespa_search_params(
        data_db, search_request_body
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
            family_geographies=(
                converted_keyword_filters["family_geographies"]
                if "family_geographies" in converted_keyword_filters.keys()
                else []
            ),
        )

    else:
        assert not produced_search_parameters.keyword_filters
        assert not produced_search_parameters.filters

    assert produced_search_parameters.sort_by == sort_field
    assert produced_search_parameters.sort_order == sort_order

    assert produced_search_parameters.metadata == (
        [
            MetadataFilter.model_validate({"name": mdata.name, "value": mdata.value})
            for mdata in produced_search_parameters.metadata
        ]
        if produced_search_parameters.metadata is not None
        else []
    )
    assert corpus_type_names == produced_search_parameters.corpus_type_names
    assert corpus_import_ids == produced_search_parameters.corpus_import_ids


@pytest.mark.search
@pytest.mark.parametrize(
    (
        "exact_match,year_range,sort_field,sort_order,"
        "keyword_filters,max_passages,page_size,offset,continuation_tokens,"
        "family_ids,document_ids,metadata_filters,corpus_type_names,corpus_import_ids"
    ),
    [
        # Tests exact match search with year range, metadata filtering, and continuation tokens
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
            [
                {"name": "family.sector", "value": "Price"},
                {"name": "family.topic", "value": "Mitigation"},
            ],
            None,
            None,
        ),
        # Tests semantic search with geographic filters, document IDs, and corpus type filtering
        (
            False,
            (1940, None),
            None,
            "desc",
            {
                "countries": ["DEU", "fra"],
                "regions": ["europe"],
            },
            20,
            10,
            0,
            ["ABC"],
            ["CCLW.document.1.0", "CCLW.document.2.0"],
            None,
            None,
            ["UNFCCC Submissions", "Laws and Policies"],
            None,
        ),
        # Tests semantic search with geographic filters, family/document IDs, and corpus import IDs
        (
            False,
            (1940, None),
            None,
            "desc",
            {
                "countries": ["DEU", "fra"],
                "regions": ["europe"],
            },
            20,
            10,
            0,
            ["ABC"],
            ["CCLW.executive.1.0", "CCLW.executive.2.0"],
            ["CCLW.document.1.0", "CCLW.document.2.0"],
            None,
            None,
            ["CCLW.corpus.1.0", "CCLW.corpus.2.0"],
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
    metadata_filters,
    corpus_type_names,
    corpus_import_ids,
):
    SearchRequestBody(
        query_string="",
        exact_match=exact_match,
        max_passages_per_doc=max_passages,  # type:ignore
        family_ids=family_ids,
        document_ids=document_ids,
        keyword_filters=keyword_filters,
        year_range=year_range,
        sort_field=sort_field,  # type:ignore
        sort_order=sort_order,
        page_size=page_size,
        offset=offset,
        continuation_tokens=continuation_tokens,
        corpus_type_names=corpus_type_names,
        corpus_import_ids=corpus_import_ids,
        metadata=(
            [MetadataFilter.model_validate(mdata) for mdata in metadata_filters]
            if metadata_filters is not None
            else []
        ),
    )


@pytest.mark.search
@pytest.mark.parametrize(
    "filters, expected",
    [
        # Tests that None filters return None
        (None, None),
        # Tests that empty filter dict returns None
        ({}, None),
        # Tests that empty regions filter returns None
        ({"regions": {}}, None),
        # Tests that valid category filter works even with empty regions
        (
            {"regions": {}, "categories": ["Executive"]},
            {"family_category": ["Executive"]},
        ),
        # Tests that invalid region names return None
        ({"regions": ["this-is-not-a-region"]}, None),
        # Tests that invalid region-country combinations return None
        (
            {
                "regions": ["latin-america-caribbean"],
                "countries": ["FRA"],
            },
            None,
        ),
        # Tests that valid region maps to correct country codes
        ({"regions": ["north-america"]}, {"family_geographies": ["CAN", "USA"]}),
        # Tests that valid region works even with invalid country codes
        # TODO: Reenable this test
        # (
        #     {
        #         "regions": ["north-america"],
        #         "countries": ["not-a-country"],
        #     },
        #     {"family_geographies": ["CAN", "USA"]},
        # ),
        # Tests that region and country filters intersect correctly
        (
            {"regions": ["north-america"], "countries": ["CAN"]},
            {"family_geographies": ["CAN"]},
        ),
        # Tests that valid ISO country codes work
        ({"countries": ["KHM"]}, {"family_geographies": ["KHM"]}),
        # Tests that subdivisions iso codes are mapped to family_geographies
        ({"subdivisions": ["US-CA"]}, {"family_geographies": ["US-CA"]}),
        # Tests that non-existent subdivisions iso codes are not mapped to family_geographies
        ({"subdivisions": ["US-CA", "XX-ZZ-YY"]}, {"family_geographies": ["US-CA"]}),
        # Tests that if selected only subdivisions are mapped to family_geographies
        (
            {
                "countries": ["china", "united-states-of-america", "australia"],
                "subdivisions": ["US-CA", "US-CO", "AU-NSW", "AU-QLD"],
            },
            {
                "family_geographies": [
                    "US-CA",
                    "US-CO",
                    "AU-NSW",
                    "AU-QLD",
                ]
            },
        ),
        # Tests that subdivisions parent countries are not included in family_geographies
        (
            {
                "countries": ["united-states-of-america"],
                "subdivisions": ["US-CA", "US-TX"],
            },
            {"family_geographies": ["US-CA", "US-TX"]},
        ),
        # # Tests that country names (not codes) return None
        # TODO: Reenable this test
        # ({"countries": ["cambodia"]}, None),
        # Tests that invalid country codes return None
        # TODO: Reenable this test
        # ({"countries": ["this-is-not-valid"]}, None),
        # Tests that multiple valid country codes work
        (
            {"countries": ["FRA", "DEU"]},
            {"family_geographies": ["FRA", "DEU"]},
        ),
        # Tests that country and category filters work together
        (
            {"countries": ["KHM"], "categories": ["Executive"]},
            {"family_category": ["Executive"], "family_geographies": ["KHM"]},
        ),
        # Tests that country and language filters work together
        (
            {"countries": ["KHM"], "languages": ["english"]},
            {"document_languages": ["english"], "family_geographies": ["KHM"]},
        ),
        # Tests that country and source filters work together
        (
            {"countries": ["KHM"], "sources": ["CCLW"]},
            {"family_source": ["CCLW"], "family_geographies": ["KHM"]},
        ),
        # Tests that region and category filters work together
        (
            {
                "regions": ["north-america"],
                "categories": ["Executive"],
            },
            {"family_category": ["Executive"], "family_geographies": ["CAN", "USA"]},
        ),
        # Tests that region and language filters work together
        (
            {"regions": ["north-america"], "languages": ["english"]},
            {"document_languages": ["english"], "family_geographies": ["CAN", "USA"]},
        ),
        # Tests that region and source filters work together
        (
            {"regions": ["north-america"], "sources": ["CCLW"]},
            {"family_source": ["CCLW"], "family_geographies": ["CAN", "USA"]},
        ),
        # Tests that category filter works in isolation
        ({"categories": ["Executive"]}, {"family_category": ["Executive"]}),
        # Tests that language filter works in isolation
        ({"languages": ["english"]}, {"document_languages": ["english"]}),
        # Tests that source filter works in isolation
        ({"sources": ["CCLW"]}, {"family_source": ["CCLW"]}),
    ],
)
def test__convert_filters(data_db, filters, expected):
    converted_filters = _convert_filters(data_db, filters)

    if converted_filters and expected:
        # Handle family_geographies field specially - ignore order, check content
        geo_key = filter_fields["geographies"]
        if geo_key in converted_filters and geo_key in expected:
            # Check that the geographies filters contain the same elements regardless of
            # order
            assert set(converted_filters[geo_key]) == set(expected[geo_key])

            # Create copies of the filters WITHOUT the geographies field for the main
            # comparison and check equality directly
            converted_copy = {
                k: v for k, v in converted_filters.items() if k != geo_key
            }
            expected_copy = {k: v for k, v in expected.items() if k != geo_key}
            assert converted_copy == expected_copy
        else:
            # No geographies field, do normal comparison
            assert converted_filters == expected
    else:
        # One or both are None/empty, do normal comparison
        assert converted_filters == expected

    if converted_filters not in [None, []]:
        print("This is the converted filters", converted_filters)
        assert isinstance(converted_filters, dict)
        assert set(converted_filters.keys()).issubset(filter_fields.values())

        expected_languages = filters.get("languages")
        expected_categories = filters.get("categories")
        expected_sources = filters.get("sources")

        assert expected_languages == converted_filters.get(filter_fields["language"])
        assert expected_sources == converted_filters.get(filter_fields["source"])
        assert expected_categories == converted_filters.get(filter_fields["category"])


@dataclass
class FamSpec:
    """Spec used to build family fixtures for tests"""

    random_seed: int
    family_import_id: str
    family_source: str
    family_name: str
    family_description: str
    family_category: str
    family_ts: str
    family_geo: str
    family_geos: list[str]
    family_metadata: dict[str, list[str]]
    corpus_import_id: str
    corpus_type_name: str
    description_hit: bool
    family_document_count: int
    document_hit_count: int
    document_title: str | None = None


_CONTENT_TYPES = ["application/pdf", "text/html"]
_LANGUAGES = ["GBR", "UKR", "ESP", "SVK", "NOR"]
_BLOCK_TYPES = ["Paragraph", "Text", "Heading", "List Element"]


def _generate_coords():
    return [
        (random.randint(0, 871), random.randint(0, 871)),
        (random.randint(0, 871), random.randint(0, 871)),
        (random.randint(0, 871), random.randint(0, 871)),
        (random.randint(0, 871), random.randint(0, 871)),
    ]


def _generate_search_response_hits(spec: FamSpec) -> Sequence[CprSdkHit]:
    random.seed(spec.random_seed)
    doc_data = {}
    hits = []
    if spec.description_hit:
        document_number = random.randint(1, spec.family_document_count)
        doc_data[document_number] = {
            "content_type": random.choice(_CONTENT_TYPES),
            "languages": random.sample(_LANGUAGES, random.randint(1, 3)),
        }
        hits.append(
            CprSdkDocument(
                family_import_id=spec.family_import_id,
                family_name=spec.family_name,
                family_description=spec.family_description,
                family_source=spec.family_source,
                family_slug=slugify(spec.family_name),
                family_category=spec.family_category,
                family_publication_ts=datetime.fromisoformat(spec.family_ts),
                family_geographies=spec.family_geos,
                corpus_import_id=spec.corpus_import_id,
                corpus_type_name=spec.corpus_type_name,
                document_cdn_object=(
                    f"{spec.family_import_id}/{slugify(spec.family_name)}"
                    f"_{document_number}"
                ),
                document_content_type=doc_data[document_number]["content_type"],
                document_import_id=f"{spec.family_import_id}.{document_number}",
                document_languages=doc_data[document_number]["languages"],
                document_slug=f"{slugify(spec.family_name)}_{document_number}",
                document_source_url=(
                    f"https://{spec.family_import_id}/{slugify(spec.family_name)}"
                    f"_{document_number}"
                ),
                document_title=spec.document_title,
            )
        )
    for _ in range(0, spec.document_hit_count):
        document_number = random.randint(1, spec.family_document_count)
        if document_number not in doc_data:
            doc_data[document_number] = {
                "content_type": random.choice(_CONTENT_TYPES),
                "languages": random.sample(_LANGUAGES, random.randint(1, 3)),
            }
        hits.append(
            CprSdkPassage(
                family_import_id=spec.family_import_id,
                family_name=spec.family_name,
                family_description=spec.family_description,
                family_source=spec.family_source,
                family_slug=slugify(spec.family_name),
                family_category=spec.family_category,
                family_publication_ts=datetime.fromisoformat(spec.family_ts),
                family_geographies=spec.family_geos,
                corpus_import_id=spec.corpus_import_id,
                corpus_type_name=spec.corpus_type_name,
                document_cdn_object=(
                    f"{spec.family_import_id}/{slugify(spec.family_name)}"
                    f"_{document_number}"
                ),
                document_content_type=doc_data[document_number]["content_type"],
                document_import_id=f"{spec.family_import_id}.{document_number}",
                document_languages=doc_data[document_number]["languages"],
                document_slug=f"{slugify(spec.family_name)}_{document_number}",
                document_source_url=(
                    f"https://{spec.family_import_id}/{slugify(spec.family_name)}"
                    f"_{document_number}"
                ),
                document_title=spec.document_title,
                text_block=" ".join(
                    random.sample(spec.family_description.split(" "), 10)
                ),
                text_block_coords=(
                    None
                    if doc_data[document_number]["content_type"] == "text/html"
                    else _generate_coords()
                ),
                text_block_id=f"block_{random.randint(1, 15000)}",
                text_block_page=(
                    None
                    if doc_data[document_number]["content_type"] == "text/html"
                    else random.randint(1, 100)
                ),
                text_block_type=random.choice(_BLOCK_TYPES),
            )
        )

    return hits


def _generate_search_response(specs: Sequence[FamSpec]) -> CprSdkSearchResponse:
    families = []
    for fam_spec in specs:
        passage_hits = _generate_search_response_hits(fam_spec)
        f = CprSdkFamily(
            id=fam_spec.family_import_id,
            hits=passage_hits,
            total_passage_hits=(len(passage_hits) * 10),
        )
        families.append(f)

    return CprSdkSearchResponse(
        total_hits=len(specs),
        total_family_hits=(len(families) * 4),
        query_time_ms=87 * len(specs),
        total_time_ms=95 * len(specs),
        families=families,
        continuation_token="ABCXYZ",
        this_continuation_token="",
        prev_continuation_token="ABCDEFXYZ",
    )


_FAM_SPEC_0 = FamSpec(
    random_seed=42,
    family_import_id="CCLW.family.0.0",
    family_source="CCLW",
    family_name="Family name 0",
    family_description="Family description 0 a b c d e f g h i j",
    family_category="Executive",
    family_ts="2023-12-12",
    family_geo="FRA",
    family_geos=["FRA"],
    family_metadata={"keyword": ["Spacial Planning"]},
    corpus_import_id="CCLW.corpus.i00000001.n0000",
    corpus_type_name="Intl. agreements",
    description_hit=True,
    family_document_count=1,
    document_hit_count=10,
    document_title="Family name 0 1",
)
_FAM_SPEC_1 = FamSpec(
    random_seed=142,
    family_import_id="CCLW.family.1.1",
    family_source="CCLW",
    family_name="Family name 1",
    family_description="Family description 1 k l m n o p q r s t",
    family_category="Legislative",
    family_ts="2022-12-25",
    family_geo="ESP",
    family_geos=["ESP"],
    family_metadata={"sector": ["Urban", "Transportation"], "keyword": ["Hydrogen"]},
    corpus_import_id="CCLW.corpus.i00000001.n0000",
    corpus_type_name="Intl. agreements",
    description_hit=False,
    family_document_count=3,
    document_hit_count=25,
    document_title="Family name 1 3",
)
_FAM_SPEC_2 = FamSpec(
    random_seed=242,
    family_import_id="UNFCCC.family.2.2",
    family_source="UNFCCC",
    family_name="Family name 2",
    family_description="Family description 2 u v w x y z A B C D",
    family_category="UNFCCC",
    family_ts="2019-01-01",
    family_geo="UKR",
    family_geos=["UKR"],
    family_metadata={"author_type": ["Non-Party"], "author": ["Anyone"]},
    corpus_import_id="CCLW.corpus.i00000001.n0000",
    corpus_type_name="Intl. agreements",
    description_hit=True,
    family_document_count=5,
    document_hit_count=4,
    document_title="Family name 2 1",
)
_FAM_SPEC_3 = FamSpec(
    random_seed=342,
    family_import_id="UNFCCC.family.3.3",
    family_source="UNFCCC",
    family_name="Family name 3",
    family_description="Family description 3 E F G H I J K L M N",
    family_category="UNFCCC",
    family_ts="2010-03-14",
    family_geo="NOR",
    family_geos=["NOR"],
    family_metadata={"author_type": ["Party"], "author": ["Anyone Else"]},
    corpus_import_id="CCLW.corpus.i00000001.n0000",
    corpus_type_name="Intl. agreements",
    description_hit=False,
    family_document_count=2,
    document_hit_count=40,
    document_title="Family name 3 2",
)


def populate_data_db(db: Session, fam_specs: Sequence[FamSpec]) -> None:
    """Minimal population of family structures required for testing conversion below"""

    for fam_spec in fam_specs:
        family = Family(
            title=fam_spec.family_name,
            import_id=fam_spec.family_import_id,
            description=fam_spec.family_description,
            family_category=FamilyCategory(fam_spec.family_category),
        )
        db.add(family)
        for fam_geo in fam_spec.family_geos:
            db.add(
                FamilyGeography(
                    family_import_id=fam_spec.family_import_id,
                    geography_id=(
                        db.query(Geography).filter(Geography.value == fam_geo).one().id
                    ),
                )
            )
        family_event = FamilyEvent(
            import_id=f"{fam_spec.family_source}.event.{fam_spec.family_import_id.split('.')[2]}.0",
            title="Published",
            date=datetime.fromisoformat(fam_spec.family_ts),
            event_type_name="Passed/Approved",
            family_import_id=fam_spec.family_import_id,
            family_document_import_id=None,
            status=EventStatus.OK,
            valid_metadata={
                "event_type": ["Passed/Approved"],
                "datetime_event_name": ["Passed/Approved"],
            },
        )
        db.add(family_event)
        family_metadata = FamilyMetadata(
            family_import_id=fam_spec.family_import_id,
            value=fam_spec.family_metadata,
        )
        db.add(family_metadata)
        for i in range(1, fam_spec.family_document_count + 1):
            physical_document = PhysicalDocument(
                title=f"{fam_spec.family_name} {i}",
                md5_sum=None,
                cdn_object=None,
                source_url=None,
                content_type=None,
            )
            db.add(physical_document)
            db.flush()
            db.refresh(physical_document)
            family_document = FamilyDocument(
                family_import_id=fam_spec.family_import_id,
                physical_document_id=physical_document.id,
                import_id=f"{fam_spec.family_import_id}.{i}",
                variant_name=None,
                document_status=DocumentStatus.PUBLISHED,
                valid_metadata={"role": ["MAIN"], "type": ["Law"]},
            )
            db.add(family_document)

    db.commit()


@pytest.mark.search
@pytest.mark.parametrize(
    "fam_specs,offset,page_size",
    [
        # Just one family, one document, 10 hits
        ([_FAM_SPEC_0], 0, 10),
        # Multiple families
        ([_FAM_SPEC_0, _FAM_SPEC_1], 0, 10),
        # Multiple families, mixed results (some docs in FAM_2 must be missing)
        ([_FAM_SPEC_0, _FAM_SPEC_2, _FAM_SPEC_1], 0, 5),
        # Test page_size, offset
        ([_FAM_SPEC_3, _FAM_SPEC_1, _FAM_SPEC_2, _FAM_SPEC_0], 2, 1),
    ],
)
def test_process_vespa_search_response(
    data_db: Session,
    fam_specs: Sequence[FamSpec],
    offset: int,
    page_size: int,
):
    # Make sure we process a response without error
    populate_data_db(data_db, fam_specs=fam_specs)

    vespa_response = _generate_search_response(fam_specs)
    search_response = process_vespa_search_response(
        db=data_db,
        vespa_search_response=vespa_response,
        limit=page_size,
        offset=offset,
        sort_within_page=False,
    )

    assert len(search_response.families) == min(len(fam_specs), page_size)
    assert search_response.query_time_ms == vespa_response.query_time_ms
    assert search_response.total_time_ms == vespa_response.total_time_ms
    assert search_response.total_family_hits == vespa_response.total_family_hits
    assert search_response.continuation_token == vespa_response.continuation_token
    assert (
        search_response.this_continuation_token
        == vespa_response.this_continuation_token
    )
    assert (
        search_response.prev_continuation_token
        == vespa_response.prev_continuation_token
    )

    # Now validate family results
    for i, fam_spec in enumerate(fam_specs[offset : offset + page_size]):
        search_response_family_i = search_response.families[i]
        assert search_response_family_i.family_slug == slugify(fam_spec.family_name)

        assert (
            search_response_family_i.total_passage_hits
            == vespa_response.families[i + offset].total_passage_hits
        )

        # Check that we have the correct document details in the response
        expected_document_ids = set(
            hit.document_import_id for hit in vespa_response.families[i + offset].hits
        )
        assert expected_document_ids  # we always expect document ids
        assert len(search_response_family_i.family_documents) == len(
            expected_document_ids
        )

        # Check that the passage match counts are as expected
        assert fam_spec.document_hit_count == sum(
            [
                len(fd.document_passage_matches)
                for fd in search_response_family_i.family_documents
            ]
        )

        # Validate data content
        for fd in search_response_family_i.family_documents:
            assert fd.document_slug.startswith(f"{slugify(fam_spec.family_name)}_")
            assert fd.document_source_url == (
                f"https://{fam_spec.family_import_id}/{fd.document_slug}"
            )
            assert fd.document_title == fam_spec.document_title
            assert fd.document_url is not None
            assert fd.document_url.endswith(
                f"{fam_spec.family_import_id}/{slugify(fam_spec.family_name)}"
                f"_{fd.document_slug[-1]}"
            )
            assert fd.document_url.startswith("https://cdn.climatepolicyradar.org/")

            assert all([pm.text for pm in fd.document_passage_matches])
            assert all([pm.text_block_id for pm in fd.document_passage_matches])
            if fd.document_content_type == "application/pdf":
                assert all(
                    [
                        pm.text_block_page is not None
                        for pm in fd.document_passage_matches
                    ]
                )
                assert all(
                    [
                        pm.text_block_coords is not None
                        for pm in fd.document_passage_matches
                    ]
                )
            else:
                assert all(
                    [pm.text_block_page is None for pm in fd.document_passage_matches]
                )
                assert all(
                    [pm.text_block_coords is None for pm in fd.document_passage_matches]
                )


@pytest.mark.search
@pytest.mark.parametrize(
    "fam_specs,offset,page_size",
    [
        # Just one family, one document, 10 hits
        ([_FAM_SPEC_0], 0, 10),
        # Multiple families
        ([_FAM_SPEC_0, _FAM_SPEC_1], 0, 10),
        # Multiple families, mixed results (some docs in FAM_2 must be missing)
        ([_FAM_SPEC_0, _FAM_SPEC_2, _FAM_SPEC_1], 0, 5),
        # Test page_size, offset
        ([_FAM_SPEC_3, _FAM_SPEC_1, _FAM_SPEC_2, _FAM_SPEC_0], 2, 1),
    ],
)
def test_process_vespa_search_response_sorting(
    data_db: Session,
    fam_specs: Sequence[FamSpec],
    offset: int,
    page_size: int,
    mocker,
):
    """
    Test that passages are sorted by text_block_page and text_block_id
    within each document when sort_within_page is True
    """
    # Mock the text block ID parsing function
    mock_parse_text_block_id = mocker.patch(
        "app.service.search._parse_text_block_id",
        side_effect=lambda x: (0, int(x.split("_")[-1])) if x else (0, 0),
    )

    # Make sure we process a response without error
    populate_data_db(data_db, fam_specs=fam_specs)

    vespa_response = _generate_search_response(fam_specs)
    search_response = process_vespa_search_response(
        db=data_db,
        vespa_search_response=vespa_response,
        limit=page_size,
        offset=offset,
        sort_within_page=True,
    )

    # Verify that passages are sorted by page number first
    for family in search_response.families:
        for document in family.family_documents:
            # Get all passages for this document
            passages = document.document_passage_matches

            # Verify that passages are sorted by page number first
            pages = [
                pm.text_block_page or mock_parse_text_block_id(pm.text_block_id)[0]
                for pm in passages
            ]
            assert pages == sorted(pages, key=lambda page: page or float("inf")), (
                f"Passages in document {document.document_slug} are not sorted by page number"
            )

            # Verify that within each page, passages are sorted by text block ID
            for page in set(pages):
                page_passages = [
                    pm
                    for pm in passages
                    if (
                        pm.text_block_page
                        or mock_parse_text_block_id(pm.text_block_id)[0]
                    )
                    == page
                ]
                if page_passages:
                    block_ids = [
                        mock_parse_text_block_id(pm.text_block_id)[1]
                        for pm in page_passages
                    ]
                    assert block_ids == sorted(block_ids), (
                        f"Passages on page {page} in document {document.document_slug} are not sorted by text block ID"
                    )

            # Verify that the content matches the expected order within this document
            sorted_content = [
                pm.text
                for pm in sorted(
                    passages,
                    key=lambda pm: (
                        pm.text_block_page
                        or mock_parse_text_block_id(pm.text_block_id)[0]
                        or float("inf"),
                        mock_parse_text_block_id(pm.text_block_id)[1],
                    ),
                )
            ]
            actual_content = [pm.text for pm in passages]
            assert sorted_content == actual_content, (
                f"Content order doesn't match page and block order in document {document.document_slug}"
            )


@pytest.mark.search
@pytest.mark.parametrize(
    "fam_specs,offset,page_size",
    [
        # Multiple families
        ([_FAM_SPEC_0, _FAM_SPEC_1], 0, 10),
        # Multiple families, mixed results (some docs in FAM_2 must be missing)
        ([_FAM_SPEC_0, _FAM_SPEC_2, _FAM_SPEC_1], 0, 5),
        # Test page_size, offset
        ([_FAM_SPEC_3, _FAM_SPEC_1, _FAM_SPEC_2, _FAM_SPEC_0], 2, 1),
    ],
)
def test_process_vespa_search_response_sorting_across_all_passages(
    data_db: Session, fam_specs: Sequence[FamSpec], offset: int, page_size: int
):
    """
    Test that passages are NOT sorted across all documents when
    sort_within_page is True
    """
    # Make sure we process a response without error
    populate_data_db(data_db, fam_specs=fam_specs)

    vespa_response = _generate_search_response(fam_specs)
    search_response = process_vespa_search_response(
        db=data_db,
        vespa_search_response=vespa_response,
        limit=page_size,
        offset=offset,
        sort_within_page=True,
    )

    # Verify that passages are sorted within each document
    for family in search_response.families:
        for document in family.family_documents:
            document_pages = [
                pm.text_block_page for pm in document.document_passage_matches
            ]
            assert document_pages == sorted(
                document_pages, key=lambda page: page or float("inf")
            ), (
                f"Passages in document {document.document_slug} are not sorted by page number"
            )

    # Get all passages across all documents
    all_passages = []
    for family in search_response.families:
        for document in family.family_documents:
            all_passages.extend(document.document_passage_matches)

    # Verify that passages are NOT sorted across all documents
    all_pages = [pm.text_block_page for pm in all_passages]
    assert all_pages != sorted(all_pages, key=lambda page: page or float("inf")), (
        "Passages should NOT be sorted across all documents when sort_within_page=True"
    )


@pytest.mark.search
def test_process_vespa_search_response_page_ordering_regression(
    data_db: Session, mocker, test_vespa
):
    """Test that passages are correctly ordered by page number when using numeric text_block_ids."""
    # Create our test data
    test_spec = FamSpec(
        random_seed=42,
        family_import_id="TEST.family.0.0",
        family_source="TEST",
        family_name="Test Family",
        family_description="Test description",
        family_category="Executive",
        family_ts="2023-12-12",
        family_geo="FRA",
        family_geos=["FRA"],
        family_metadata={"keyword": ["Test"]},
        corpus_import_id="TEST.corpus.i00000001.n0000",
        corpus_type_name="Test Type",
        description_hit=True,
        family_document_count=1,
        document_hit_count=4,
    )

    # Populate test data
    populate_data_db(data_db, fam_specs=[test_spec])

    # Create our test passages
    test_passages = [
        # Page 2 passage first
        CprSdkPassage(
            family_import_id=test_spec.family_import_id,
            family_name=test_spec.family_name,
            family_description=test_spec.family_description,
            family_source=test_spec.family_source,
            family_slug=slugify(test_spec.family_name),
            family_category=test_spec.family_category,
            family_publication_ts=datetime.fromisoformat(test_spec.family_ts),
            family_geographies=test_spec.family_geos,
            corpus_import_id=test_spec.corpus_import_id,
            corpus_type_name=test_spec.corpus_type_name,
            document_cdn_object=f"{test_spec.family_import_id}/doc_1",
            document_content_type="application/pdf",
            document_import_id=f"{test_spec.family_import_id}.1",
            document_languages=["english"],
            document_slug="test-doc-1",
            document_source_url="https://example.com/doc1",
            text_block="Page 2 content",
            text_block_id="36",
            text_block_page=1,
            text_block_coords=[(0, 0), (100, 0), (100, 100), (0, 100)],
            text_block_type="Paragraph",
        ),
        # Page 11 passage second
        CprSdkPassage(
            family_import_id=test_spec.family_import_id,
            family_name=test_spec.family_name,
            family_description=test_spec.family_description,
            family_source=test_spec.family_source,
            family_slug=slugify(test_spec.family_name),
            family_category=test_spec.family_category,
            family_publication_ts=datetime.fromisoformat(test_spec.family_ts),
            family_geographies=test_spec.family_geos,
            corpus_import_id=test_spec.corpus_import_id,
            corpus_type_name=test_spec.corpus_type_name,
            document_cdn_object=f"{test_spec.family_import_id}/doc_1",
            document_content_type="application/pdf",
            document_import_id=f"{test_spec.family_import_id}.1",
            document_languages=["english"],
            document_slug="test-doc-1",
            document_source_url="https://example.com/doc1",
            text_block="Page 11 content",
            text_block_id="215",
            text_block_page=11,
            text_block_coords=[(0, 0), (100, 0), (100, 100), (0, 100)],
            text_block_type="Paragraph",
        ),
        # Page 14 passage third
        CprSdkPassage(
            family_import_id=test_spec.family_import_id,
            family_name=test_spec.family_name,
            family_description=test_spec.family_description,
            family_source=test_spec.family_source,
            family_slug=slugify(test_spec.family_name),
            family_category=test_spec.family_category,
            family_publication_ts=datetime.fromisoformat(test_spec.family_ts),
            family_geographies=test_spec.family_geos,
            corpus_import_id=test_spec.corpus_import_id,
            corpus_type_name=test_spec.corpus_type_name,
            document_cdn_object=f"{test_spec.family_import_id}/doc_1",
            document_content_type="application/pdf",
            document_import_id=f"{test_spec.family_import_id}.1",
            document_languages=["english"],
            document_slug="test-doc-1",
            document_source_url="https://example.com/doc1",
            text_block="Page 14 content",
            text_block_id="276",
            text_block_page=14,
            text_block_coords=[(0, 0), (100, 0), (100, 100), (0, 100)],
            text_block_type="Paragraph",
        ),
        # Page 1 passage last
        CprSdkPassage(
            family_import_id=test_spec.family_import_id,
            family_name=test_spec.family_name,
            family_description=test_spec.family_description,
            family_source=test_spec.family_source,
            family_slug=slugify(test_spec.family_name),
            family_category=test_spec.family_category,
            family_publication_ts=datetime.fromisoformat(test_spec.family_ts),
            family_geographies=test_spec.family_geos,
            corpus_import_id=test_spec.corpus_import_id,
            corpus_type_name=test_spec.corpus_type_name,
            document_cdn_object=f"{test_spec.family_import_id}/doc_1",
            document_content_type="application/pdf",
            document_import_id=f"{test_spec.family_import_id}.1",
            document_languages=["english"],
            document_slug="test-doc-1",
            document_source_url="https://example.com/doc1",
            text_block="Page 1 content",
            text_block_id="16",
            text_block_page=0,
            text_block_coords=[(0, 0), (100, 0), (100, 100), (0, 100)],
            text_block_type="Paragraph",
        ),
    ]

    # Mock the search method on the test_vespa instance
    mock_search = mocker.patch.object(test_vespa, "search")
    mock_search.return_value = CprSdkSearchResponse(
        total_hits=1,
        total_family_hits=1,
        query_time_ms=100,
        total_time_ms=110,
        families=[
            CprSdkFamily(
                id=test_spec.family_import_id,
                hits=test_passages,
                total_passage_hits=4,
            )
        ],
        continuation_token=None,
        this_continuation_token="",
        prev_continuation_token=None,
    )

    # Create a search request body that matches the production scenario
    # where page 1 was being ordered at the end of the list of matches.
    search_body = SearchRequestBody(
        query_string="carbon footprint",
        exact_match=False,
        keyword_filters={},
        year_range=(1947, 2025),
        sort_by=None,
        sort_order="desc",
        page_size=20,
        limit=100,
        offset=0,
        corpus_import_ids=[test_spec.corpus_import_id],
        metadata=[],
        concept_filters=[],  # Empty list, not None
        document_ids=[f"{test_spec.family_import_id}"],
        continuation_tokens=[],
        sort_within_page=True,
    )

    response = make_search_request(
        db=data_db, vespa_search_adapter=test_vespa, search_body=search_body
    )

    # Verify that passages are ordered correctly by page number
    assert len(response.families) == 1
    family = response.families[0]
    assert len(family.family_documents) == 1
    document = family.family_documents[0]
    passages = document.document_passage_matches
    assert len(passages) == 4

    # Check that passages are in correct order by page number
    expected_pages = [1, 2, 12, 15]
    actual_pages = [p.text_block_page for p in passages]
    assert actual_pages == expected_pages, (
        f"Expected pages {expected_pages}, got {actual_pages}"
    )

    # Check the actual content matches the expected order
    expected_content = [
        "Page 1 content",
        "Page 2 content",
        "Page 11 content",
        "Page 14 content",
    ]
    actual_content = [p.text for p in passages]
    assert actual_content == expected_content, (
        f"Expected content {expected_content}, got {actual_content}"
    )
