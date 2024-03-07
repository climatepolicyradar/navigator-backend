import random
from dataclasses import dataclass
from datetime import datetime
from slugify import slugify
from typing import Sequence

import pytest
from cpr_data_access.models.search import (
    Document as DataAccessDocument,
    Family as DataAccessFamily,
    Hit as DataAccessHit,
    Passage as DataAccessPassage,
    SearchResponse as DataAccessSearchResponse,
    filter_fields,
)
from sqlalchemy.orm import Session

from app.core.config import VESPA_SEARCH_MATCHES_PER_DOC, VESPA_SEARCH_LIMIT
from app.core.search import (
    FilterField,
    SearchRequestBody,
    SortField,
    SortOrder,
    create_vespa_search_params,
    process_vespa_search_response,
    _convert_filters,
    _convert_sort_field,
    _convert_sort_order,
)
from tests.core.ingestion.helpers import populate_for_ingest


from db_client.models.app.users import Organisation
from db_client.models.document import PhysicalDocument
from db_client.models.law_policy import (
    EventStatus,
    Family,
    FamilyCategory,
    FamilyDocument,
    FamilyEvent,
    FamilyMetadata,
    Geography,
    MetadataOrganisation,
    MetadataTaxonomy,
)
from db_client.models.law_policy.family import DocumentStatus


def db_setup(test_db):
    # Make sure we have geography tables etc populated
    populate_for_ingest(test_db)
    test_db.commit()


# Make sure we cover a decent number of the potential options
@pytest.mark.parametrize(
    (
        "query_string,exact_match,year_range,sort_field,sort_order,"
        "keyword_filters,max_passages,limit,offset,continuation_token"
    ),
    [
        ("hello", True, None, None, SortOrder.ASCENDING, None, 10, 10, 10, None),
        (
            "world",
            True,
            (1940, 1960),
            SortField.TITLE,
            SortOrder.DESCENDING,
            {FilterField.CATEGORY: ["Legislative"], FilterField.REGION: ["europe"]},
            10,
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
            20,
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
            20,
            10,
            0,
            "ABC",
        ),
        (
            "hello",
            True,
            None,
            SortField.TITLE,
            SortOrder.ASCENDING,
            None,
            10,
            10,
            0,
            None,
        ),
        (
            "world",
            True,
            (1940, 1960),
            SortField.DATE,
            SortOrder.DESCENDING,
            None,
            50,
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
            1000,
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
            100,
            10,
            0,
            "ABC",
        ),
        (
            "hello",
            True,
            None,
            SortField.DATE,
            SortOrder.ASCENDING,
            None,
            10,
            15,
            5,
            None,
        ),
        (
            "world",
            True,
            (1940, 1960),
            None,
            SortOrder.DESCENDING,
            None,
            10,
            10,
            0,
            "ABC",
        ),
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
    max_passages,
    limit,
    offset,
    continuation_token,
):
    db_setup(test_db)

    search_request_body = SearchRequestBody(
        query_string=query_string,
        exact_match=exact_match,
        max_passages_per_doc=max_passages,
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
    assert produced_search_parameters.max_hits_per_family == min(
        max_passages, VESPA_SEARCH_MATCHES_PER_DOC
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
            assert len(countries) > 5
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
        {FilterField.COUNTRY: ["cambodia"]},
        {FilterField.REGION: ["south_america"], FilterField.COUNTRY: ["france"]},
    ],
)
def test__convert_filters(test_db, filters):
    db_setup(test_db)
    converted_filters = _convert_filters(test_db, filters)

    if filters in [None, []]:
        assert converted_filters in [None, []]

    if filters not in [None, []]:
        assert converted_filters not in [None, []]

    if converted_filters not in [None, []]:
        assert isinstance(converted_filters, dict)
        assert set(converted_filters.keys()).issubset(filter_fields.values())

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

        assert expected_countries == converted_filters.get(filter_fields["geography"])
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
    family_metadata: dict[str, list[str]]

    description_hit: bool
    family_document_count: int
    document_hit_count: int


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


def _generate_search_response_hits(spec: FamSpec) -> Sequence[DataAccessHit]:
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
            DataAccessDocument(
                family_import_id=spec.family_import_id,
                family_name=spec.family_name,
                family_description=spec.family_description,
                family_source=spec.family_source,
                family_slug=slugify(spec.family_name),
                family_category=spec.family_category,
                family_publication_ts=datetime.fromisoformat(spec.family_ts),
                family_geography=spec.family_geo,
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
            DataAccessPassage(
                family_import_id=spec.family_import_id,
                family_name=spec.family_name,
                family_description=spec.family_description,
                family_source=spec.family_source,
                family_slug=slugify(spec.family_name),
                family_category=spec.family_category,
                family_publication_ts=datetime.fromisoformat(spec.family_ts),
                family_geography=spec.family_geo,
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
                text_block=" ".join(
                    random.sample(spec.family_description.split(" "), 10)
                ),
                text_block_coords=(
                    None
                    if doc_data[document_number]["content_type"] == "text/html"
                    else _generate_coords()
                ),
                text_block_id=f"block_{random.randint(1,15000)}",
                text_block_page=(
                    None
                    if doc_data[document_number]["content_type"] == "text/html"
                    else random.randint(1, 100)
                ),
                text_block_type=random.choice(_BLOCK_TYPES),
            )
        )

    return hits


def _generate_search_response(specs: Sequence[FamSpec]) -> DataAccessSearchResponse:
    families = []
    for fam_spec in specs:
        f = DataAccessFamily(
            id=fam_spec.family_import_id,
            hits=_generate_search_response_hits(fam_spec),
        )
        families.append(f)

    return DataAccessSearchResponse(
        total_hits=len(specs),
        query_time_ms=87 * len(specs),
        total_time_ms=95 * len(specs),
        families=families,
        continuation_token="ABC,XYZ",
    )


_FAM_SPEC_0 = FamSpec(
    random_seed=42,
    family_import_id="CCLW.family.0.0",
    family_source="CCLW",
    family_name="Family name 0",
    family_description="Family description 0 a b c d e f g h i j",
    family_category="Executive",
    family_ts="2023-12-12",
    family_geo="france",
    family_metadata={"keyword": ["Spacial Planning"]},
    description_hit=True,
    family_document_count=1,
    document_hit_count=10,
)
_FAM_SPEC_1 = FamSpec(
    random_seed=142,
    family_import_id="CCLW.family.1.1",
    family_source="CCLW",
    family_name="Family name 1",
    family_description="Family description 1 k l m n o p q r s t",
    family_category="Legislative",
    family_ts="2022-12-25",
    family_geo="spain",
    family_metadata={"sector": ["Urban", "Transportation"], "keyword": ["Hydrogen"]},
    description_hit=False,
    family_document_count=3,
    document_hit_count=25,
)
_FAM_SPEC_2 = FamSpec(
    random_seed=242,
    family_import_id="UNFCCC.family.2.2",
    family_source="UNFCCC",
    family_name="Family name 2",
    family_description="Family description 2 u v w x y z A B C D",
    family_category="UNFCCC",
    family_ts="2019-01-01",
    family_geo="ukraine",
    family_metadata={"author_type": ["Non-Party"], "author": ["Anyone"]},
    description_hit=True,
    family_document_count=5,
    document_hit_count=4,
)
_FAM_SPEC_3 = FamSpec(
    random_seed=342,
    family_import_id="UNFCCC.family.3.3",
    family_source="UNFCCC",
    family_name="Family name 3",
    family_description="Family description 3 E F G H I J K L M N",
    family_category="UNFCCC",
    family_ts="2010-03-14",
    family_geo="norway",
    family_metadata={"author_type": ["Party"], "author": ["Anyone Else"]},
    description_hit=False,
    family_document_count=2,
    document_hit_count=40,
)


def populate_test_db(db: Session, fam_specs: Sequence[FamSpec]) -> None:
    """Minimal population of family structures required for testing conversion below"""
    db_setup(db)

    for fam_spec in fam_specs:
        organisation = (
            db.query(Organisation)
            .filter(Organisation.name == fam_spec.family_source)
            .one()
        )
        family = Family(
            title=fam_spec.family_name,
            import_id=fam_spec.family_import_id,
            description=fam_spec.family_description,
            geography_id=(
                db.query(Geography)
                .filter(Geography.slug == fam_spec.family_geo)
                .one()
                .id
            ),
            family_category=FamilyCategory(fam_spec.family_category),
        )
        db.add(family)
        family_event = FamilyEvent(
            import_id=f"{fam_spec.family_source}.event.{fam_spec.family_import_id.split('.')[2]}.0",
            title="Published",
            date=datetime.fromisoformat(fam_spec.family_ts),
            event_type_name="Passed/Approved",
            family_import_id=fam_spec.family_import_id,
            family_document_import_id=None,
            status=EventStatus.OK,
        )
        db.add(family_event)
        family_metadata = FamilyMetadata(
            family_import_id=fam_spec.family_import_id,
            taxonomy_id=(
                db.query(MetadataOrganisation, MetadataTaxonomy)
                .filter(MetadataOrganisation.organisation_id == organisation.id)
                .join(
                    MetadataTaxonomy,
                    MetadataOrganisation.taxonomy_id == MetadataTaxonomy.id,
                )
                .one()[1]
                .id
            ),
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
                document_type=None,
                document_role=None,
            )
            db.add(family_document)

    db.commit()


@pytest.mark.parametrize(
    "fam_specs,offset,limit",
    [
        # Just one family, one document, 10 hits
        ([_FAM_SPEC_0], 0, 10),
        # Multiple families
        ([_FAM_SPEC_0, _FAM_SPEC_1], 0, 10),
        # Multiple families, mixed results (some docs in FAM_2 must be missing)
        ([_FAM_SPEC_0, _FAM_SPEC_2, _FAM_SPEC_1], 0, 5),
        # Test limit, offset
        ([_FAM_SPEC_3, _FAM_SPEC_1, _FAM_SPEC_2, _FAM_SPEC_0], 2, 1),
    ],
)
def test_process_vespa_search_response(
    test_db: Session,
    fam_specs: Sequence[FamSpec],
    offset: int,
    limit: int,
):
    # Make sure we process a response without error
    populate_test_db(test_db, fam_specs=fam_specs)

    vespa_response = _generate_search_response(fam_specs)
    search_response = process_vespa_search_response(
        db=test_db,
        vespa_search_response=vespa_response,
        limit=limit,
        offset=offset,
    )

    assert len(search_response.families) == min(len(fam_specs), limit)
    assert search_response.query_time_ms == vespa_response.query_time_ms
    assert search_response.total_time_ms == vespa_response.total_time_ms
    assert search_response.continuation_token == vespa_response.continuation_token

    # Now validate family results
    for i, fam_spec in enumerate(fam_specs[offset : offset + limit]):
        search_response_family_i = search_response.families[i]
        assert search_response_family_i.family_slug == slugify(fam_spec.family_name)

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
            assert fd.document_title == f"{fam_spec.family_name} {fd.document_slug[-1]}"
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
