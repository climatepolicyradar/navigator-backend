from datetime import datetime

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import Session, SQLModel

from app.database import get_session
from app.main import app
from app.models import (
    Corpus,
    CorpusType,
    Family,
    FamilyDocument,
    FamilyDocumentStatus,
    FamilyEvent,
    FamilyMetadata,
    FamilyPublic,
    Geography,
    Organisation,
    PhysicalDocument,
    Slug,
)
from app.router import APIItemResponse, APIListResponse, GeographyDocumentCount
from app.settings import settings


# Base on
# @see: https://docs.pytest.org/en/stable/how-to/fixtures.html#factories-as-fixtures
@pytest.fixture
def make_family():
    def _make_family(id: int):
        organisation = Organisation(id=id, name="Test Org")
        corpus_type = CorpusType(
            name=f"Corpus type {id}", description=f"Test corpus type {id}"
        )
        corpus = Corpus(
            import_id=f"corpus_{id}",
            title=f"Test Corpus {id}",
            organisation=organisation,
            organisation_id=organisation.id,
            corpus_type_name=corpus_type.name,
        )
        physical_document = PhysicalDocument(
            id=id,
            title=f"Test Physical Document {id}",
            source_url="https://example.com/test-physical-document",
            md5_sum="test_md5_sum",
            cdn_object="https://cdn.example.com/test-physical-document",
            content_type="application/pdf",
        )
        family_document = FamilyDocument(
            import_id=f"family_document_{id}",
            variant_name="MAIN",
            document_status=FamilyDocumentStatus.CREATED,
            family_import_id=f"family_{id}",
            physical_document_id=id,
            valid_metadata={
                "title": "Test Family Document",
                "slug": "test-family-document",
                "corpus": "Test Corpus",
                "corpus_id": "corpus_1",
                "type": "Legislative",
                "status": "Active",
                "language": ["en"],
                "jurisdiction": ["DE", "FR"],
            },
            unparsed_events=[
                FamilyEvent(
                    import_id=f"family_document_event_{id}_1",
                    title=f"Family event {id} 1",
                    date=datetime.fromisoformat("2016-12-01T00:00:00+00:00"),
                    event_type_name="Passed/Approved",
                    status="OK",
                ),
                FamilyEvent(
                    import_id=f"family_document_event_{id}_2",
                    title=f"Family event {id} 2",
                    date=datetime.fromisoformat("2023-11-17T00:00:00+00:00"),
                    event_type_name="Updated",
                    status="OK",
                ),
            ],
        )
        family = Family(
            title=f"Test family {id}",
            import_id=f"family_{id}",
            description="Test family",
            corpus=corpus,
            concepts=[
                {
                    "id": "test concepts 1",
                    "ids": [],
                    "type": "legal_entity",
                    "relation": "jurisdiction",
                    "preferred_label": "test concept 1",
                },
                {
                    "id": "test concepts 2",
                    "ids": [],
                    "type": "legal_entity",
                    "relation": "jurisdiction",
                    "preferred_label": "test concept 2",
                },
            ],
            unparsed_slug=[
                Slug(
                    name=f"test-family-{id}",
                    family_import_id=f"family_{id}",
                    family_document_import_id=None,
                    collection_import_id=None,
                )
            ],
            unparsed_geographies=[],
            family_category="Legislative",
            unparsed_metadata=FamilyMetadata(
                family_import_id=f"family_{id}",
                value={
                    "topic": ["Adaptation"],
                    "hazard": [
                        "Heat Waves And Heat Stress",
                        "Storms",
                        "Floods",
                        "Droughts",
                        "Sea Level Rise",
                    ],
                    "sector": [
                        "Agriculture",
                        "Energy",
                        "Health",
                        "LULUCF",
                        "Tourism",
                        "Transport",
                        "Water",
                    ],
                    "keyword": ["Adaptation"],
                    "framework": ["Adaptation"],
                    "instrument": [
                        "Research & Development, knowledge generation|Information"
                    ],
                },
            ),
            unparsed_events=[
                FamilyEvent(
                    import_id=f"event_{id}_1",
                    title="Law passed",
                    date=datetime.fromisoformat("2016-12-01T00:00:00+00:00"),
                    event_type_name="Passed/Approved",
                    status="OK",
                ),
                FamilyEvent(
                    import_id=f"event_{id}_2",
                    title="National Implementation Programme on Climate Adaptation (NUPKA)",
                    date=datetime.fromisoformat("2023-11-17T00:00:00+00:00"),
                    event_type_name="Updated",
                    status="OK",
                ),
            ],
        )

        return (corpus_type, corpus, family, family_document, physical_document)

    return _make_family


# Mostly inspired by
# @see: https://sqlmodel.tiangolo.com/tutorial/fastapi/tests/
@pytest.fixture(scope="session")
def engine():
    # This gets the database URL for the test-db service from the docker-compose.yml.
    engine = create_engine(settings.navigator_database_url.get_secret_value())
    SQLModel.metadata.create_all(engine)
    yield engine
    SQLModel.metadata.drop_all(engine)


@pytest.fixture
def session(engine):
    connection = engine.connect()
    transaction = connection.begin()

    # Use SQLAlchemy sessionmaker with SQLModel's Session
    SessionLocal = sessionmaker(bind=connection, class_=Session, expire_on_commit=False)
    session = SessionLocal()

    yield session

    session.close()
    transaction.rollback()
    connection.close()


@pytest.fixture(name="client")
def client_fixture(session: Session):
    def get_session_override():
        yield session

    app.dependency_overrides[get_session] = get_session_override
    client = TestClient(app)
    yield client
    app.dependency_overrides.clear()


def test_read_family_404(client: TestClient):
    response = client.get("/families/family_123")
    assert response.status_code == 404  # nosec B101


def test_read_family_200(client: TestClient, session: Session, make_family):
    (corpus_type, corpus, family, family_document, physical_document) = make_family(123)

    session.add(corpus_type)
    session.add(corpus)
    session.add(family)
    session.add(family_document)
    session.add(physical_document)

    session.commit()

    response = client.get("/families/family_123")

    # TODO: https://linear.app/climate-policy-radar/issue/APP-735/work-out-a-way-to-ignore-testpy-files-in-bandit
    assert response.status_code == 200  # nosec B101
    response = APIItemResponse[FamilyPublic].model_validate(response.json())
    assert response.data.import_id == "family_123"  # nosec B101


def test_aggregations_by_geography_returns_a_count_of_documents_per_geography(
    client: TestClient, session: Session, make_family
):
    (corpus_type, corpus, family, family_document, physical_document) = make_family(456)
    family.unparsed_geographies = [
        Geography(
            id=1,
            slug="germany",
            value="DE",
            display_value="Germany",
            type="ISO 3166-1",
        ),
    ]

    session.add(corpus_type)
    session.add(corpus)
    session.add(family)
    session.add(family_document)
    session.add(physical_document)
    session.commit()

    response = client.get("/families/aggregations/by-geography")

    assert response.status_code == 200
    response = APIListResponse[GeographyDocumentCount].model_validate(response.json())
    assert response.data == [
        GeographyDocumentCount(code="DE", name="Germany", type="ISO 3166-1", count=1)
    ]


def test_aggregations_by_geography_returns_a_count_for_each_geo_when_document_has_multiple_geos(
    client: TestClient, session: Session, make_family
):
    (corpus_type, corpus, family, family_document, physical_document) = make_family(789)
    family.unparsed_geographies = [
        Geography(
            id=1,
            slug="germany",
            value="DE",
            display_value="Germany",
            type="ISO 3166-1",
        ),
        Geography(
            id=2,
            slug="france",
            value="FR",
            display_value="France",
            type="ISO 3166-1",
        ),
    ]
    session.add(corpus_type)
    session.add(corpus)
    session.add(family)
    session.add(family_document)
    session.add(physical_document)
    session.commit()

    response = client.get("/families/aggregations/by-geography")

    assert response.status_code == 200
    response = APIListResponse[GeographyDocumentCount].model_validate(response.json())
    assert len(response.data) == 2

    for response_count in response.data:
        assert response_count in [
            GeographyDocumentCount(
                code="FR", name="France", type="ISO 3166-1", count=1
            ),
            GeographyDocumentCount(
                code="DE", name="Germany", type="ISO 3166-1", count=1
            ),
        ]


def test_aggregations_by_geography_does_not_include_geographies_without_documents_in_response(
    client: TestClient, session: Session
):
    geography = Geography(
        id=2,
        slug="france",
        value="FR",
        display_value="France",
        type="ISO 3166-1",
    )
    session.add(geography)
    session.commit()

    response = client.get("/families/aggregations/by-geography")

    assert response.status_code == 200
    response = APIListResponse[GeographyDocumentCount].model_validate(response.json())
    assert response.data == []


def test_aggregations_by_geography_filters_response_by_corpus(
    client: TestClient, session: Session, make_family
):
    (corpus_type1, corpus1, family1, family_document1, physical_document1) = (
        make_family(3)
    )
    (corpus_type2, corpus2, family2, family_document2, physical_document2) = (
        make_family(4)
    )

    assert corpus1.import_id != corpus2.import_id
    geography = Geography(
        id=1,
        slug="germany",
        value="DE",
        display_value="Germany",
        type="ISO 3166-1",
    )
    family1.unparsed_geographies = [geography]
    family2.unparsed_geographies = [geography]

    session.add(corpus_type1)
    session.add(corpus1)
    session.add(family1)
    session.add(family_document1)
    session.add(physical_document1)

    session.add(corpus_type2)
    session.add(corpus2)
    session.add(family2)
    session.add(family_document2)
    session.add(physical_document2)

    session.commit()

    response = client.get(
        f"/families/aggregations/by-geography?corpus.import_id={corpus1.import_id}"
    )
    assert response.status_code == 200
    response = APIListResponse[GeographyDocumentCount].model_validate(response.json())
    assert response.data == [
        GeographyDocumentCount(code="DE", name="Germany", type="ISO 3166-1", count=1)
    ]


def test_aggregations_by_geography_filters_response_by_multiple_corpora(
    client: TestClient, session: Session, make_family
):
    (corpus_type1, corpus1, family1, family_document1, physical_document1) = (
        make_family(3)
    )
    (corpus_type2, corpus2, family2, family_document2, physical_document2) = (
        make_family(4)
    )

    assert corpus1.import_id != corpus2.import_id
    geography = Geography(
        id=1,
        slug="germany",
        value="DE",
        display_value="Germany",
        type="ISO 3166-1",
    )
    family1.unparsed_geographies = [geography]
    family2.unparsed_geographies = [geography]

    session.add(corpus_type1)
    session.add(corpus1)
    session.add(family1)
    session.add(family_document1)
    session.add(physical_document1)

    session.add(corpus_type2)
    session.add(corpus2)
    session.add(family2)
    session.add(family_document2)
    session.add(physical_document2)

    session.commit()

    response = client.get(
        f"/families/aggregations/by-geography?corpus.import_id={corpus1.import_id}&corpus.import_id={corpus2.import_id}"
    )
    assert response.status_code == 200
    response = APIListResponse[GeographyDocumentCount].model_validate(response.json())

    assert response.data == [
        GeographyDocumentCount(code="DE", name="Germany", type="ISO 3166-1", count=2),
    ]


def test_aggregations_by_geography_defaults_to_returning_counts_for_all_corpora_when_corpus_filter_not_supplied(
    client: TestClient, session: Session, make_family
):
    (corpus_type1, corpus1, family1, family_document1, physical_document1) = (
        make_family(3)
    )
    (corpus_type2, corpus2, family2, family_document2, physical_document2) = (
        make_family(4)
    )

    assert corpus1.import_id != corpus2.import_id
    geography = Geography(
        id=1,
        slug="germany",
        value="DE",
        display_value="Germany",
        type="ISO 3166-1",
    )
    family1.unparsed_geographies = [geography]
    family2.unparsed_geographies = [geography]

    session.add(corpus_type1)
    session.add(corpus1)
    session.add(family1)
    session.add(family_document1)
    session.add(physical_document1)

    session.add(corpus_type2)
    session.add(corpus2)
    session.add(family2)
    session.add(family_document2)
    session.add(physical_document2)

    session.commit()

    response = client.get("/families/aggregations/by-geography")
    assert response.status_code == 200
    response = APIListResponse[GeographyDocumentCount].model_validate(response.json())

    assert response.data == [
        GeographyDocumentCount(code="DE", name="Germany", type="ISO 3166-1", count=2),
    ]


def test_aggregations_by_geography_returns_200_with_empty_body_when_corpus_does_not_exist(
    client: TestClient, session: Session, make_family
):
    (corpus_type1, corpus1, family1, family_document1, physical_document1) = (
        make_family(3)
    )

    geography = Geography(
        id=1,
        slug="germany",
        value="DE",
        display_value="Germany",
        type="ISO 3166-1",
    )
    family1.unparsed_geographies = [geography]

    session.add(corpus_type1)
    session.add(corpus1)
    session.add(family1)
    session.add(family_document1)
    session.add(physical_document1)
    session.commit()

    response = client.get(
        "/families/aggregations/by-geography?corpus.import_id=invalid"
    )

    assert response.status_code == 200
    assert response.json()["data"] == []


def test_aggregations_by_geography_response_filters_response_by_document_status(
    client: TestClient, session: Session, make_family
):
    (corpus_type1, corpus1, family1, family_document1, physical_document1) = (
        make_family(5)
    )
    (corpus_type2, corpus2, family2, family_document2, physical_document2) = (
        make_family(6)
    )

    geography = Geography(
        id=1,
        slug="germany",
        value="DE",
        display_value="Germany",
        type="ISO 3166-1",
    )
    family1.unparsed_geographies = [geography]
    family2.unparsed_geographies = [geography]

    family_document1.document_status = FamilyDocumentStatus.PUBLISHED
    family_document2.document_status = FamilyDocumentStatus.CREATED

    session.add(corpus_type1)
    session.add(corpus1)
    session.add(family1)
    session.add(family_document1)
    session.add(physical_document1)

    session.add(corpus_type2)
    session.add(corpus2)
    session.add(family2)
    session.add(family_document2)
    session.add(physical_document2)

    session.commit()

    response = client.get(
        "/families/aggregations/by-geography?documents.document_status=published"
    )
    assert response.status_code == 200
    response = APIListResponse[GeographyDocumentCount].model_validate(response.json())
    assert response.data == [
        GeographyDocumentCount(code="DE", name="Germany", type="ISO 3166-1", count=1)
    ]


def test_aggregations_by_geography_response_filters_response_by_multiple_document_statuses(
    client: TestClient, session: Session, make_family
):
    (corpus_type1, corpus1, family1, family_document1, physical_document1) = (
        make_family(7)
    )
    (corpus_type2, corpus2, family2, family_document2, physical_document2) = (
        make_family(8)
    )
    (corpus_type3, corpus3, family3, family_document3, physical_document3) = (
        make_family(9)
    )

    geography = Geography(
        id=1,
        slug="germany",
        value="DE",
        display_value="Germany",
        type="ISO 3166-1",
    )
    family1.unparsed_geographies = [geography]
    family2.unparsed_geographies = [geography]
    family3.unparsed_geographies = [geography]

    family_document1.document_status = FamilyDocumentStatus.PUBLISHED
    family_document2.document_status = FamilyDocumentStatus.DELETED
    family_document3.document_status = FamilyDocumentStatus.CREATED

    session.add(corpus_type1)
    session.add(corpus1)
    session.add(family1)
    session.add(family_document1)
    session.add(physical_document1)

    session.add(corpus_type2)
    session.add(corpus2)
    session.add(family2)
    session.add(family_document2)
    session.add(physical_document2)

    session.add(corpus_type3)
    session.add(corpus3)
    session.add(family3)
    session.add(family_document3)
    session.add(physical_document3)

    session.commit()

    response = client.get(
        "/families/aggregations/by-geography?documents.document_status=published&documents.document_status=created"
    )
    assert response.status_code == 200
    response = APIListResponse[GeographyDocumentCount].model_validate(response.json())
    assert response.data == [
        GeographyDocumentCount(code="DE", name="Germany", type="ISO 3166-1", count=2)
    ]


def test_aggregations_by_geography_defaults_to_returning_counts_for_all_statuses_when_document_status_filter_not_supplied(
    client: TestClient, session: Session, make_family
):
    (corpus_type1, corpus1, family1, family_document1, physical_document1) = (
        make_family(10)
    )
    (corpus_type2, corpus2, family2, family_document2, physical_document2) = (
        make_family(11)
    )

    assert corpus1.import_id != corpus2.import_id
    geography = Geography(
        id=1,
        slug="germany",
        value="DE",
        display_value="Germany",
        type="ISO 3166-1",
    )
    family1.unparsed_geographies = [geography]
    family2.unparsed_geographies = [geography]

    family_document1.document_status = FamilyDocumentStatus.PUBLISHED
    family_document2.document_status = FamilyDocumentStatus.DELETED

    session.add(corpus_type1)
    session.add(corpus1)
    session.add(family1)
    session.add(family_document1)
    session.add(physical_document1)

    session.add(corpus_type2)
    session.add(corpus2)
    session.add(family2)
    session.add(family_document2)
    session.add(physical_document2)

    session.commit()

    response = client.get("/families/aggregations/by-geography")
    assert response.status_code == 200
    response = APIListResponse[GeographyDocumentCount].model_validate(response.json())

    assert response.data == [
        GeographyDocumentCount(code="DE", name="Germany", type="ISO 3166-1", count=2),
    ]


def test_aggregations_by_geography_returns_422_unprocessable_entity_when_document_status_invalid(
    client: TestClient, session: Session, make_family
):
    (corpus_type1, corpus1, family1, family_document1, physical_document1) = (
        make_family(12)
    )

    geography = Geography(
        id=1,
        slug="germany",
        value="DE",
        display_value="Germany",
        type="ISO 3166-1",
    )
    family1.unparsed_geographies = [geography]

    session.add(corpus_type1)
    session.add(corpus1)
    session.add(family1)
    session.add(family_document1)
    session.add(physical_document1)
    session.commit()

    response = client.get(
        "/families/aggregations/by-geography?documents.document_status=invalid"
    )

    assert response.status_code == 422
    assert (
        response.json()["detail"][0]["msg"]
        == "Input should be 'created', 'published' or 'deleted'"
    )


def test_aggregations_by_geography_filters_response_by_corpus_AND_document_status(
    client: TestClient, session: Session, make_family
):
    (corpus_type1, corpus1, family1, family_document1, physical_document1) = (
        make_family(13)
    )
    (corpus_type2, corpus2, family2, family_document2, physical_document2) = (
        make_family(14)
    )
    (_, _, family3, family_document3, physical_document3) = make_family(15)

    geography = Geography(
        id=1,
        slug="germany",
        value="DE",
        display_value="Germany",
        type="ISO 3166-1",
    )
    # make family2 and family3 belong to the same corpus
    family3.corpus = corpus2

    family1.unparsed_geographies = [geography]
    family2.unparsed_geographies = [geography]

    # make documents from family1 and family2 published
    family_document1.document_status = FamilyDocumentStatus.PUBLISHED
    family_document2.document_status = FamilyDocumentStatus.PUBLISHED

    session.add(corpus_type1)
    session.add(corpus1)
    session.add(family1)
    session.add(family_document1)
    session.add(physical_document1)

    session.add(corpus_type2)
    session.add(corpus2)
    session.add(family2)
    session.add(family_document2)
    session.add(physical_document2)

    session.add(family3)
    session.add(family_document3)
    session.add(physical_document3)

    session.commit()

    response = client.get(
        f"/families/aggregations/by-geography?corpus.import_id={corpus2.import_id}&document.document_status=published"
    )
    assert response.status_code == 200
    response = APIListResponse[GeographyDocumentCount].model_validate(response.json())

    # only documents from family2 should be counted
    assert response.data == [
        GeographyDocumentCount(code="DE", name="Germany", type="ISO 3166-1", count=1)
    ]


def test_read_family_corpus_import_id_filter(
    client: TestClient, session: Session, make_family
):
    (corpus_type1, corpus1, family1, family_document1, physical_document1) = (
        make_family(1)
    )
    (corpus_type2, corpus2, family2, family_document2, physical_document2) = (
        make_family(2)
    )

    session.add(corpus_type1)
    session.add(corpus1)
    session.add(family1)
    session.add(family_document1)
    session.add(physical_document1)

    session.add(corpus_type2)
    session.add(corpus2)
    session.add(family2)
    session.add(family_document2)
    session.add(physical_document2)

    session.commit()

    # joins on OR
    expected_family_ids1 = [family1.import_id, family2.import_id]
    response1 = client.get(
        f"/families/?corpus.import_id={corpus1.import_id}&corpus.import_id={corpus2.import_id}"
    )
    assert response1.status_code == 200  # nosec B101
    data1 = APIListResponse[FamilyPublic].model_validate(response1.json())
    assert len(data1.data) == 2
    assert {family.import_id for family in data1.data} == set(expected_family_ids1)

    expected_family_ids2 = [family1.import_id]
    response2 = client.get(f"/families/?corpus.import_id={corpus1.import_id}")
    data2 = APIListResponse[FamilyPublic].model_validate(response2.json())
    assert len(data2.data) == 1
    assert {family.import_id for family in data2.data} == set(expected_family_ids2)
