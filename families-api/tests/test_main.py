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
    Family,
    FamilyDocument,
    FamilyEvent,
    FamilyMetadata,
    FamilyPublic,
    Organisation,
    PhysicalDocument,
    Slug,
)
from app.router import APIItemResponse, APIListResponse
from app.settings import settings


# Base on
# @see: https://docs.pytest.org/en/stable/how-to/fixtures.html#factories-as-fixtures
@pytest.fixture
def make_family():
    def _make_family(id: int):
        organisation = Organisation(id=id, name="Test Org")
        corpus = Corpus(
            import_id=f"corpus_{id}",
            title=f"Test Corpus {id}",
            organisation=organisation,
            organisation_id=organisation.id,
            corpus_type_name="Intl. agreements",
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

        return (corpus, physical_document, family_document, family)

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
    (corpus, family, family_document, physical_document) = make_family(123)

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


def test_read_family_corpus_import_id_filter(
    client: TestClient, session: Session, make_family
):
    (corpus1, family1, family_document1, physical_document1) = make_family(1)
    (corpus2, family2, family_document2, physical_document2) = make_family(2)

    session.add(corpus1)
    session.add(family1)
    session.add(family_document1)
    session.add(physical_document1)

    session.add(corpus2)
    session.add(family2)
    session.add(family_document2)
    session.add(physical_document2)

    session.commit()

    # joins on OR
    response1 = client.get(
        f"/families/?corpus.import_id={corpus1.import_id}&corpus.import_id={corpus2.import_id}"
    )
    assert response1.status_code == 200  # nosec B101
    data1 = APIListResponse[FamilyPublic].model_validate(response1.json())
    assert len(data1.data) == 2

    response2 = client.get(f"/families/?corpus.import_id={corpus1.import_id}")
    data2 = APIListResponse[FamilyPublic].model_validate(response2.json())
    assert len(data2.data) == 1
