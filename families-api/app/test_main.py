from datetime import datetime

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import Session, SQLModel

from .main import (
    APIItemResponse,
    Corpus,
    Family,
    FamilyDocument,
    FamilyEvent,
    FamilyMetadata,
    FamilyPublic,
    Geography,
    Organisation,
    PhysicalDocument,
    Slug,
    app,
    get_session,
    settings,
)


# Mostly inspired by
# @see: https://sqlmodel.tiangolo.com/tutorial/fastapi/tests/
@pytest.fixture(scope="session")
def engine():
    engine = create_engine(settings.navigator_database_url)
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


def test_read_family_200(client: TestClient, session: Session):
    organisation = Organisation(id=123, name="Test Org")
    corpus = Corpus(
        import_id="corpus_1",
        title="Test Corpus",
        organisation=organisation,
        organisation_id=organisation.id,
        corpus_type_name="Intl. agreements",
    )
    physical_document = PhysicalDocument(
        id=123,
        title="Test Physical Document",
        source_url="https://example.com/test-physical-document",
        md5_sum="test_md5_sum",
        cdn_object="https://cdn.example.com/test-physical-document",
        content_type="application/pdf",
    )
    family_document = FamilyDocument(
        import_id="family_document_1",
        variant_name="MAIN",
        family_import_id="family_123",
        physical_document_id=123,
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
                import_id="family_document_event_1",
                title="Family event 1",
                date=datetime.fromisoformat("2016-12-01T00:00:00+00:00"),
                event_type_name="Passed/Approved",
                status="OK",
            ),
            FamilyEvent(
                import_id="family_document_event_2",
                title="Family event 2",
                date=datetime.fromisoformat("2023-11-17T00:00:00+00:00"),
                event_type_name="Updated",
                status="OK",
            ),
        ],
    )
    family = Family(
        title="Test family",
        import_id="family_123",
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
                name="test-family",
                family_import_id="family_123",
                family_document_import_id=None,
            )
        ],
        unparsed_geographies=[
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
        ],
        family_category="Legislative",
        unparsed_metadata=FamilyMetadata(
            family_import_id="family_123",
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
                import_id="event_1",
                title="Law passed",
                date=datetime.fromisoformat("2016-12-01T00:00:00+00:00"),
                event_type_name="Passed/Approved",
                status="OK",
            ),
            FamilyEvent(
                import_id="event_2",
                title="National Implementation Programme on Climate Adaptation (NUPKA)",
                date=datetime.fromisoformat("2023-11-17T00:00:00+00:00"),
                event_type_name="Updated",
                status="OK",
            ),
        ],
    )
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
