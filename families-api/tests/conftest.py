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
    Organisation,
    PhysicalDocument,
    Slug,
)
from app.settings import settings


# Based on
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
            attribution_url="https://policyradar.org",
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
            last_modified=datetime.now(),
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
