import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import Session, SQLModel

from .main import (
    APIItemResponse,
    Corpus,
    Family,
    FamilyPublic,
    Geography,
    Organisation,
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
        geogeraphies=[
            Geography(
                id=1,
                slug="germany",
                value="DE",
                display_value="Germany",
                type="ISO 3166-1",
                parent_id=0,
            ),
            Geography(
                id=2,
                slug="france",
                value="FR",
                display_value="France",
                type="ISO 3166-1",
                parent_id=0,
            ),
        ],
    )
    session.add(corpus)
    session.add(family)
    session.commit()

    response = client.get("/families/family_123")

    # TODO: https://linear.app/climate-policy-radar/issue/APP-735/work-out-a-way-to-ignore-testpy-files-in-bandit
    assert response.status_code == 200  # nosec B101
    response = APIItemResponse[FamilyPublic].model_validate(response.json())
    assert response.data.import_id == "family_123"  # nosec B101
