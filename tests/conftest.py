import os
import typing as t
import uuid

import pytest
from fastapi.testclient import TestClient
from moto import mock_s3
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import create_database, database_exists, drop_database

from app.core import security
from app.core.aws import S3Client, get_s3_client
from app.core.search import OpenSearchConnection, OpenSearchConfig
from app.db.models.app import AppUser
from app.db.session import Base, get_db
from app.main import app
from cpr_data_access.search_adaptors import VespaSearchAdapter
from cpr_data_access.search_adaptors import Vespa
from cpr_data_access.embedding import Embedder


@pytest.fixture
def s3_document_bucket_names() -> dict:
    return {
        "queue": os.environ.get("DOCUMENT_BUCKET", "cpr-document-queue"),
    }


@pytest.fixture
def test_s3_client(s3_document_bucket_names):
    bucket_names = s3_document_bucket_names.values()

    with mock_s3():
        s3_client = S3Client()
        for bucket in bucket_names:
            s3_client.client.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={
                    "LocationConstraint": os.getenv("AWS_REGION")
                },
            )

        # Test document in queue for action submission
        s3_client.client.put_object(
            Bucket=s3_document_bucket_names["queue"],
            Key="test_document.pdf",
            Body=bytes(1024),
        )

        yield s3_client


@pytest.fixture(scope="session")
def test_opensearch():
    """Provide a test OpenSearch DB"""
    connection = OpenSearchConnection(
        OpenSearchConfig(
            url=os.environ["OPENSEARCH_URL"],
            username=os.environ["OPENSEARCH_USER"],
            password=os.environ["OPENSEARCH_PASSWORD"],
            index_prefix=f"{os.environ['OPENSEARCH_INDEX_PREFIX']}_test",
        )
    )
    yield connection


@pytest.fixture(scope="session")
def test_vespa():
    """Connect to local vespa instance"""

    def __mocked_init__(self, embedder: t.Optional[Embedder] = None):
        self.client = Vespa(url="http://vespatest", port=8080)
        self.embedder = embedder or Embedder()

    VespaSearchAdapter.__init__ = __mocked_init__

    yield VespaSearchAdapter()


def get_test_db_url() -> str:
    return os.environ["DATABASE_URL"] + f"_test_{uuid.uuid4()}"


@pytest.fixture
def create_test_db():
    """Create a test database and use it for the whole test session."""

    test_db_url = get_test_db_url()

    # Create the test database
    if database_exists(test_db_url):
        drop_database(test_db_url)
    create_database(test_db_url)
    try:
        test_engine = create_engine(test_db_url)
        Base.metadata.create_all(test_engine)  # type: ignore

        # Run the tests
        yield
    finally:
        # Drop the test database
        drop_database(test_db_url)


@pytest.fixture
def test_db(scope="function"):
    """Create a fresh test database for each test."""

    test_db_url = get_test_db_url()

    # Create the test database
    if database_exists(test_db_url):
        drop_database(test_db_url)
    create_database(test_db_url)
    try:
        test_engine = create_engine(test_db_url)
        connection = test_engine.connect()
        Base.metadata.create_all(test_engine)  # type: ignore
        test_session_maker = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=test_engine,
        )
        test_session = test_session_maker()

        # Run the tests
        yield test_session
    finally:
        test_session.close()
        connection.close()
        # Drop the test database
        drop_database(test_db_url)


@pytest.fixture
def client(test_db, test_s3_client):
    """Get a TestClient instance that reads/write to the test database."""

    def get_test_db():
        yield test_db

    def get_test_s3_client():
        yield test_s3_client

    app.dependency_overrides[get_db] = get_test_db
    app.dependency_overrides[get_s3_client] = get_test_s3_client

    yield TestClient(app)


@pytest.fixture
def test_password() -> str:
    return "securepassword"


def get_password_hash() -> str:
    """Password hashing can be expensive so a mock will be much faster"""
    return "supersecrethash"


@pytest.fixture
def test_user(test_db) -> AppUser:
    """Make a test user in the database"""

    app_user = AppUser(
        email="fake@email.com",
        name="Fake User",
        hashed_password=get_password_hash(),
        is_superuser=False,
    )
    test_db.add(app_user)
    test_db.commit()
    return app_user


@pytest.fixture
def test_superuser(test_db) -> AppUser:
    """Superuser for testing"""

    user = AppUser(
        email="fakesuper@email.com",
        name="Fake Super User",
        hashed_password=get_password_hash(),
        is_superuser=True,
    )
    test_db.add(user)
    test_db.commit()
    return user


def verify_password_mock(first: str, second: str) -> bool:
    return True


@pytest.fixture
def superuser_token_headers(
    client: TestClient, test_superuser, test_password, monkeypatch
) -> t.Dict[str, str]:
    monkeypatch.setattr(security, "verify_password", verify_password_mock)

    login_data = {
        "username": test_superuser.email,
        "password": test_password,
    }
    r = client.post("/api/tokens", data=login_data)
    tokens = r.json()
    a_token = tokens["access_token"]
    headers = {"Authorization": f"Bearer {a_token}"}
    return headers
