import datetime
import os
import typing as t
import uuid
from contextlib import contextmanager
from typing import Optional

import boto3
import pytest
from cpr_sdk.embedding import Embedder
from cpr_sdk.search_adaptors import Vespa, VespaSearchAdapter
from db_client import run_migrations
from db_client.models import Base
from db_client.models.organisation import AppUser
from fastapi.testclient import TestClient
from freezegun import freeze_time
from moto import mock_s3
from sqlalchemy import create_engine, event
from sqlalchemy.engine import Connection
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import create_database, database_exists, drop_database
from surrogate import surrogate

from app.core import security
from app.core.aws import S3Client, get_s3_client
from app.db.session import get_db
from app.main import app


@pytest.fixture(scope="function")
def mock_aws_creds():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"


@pytest.fixture(scope="function")
def mock_pipeline_bucket(mock_aws_creds):
    pipeline_bucket_name = "test_pipeline_bucket"
    ingest_trigger_root = "input"
    os.environ["PIPELINE_BUCKET"] = pipeline_bucket_name
    os.environ["INGEST_TRIGGER_ROOT"] = ingest_trigger_root

    with mock_s3():
        mock_s3_client = boto3.client("s3", region_name=os.getenv("AWS_REGION"))
        mock_s3_client.create_bucket(
            Bucket=pipeline_bucket_name,
            CreateBucketConfiguration={
                "LocationConstraint": os.getenv("AWS_REGION"),
            },
        )

        test_prefixes = [
            "2024-15-22T21.53.26.945831",
            "2024-01-02T18.10.56.827645",
            "2023-12-10T23.11.27.584565",
            "2023-07-15T14.33.31.783564",
            "2022-11-06T14.57.17.873576",
            "2022-05-03T15.38.21.245423",
        ]
        for prefix in test_prefixes:

            mock_s3_client.put_object(
                Bucket=pipeline_bucket_name,
                Key=f"{ingest_trigger_root}/{prefix}/test_file.txt",
                Body="data".encode(),
            )
        yield pipeline_bucket_name


@pytest.fixture
def s3_document_bucket_names() -> dict:
    return {
        "queue": os.environ.get("DOCUMENT_BUCKET", "cpr-document-queue"),
    }


@pytest.fixture
def test_s3_client(s3_document_bucket_names):
    bucket_names = s3_document_bucket_names.values()

    with mock_s3():
        s3_client = S3Client(dev_mode=False)
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
def test_vespa():
    """Connect to local vespa instance"""

    def __mocked_init__(
        self,
        instance_url: str,
        cert_directory: Optional[str] = None,
        embedder: Optional[Embedder] = None,
    ):
        self.client = Vespa(url=instance_url, port=8080)
        self.embedder = embedder or Embedder()

    VespaSearchAdapter.__init__ = __mocked_init__

    yield VespaSearchAdapter(instance_url="http://vespatest")


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
    """
    Create a fresh test database for each test.

    This will populate the db with the schema in the code (no migrations)
    Therefore it is quick but contains no data.

    Note: use with `test_client`

    """

    test_db_url = get_test_db_url()

    # Create the test database
    if database_exists(test_db_url):
        drop_database(test_db_url)
    create_database(test_db_url)

    test_session = None
    connection = None
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
        if test_session is not None:
            test_session.close()
        if connection is not None:
            connection.close()

        # Drop the test database
        drop_database(test_db_url)


@pytest.fixture(scope="session")
def data_db_connection() -> t.Generator[Connection, None, None]:
    test_db_url = get_test_db_url()

    if database_exists(test_db_url):
        drop_database(test_db_url)
    create_database(test_db_url)

    saved_db_url = os.environ["DATABASE_URL"]
    os.environ["DATABASE_URL"] = test_db_url

    test_engine = create_engine(test_db_url)

    run_migrations(test_engine)
    connection = test_engine.connect()

    yield connection
    connection.close()

    os.environ["DATABASE_URL"] = saved_db_url
    drop_database(test_db_url)


@pytest.fixture(scope="function")
def data_db(data_db_connection):
    transaction = data_db_connection.begin()

    SessionLocal = sessionmaker(
        autocommit=False, autoflush=False, bind=data_db_connection
    )
    session = SessionLocal()

    yield session

    session.close()
    transaction.rollback()


@pytest.fixture
def data_client(data_db, test_s3_client):
    """Get a TestClient instance that reads/write to the data_db database."""

    def get_data_db():
        yield data_db

    def get_test_s3_client():
        yield test_s3_client

    app.dependency_overrides[get_db] = get_data_db
    app.dependency_overrides[get_s3_client] = get_test_s3_client

    yield TestClient(app)


@pytest.fixture
def test_client(test_db, test_s3_client):
    """Get a TestClient instance that reads/write to the test_db database."""

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


@pytest.fixture
def data_superuser(data_db) -> AppUser:
    """Superuser for testing"""

    user = AppUser(
        email="fakesuper@email.com",
        name="Fake Super User",
        hashed_password=get_password_hash(),
        is_superuser=True,
    )
    data_db.add(user)
    data_db.commit()
    return user


def verify_password_mock(first: str, second: str) -> bool:
    return True


@pytest.fixture
def superuser_token_headers(
    test_client: TestClient, test_superuser, test_password, monkeypatch
) -> t.Dict[str, str]:
    monkeypatch.setattr(security, "verify_password", verify_password_mock)

    login_data = {
        "username": test_superuser.email,
        "password": test_password,
    }
    r = test_client.post("/api/tokens", data=login_data)
    tokens = r.json()
    a_token = tokens["access_token"]
    headers = {"Authorization": f"Bearer {a_token}"}
    return headers


@pytest.fixture
def data_superuser_token_headers(
    data_client: TestClient, data_superuser, test_password, monkeypatch
) -> t.Dict[str, str]:
    monkeypatch.setattr(security, "verify_password", verify_password_mock)

    login_data = {
        "username": data_superuser.email,
        "password": test_password,
    }
    r = data_client.post("/api/tokens", data=login_data)
    tokens = r.json()
    a_token = tokens["access_token"]
    headers = {"Authorization": f"Bearer {a_token}"}
    return headers


@pytest.fixture()
def patch_current_time():
    return patch_time


@contextmanager
def patch_time(time_to_freeze, entity):
    with freeze_time(time_to_freeze) as frozen_time:

        def set_initial_timestamp(mapper, connection, target):
            now = datetime.datetime.now(datetime.timezone.utc)

            if hasattr(target, "created"):
                target.created = now
            if hasattr(target, "last_modified"):
                target.last_modified = now

        event.listen(entity, "before_insert", set_initial_timestamp, propagate=True)
        yield frozen_time
        event.remove(entity, "before_insert", set_initial_timestamp)


@pytest.fixture(scope="session", autouse=True)
def stub_freezegun_dynamic_imports():
    """Needed for patch_time to work due to freezegun imports."""
    with surrogate("transformers.models.deprecated.open_llama.tokenization_open_llama"):
        with surrogate(
            "transformers.models.deprecated.open_llama.tokenization_open_llama_fast"
        ):
            with surrogate("transformers.models.open_llama.tokenization_open_llama"):
                with surrogate(
                    "transformers.models.open_llama.tokenization_open_llama_fast"
                ):
                    yield
