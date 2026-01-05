"""
Code for DB session management.

Notes: October 2025.
Stray connection leaks are being caused by services calling get_db()
without closing sessions, particularly via the defensive programming
pattern we were using in the admin service where cleanup
wasn't implemented properly.
"""

import logging
import os

import psycopg2

from app.load.api.aws import get_aws_session, get_ssm_parameter

_LOGGER = logging.getLogger(__name__)

STATEMENT_TIMEOUT = os.getenv("STATEMENT_TIMEOUT", "10000")  # ms
DB_USERNAME = os.getenv("DB_MASTER_USERNAME")
DB_PASSWORD = os.getenv("MANAGED_DB_PASSWORD")
CLUSTER_URL = os.getenv("LOAD_DATABASE_URL")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
SQLALCHEMY_DATABASE_URI = f"postgresql://{DB_USERNAME}:{DB_PASSWORD}@{CLUSTER_URL}:{DB_PORT}/{DB_NAME}?sslmode=no-verify"

# # Engine with connection pooling to prevent connection leaks
# # Lazy initialisation - created once per worker
_engine = create_engine(
    SQLALCHEMY_DATABASE_URI,
    pool_pre_ping=True,  # Verify connections before use
    pool_size=10,  # Base connection pool size
    max_overflow=100,  # Additional connections when pool exhausted
    pool_recycle=1800,  # Recycle connections after 30 minutes
    pool_timeout=30,  # Wait up to 30s for a connection before error
    connect_args={"options": f"-c statement_timeout={STATEMENT_TIMEOUT}"},
)

# # Session factory, exported callable for tests
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=_engine)


def get_db():
    """Get the database session.

    Tries to get a database session. If there is no session, it will
    create one AFTER the uvicorn stuff has started.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


REGION = "eu-west-1"


def test_db_connection():
    """Test database connection using IAM authentication token."""
    session = get_aws_session()
    client = session.client("rds")

    cluster_endpoint = get_ssm_parameter("/data-in-pipeline-load-api/load-database-url")

    token = client.generate_db_auth_token(
        DBHostname=cluster_endpoint,
        Port=DB_PORT,
        DBUsername=DB_USERNAME,
        Region=REGION,
    )

    try:
        conn = psycopg2.connect(
            host=cluster_endpoint,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USERNAME,
            password=token,
            sslrootcert="SSLCERTIFICATE",
        )
        cur = conn.cursor()
        cur.execute("""SELECT now()""")
        query_results = cur.fetchall()
        _LOGGER.info("Database connection successful.Current time: %s", query_results)
        print(query_results)
    except Exception as e:
        _LOGGER.error("Database connection failed due to %s", e)
        print(f"Database connection failed due to {e}")
