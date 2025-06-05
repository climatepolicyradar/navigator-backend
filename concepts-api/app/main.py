import logging
import os
from contextlib import asynccontextmanager
from typing import Any

import duckdb
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings

from .telemetry import Telemetry
from .telemetry_config import ServiceManifest, TelemetryConfig

# Global connection variable
conn = None

_LOGGER = logging.getLogger(__name__)

# Open Telemetry
ENV = os.getenv("ENV", "development")
os.environ["OTEL_PYTHON_LOG_CORRELATION"] = "True"
try:
    otel_config = TelemetryConfig.from_service_manifest(
        ServiceManifest.from_file("service-manifest.json"), ENV, "0.1.0"
    )
except Exception as _:
    _LOGGER.error("Failed to load service manifest, using defaults")
    otel_config = TelemetryConfig(
        service_name="navigator-backend",
        namespace_name="navigator",
        service_version="0.0.0",
        environment=ENV,
    )

telemetry = Telemetry(otel_config)
tracer = telemetry.get_tracer()


def get_db():
    """Get database connection or raise error."""
    if conn is None:
        raise HTTPException(
            status_code=500, detail="Database connection not initialised"
        )
    return conn


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    global conn
    try:
        conn = duckdb.connect("initial-data/concepts.db", read_only=True)
        _LOGGER.info("🔌 Database connection established")
        yield
    except Exception as e:
        _LOGGER.error(f"❌ Database connection failed: {e}")
        raise
    finally:
        # Shutdown
        if conn:
            conn.close()
            _LOGGER.info("🚪 Database connection closed")


class Settings(BaseSettings):
    # @related: GITHUB_SHA_ENV_VAR
    github_sha: str = "unknown"


settings = Settings()


router = APIRouter(
    prefix="/concepts",
)
app = FastAPI(
    title="Concepts API",
    lifespan=lifespan,
    docs_url="/concepts/docs",
    redoc_url="/concepts/redoc",
    openapi_url="/concepts/openapi.json",
)


@router.get("/search")
async def search_concepts(
    q: str | None = None, limit: int = 10, has_classifier: bool = False
):
    db = get_db()
    if not q:
        if has_classifier:
            result = db.execute(
                """
                SELECT
                    wikibase_id,
                    preferred_label,
                    alternative_labels,
                    negative_labels,
                    description,
                    definition,
                    labelled_passages,
                    has_classifier,
                FROM concepts
                WHERE has_classifier = ?
                LIMIT ?
                """,
                [has_classifier, limit],
            )
        else:
            result = db.execute(
                """
                SELECT
                    wikibase_id,
                    preferred_label,
                    alternative_labels,
                    negative_labels,
                    description,
                    definition,
                    labelled_passages,
                    has_classifier,
                FROM concepts
                LIMIT ?
                """,
                [limit],
            )
    else:
        if has_classifier:
            result = db.execute(
                """
                SELECT
                    wikibase_id,
                    preferred_label,
                    alternative_labels,
                    negative_labels,
                    description,
                    definition,
                    labelled_passages,
                    has_classifier,
                FROM concepts
                WHERE preferred_label ILIKE ?
                AND has_classifier = ?
                LIMIT ?
                """,
                [f"{q}%", has_classifier, limit],
            )
        else:
            result = db.execute(
                """
                SELECT
                    wikibase_id,
                    preferred_label,
                    alternative_labels,
                    negative_labels,
                    description,
                    definition,
                    labelled_passages,
                    has_classifier,
                FROM concepts
                WHERE preferred_label ILIKE ?
                LIMIT ?
                """,
                [f"{q}%", limit],
            )

    if result.description is not None and (rows := result.fetchall()):
        columns = [desc[0] for desc in result.description]
        return [dict(zip(columns, row, strict=False)) for row in rows]

    raise HTTPException(status_code=404, detail="No results found")


class BatchSearchModel(BaseModel):
    ids: list[str] = Field(Query(default=[]))


@router.get("/batch_search")
async def batch_search_concepts(
    dto: BatchSearchModel = Depends(),
) -> list[dict[Any, Any]]:  # noqa: B008
    _LOGGER.info(f"🔍 Searching for {len(dto.ids)} concepts")
    if not dto.ids:
        raise HTTPException(status_code=400, detail="No IDs provided")

    db = get_db()
    try:
        placeholders = ",".join([f"'{id}'" for id in dto.ids])
        # trunk-ignore(bandit/B608)
        query = f"""
            SELECT
                wikibase_id,
                preferred_label,
                alternative_labels,
                negative_labels,
                description,
                definition,
                labelled_passages,
                has_classifier
            FROM concepts
            WHERE wikibase_id IN ({placeholders})
        """
        result = db.execute(query)
        matches = []
        if (
            result is not None
            and result.description is not None
            and (rows := result.fetchall())
        ):
            columns = [desc[0] for desc in result.description]
            matches = [dict(zip(columns, row, strict=False)) for row in rows]

            # Log missing IDs for debugging
            found_ids = {match["wikibase_id"] for match in matches}
            missing_ids = set(dto.ids) - found_ids

            if missing_ids:
                _LOGGER.warning(f"🕵️ Missing IDs: {missing_ids}")

        return matches

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Database query failed: {str(e)}"
        ) from Exception


@router.get("/{concept_id}")
async def get_concept(concept_id: str):
    # Get column names from description
    db = get_db()

    concept = {}
    result = db.execute(
        """
        SELECT 
            wikibase_id,
            preferred_label,
            alternative_labels,
            negative_labels,
            description,
            definition,
            labelled_passages,
            has_classifier,
        FROM concepts 
        WHERE wikibase_id = ?
    """,
        [concept_id],
    )
    if result is not None and result.description is not None:
        columns = [desc[0] for desc in result.description]
        row = result.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="Concept not found")

        concept = dict(zip(columns, row, strict=False))

    related = []
    related_result = db.execute(
        """
        SELECT c.* FROM concepts c
        JOIN concept_related_relations r 
        ON c.wikibase_id = r.concept_id2 OR c.wikibase_id = r.concept_id1
        WHERE r.concept_id1 = ? OR r.concept_id2 = ?
    """,
        [concept_id, concept_id],
    )
    if related_result is not None and related_result.description is not None:
        related_columns = [desc[0] for desc in related_result.description]
        related = [
            dict(zip(related_columns, r, strict=False))
            for r in related_result.fetchall()
        ]

    subconcepts = []
    subconcepts_result = db.execute(
        """
        SELECT c.* FROM concepts c
        JOIN concept_subconcept_relations r 
        ON c.wikibase_id = r.subconcept_id
        WHERE r.concept_id = ?
    """,
        [concept_id],
    )
    if subconcepts_result is not None and subconcepts_result.description is not None:
        subconcepts_columns = [desc[0] for desc in subconcepts_result.description]
        subconcepts = [
            dict(zip(subconcepts_columns, s, strict=False))
            for s in subconcepts_result.fetchall()
        ]

    return {"concept": concept, "related_concepts": related, "subconcepts": subconcepts}


@router.get("/health")
async def health_check():
    db = get_db()

    try:
        db.execute("SELECT 1").fetchone()
        return {
            "status": "ok",
            # @related: GITHUB_SHA_ENV_VAR
            "version": settings.github_sha,
        }
    except Exception:
        raise HTTPException(
            status_code=500, detail="Database connection failed"
        ) from Exception


app.include_router(router)

# Open Telemetry
telemetry.instrument_fastapi(app)
telemetry.setup_exception_hook()
