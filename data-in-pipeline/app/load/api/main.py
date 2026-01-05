import logging
import sys
from pathlib import Path

from fastapi import FastAPI

# Configure logging before anything else
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
_LOGGER = logging.getLogger(__name__)

# We always use a path relative to the file as the calling process can come
# from multiple locations
root_dir = Path(__file__).parent.parent

_LOGGER.info("🚀 Starting FastAPI application")

try:
    from routers import router
except Exception as e:
    _LOGGER.exception("❌ Failed to import routers: %s", e)
    raise

# Create the FastAPI app
app = FastAPI(
    docs_url="/load/docs",
    redoc_url="/load/redoc",
    openapi_url="/load/openapi.json",
)

# Include router in app
app.include_router(router)
_LOGGER.info("✅ FastAPI application initialised")
