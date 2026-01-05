from pathlib import Path

from fastapi import FastAPI
from routers import router

# We always use a path relative to the file as the calling process can come
# from multiple locations
root_dir = Path(__file__).parent.parent

# Create the FastAPI app
app = FastAPI(
    docs_url="/load/docs",
    redoc_url="/load/redoc",
    openapi_url="/load/openapi.json",
)

# Include router in app
app.include_router(router)
