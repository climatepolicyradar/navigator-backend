from fastapi import Request
from fastapi.middleware.cors import CORSMiddleware

from app.clients.db.session import SessionLocal
from app.main import app

_ALLOW_ORIGIN_REGEX = (
    r"http://localhost:3000|"
    r"https://.+\.climatepolicyradar\.org|"
    r"https://.+\.dev.climatepolicyradar\.org|"
    r"https://.+\.sandbox\.climatepolicyradar\.org|"
    r"https://climate-laws\.org|"
    r"https://.+\.climate-laws\.org|"
    r"https://climateprojectexplorer\.org|"
    r"https://.+\.climateprojectexplorer\.org"
)

# Add CORS middleware to allow cross origin requests from any port
app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=_ALLOW_ORIGIN_REGEX,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def db_session_middleware(request: Request, call_next):
    request.state.db = SessionLocal()
    response = await call_next(request)
    request.state.db.close()
    return response


@app.middleware("http")
async def app_token_middleware(request: Request, call_next):
    # Skip token validation for health check and root endpoints
    if request.url.path not in [
        "/geographies",  # World map endpoint
        "/documents",  #
        "/searches",  # Search endpoint
        "/searches/download-all-data",  # Whole data download dump endpoint
        "/searches/download-csv",  # This search download endpoint
        "/summaries/geography",  # Geographies page family info endpoint
    ]:
        return await call_next(request)

    # Get the app token from headers and store it in state
    app_token = request.headers.get("app-token")
    request.state.raw_token = app_token

    response = await call_next(request)
    return response
