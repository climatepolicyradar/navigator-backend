import logging
from typing import TypeVar

from data_in_models.models import Document as DocumentOutput
from data_in_models.models import Label
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from app.repository import (
    check_db_health,
    get_all_documents,
    get_document_by_id,
    select_label,
    select_labels,
)
from app.session import get_db
from app.settings import settings

app = FastAPI(
    title="Data-in API",
    docs_url="/data-in/docs",
    redoc_url="/data-in/redoc",
    openapi_url="/data-in/openapi.json",
)
router = APIRouter(prefix="/data-in")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

APIDataType = TypeVar("APIDataType")

_LOGGER = logging.getLogger(__name__)


class APIListResponse[APIDataType](BaseModel):
    """Generic paginated list response."""

    data: list[APIDataType]
    total: int
    page: int
    page_size: int


class APIItemResponse[APIDataType](BaseModel):
    """Generic single item response."""

    data: APIDataType


@router.get("/")
def root():
    return {"status": "ok"}


# We use both routers to make sure we can have /data-in/health available publicly
# and /health available to the AppRunner health check.
@app.get("/health")
@router.get("/health")
def health():
    return {"status": "ok"}


@router.get("/db-health-check")
def db_health_check(db=Depends(get_db)):
    try:
        is_healthy = check_db_health(db)

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )

    if not is_healthy:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database connection unhealthy",
        )
    return {"status": "ok", "version": settings.github_sha}


@router.get("/documents", response_model=APIListResponse[DocumentOutput])
def list_documents(
    page: int = Query(1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    db=Depends(get_db),
):
    try:
        all_documents = get_all_documents(db, page=page, page_size=page_size)
        total_documents = len(all_documents)

        return APIListResponse(
            data=all_documents, total=total_documents, page=page, page_size=page_size
        )
    except Exception as e:
        _LOGGER.exception(f"Failed to fetch documents: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@router.get("/documents/{document_id}", response_model=APIItemResponse[DocumentOutput])
def get_document(document_id: str, db=Depends(get_db)):
    try:
        document = get_document_by_id(db, document_id)

        if not document:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Document with ID {document_id} not found",
            )

        return APIItemResponse(data=document)
    except HTTPException:
        raise
    except Exception as e:
        _LOGGER.exception(f"Failed to fetch document {document_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@router.get("/labels", response_model=APIListResponse[Label])
def read_labels(
    page: int = Query(1, ge=1),
    page_size: int = Query(default=1000, ge=1, le=1000),
    db=Depends(get_db),
):
    try:
        all_labels = select_labels(db, page=page, page_size=page_size)
        total_labels = len(all_labels)

        return APIListResponse(
            data=all_labels, total=total_labels, page=page, page_size=page_size
        )
    except Exception as e:
        _LOGGER.exception(f"Failed to fetch labels: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@router.get("/labels/{label_id}", response_model=APIItemResponse[Label])
def read_label(label_id: str, db=Depends(get_db)):
    try:
        label = select_label(db, label_id)

        if not label:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Label with ID {label_id} not found",
            )

        return APIItemResponse(data=label)
    except HTTPException:
        raise
    except Exception as e:
        _LOGGER.exception(f"Failed to fetch label {label_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


app.include_router(router)
