import logging
from typing import TypeVar

from fastapi import Depends, FastAPI, HTTPException, Query, status
from pydantic import BaseModel

from app.repository import check_db_health, get_all_documents, get_document_by_id
from app.session import get_db
from app.settings import settings

app = FastAPI(title="DATA IN API")

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


@app.get("/")
def root():
    return {"status": "ok"}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/db-health-check")
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


@app.get("/documents", response_model=APIListResponse[str])
def list_documents(
    page: int = Query(1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    db=Depends(get_db),
):
    try:
        all_documents = get_all_documents(db, page=page, page_size=page_size)
        total_documents = len(all_documents)

        return APIListResponse[str](
            data=all_documents, total=total_documents, page=page, page_size=page_size
        )
    except Exception as e:
        _LOGGER.exception(f"Failed to fetch documents: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@app.get("/documents/{document_id}", response_model=APIItemResponse[str])
def get_document(document_id: str, db=Depends(get_db)):
    try:
        document = get_document_by_id(db, document_id)

        if not document:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Document with ID {document_id} not found",
            )

        return APIItemResponse[str](data=document)
    except HTTPException:
        raise
    except Exception as e:
        _LOGGER.exception(f"Failed to fetch document {document_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )
