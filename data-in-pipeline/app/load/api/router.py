import logging
from pathlib import Path
from typing import Generic, TypeVar

from fastapi import APIRouter, Query
from pydantic import BaseModel

_LOGGER = logging.getLogger(__name__)

# we always use a path relative to the file as the calling process can come
# from multiple locations
root_dir = Path(__file__).parent.parent

router = APIRouter(prefix="/documents")


APIDataType = TypeVar("APIDataType")


class APIListResponse(BaseModel, Generic[APIDataType]):
    data: list[APIDataType]
    total: int
    page: int
    page_size: int


class APIItemResponse(BaseModel, Generic[APIDataType]):
    data: APIDataType


@router.get("/")
def read_documents(
    *,
    # session: Session = Depends(get_session),
    page: int = Query(1, ge=1),
    # page_size: int = Query(
    #     default=10,
    #     ge=1,
    #     le=100,
    # ),
):
    # limit = page_size
    # offset = (page - 1) * limit

    documents = []

    return APIListResponse(
        data=list(documents),
        total=len(documents),
        page=page,
        page_size=len(documents),
    )


@router.get("/{document_id}")
def read_document(*, document_id: str):
    document = {}

    # if document is None:
    #     raise HTTPException(status_code=404, detail="Not found")

    return APIItemResponse(data=document)


@router.post("/")
def create_document():

    return APIItemResponse(data="")


@router.put("/{document_id}")
def update_document(*, document_id: str):

    return APIItemResponse(data=document_id)


@router.delete("/{document_id}")
def delete_document(*, document_id: str):

    return APIItemResponse(data=document_id)
