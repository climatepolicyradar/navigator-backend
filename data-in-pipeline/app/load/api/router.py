import logging
from pathlib import Path
from typing import Generic, TypeVar

from fastapi import APIRouter
from pydantic import BaseModel

_LOGGER = logging.getLogger(__name__)

# we always use a path relative to the file as the calling process can come
# from multiple locations
root_dir = Path(__file__).parent.parent

router = APIRouter(prefix="/documents")


APIDataType = TypeVar("APIDataType")


class APIItemResponse(BaseModel, Generic[APIDataType]):
    data: APIDataType


@router.post("/")
def create_document():
    return APIItemResponse(data="")
