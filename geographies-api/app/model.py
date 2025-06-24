from enum import Enum
from typing import Generic, Optional, TypeVar

from pydantic import BaseModel
from pydantic_settings import BaseSettings
from sqlmodel import SQLModel

APIDataType = TypeVar("APIDataType")


class APIResponse(SQLModel, Generic[APIDataType]):
    data: list[APIDataType]
    total: int
    page: int
    page_size: int


class Settings(BaseSettings):
    # @related: GITHUB_SHA_ENV_VAR
    github_sha: str = "unknown"


class Geography(SQLModel):
    id: int


class GeographyDocumentCount(SQLModel):
    alpha3: str
    name: str
    count: int


class RegionType(Enum):
    WORLD_BANK_REGION = "World Bank Region"


class RegionResponse(BaseModel):
    name: str
    type: RegionType
    slug: str


class CountryResponse(BaseModel):
    alpha_2: str
    alpha_3: str
    name: str
    official_name: Optional[str] = None
    numeric: str
    flag: Optional[str] = None


class SubdivisionResponse(BaseModel):
    code: str
    name: str
    type: str
    country_alpha_2: str
    country_alpha_3: str
