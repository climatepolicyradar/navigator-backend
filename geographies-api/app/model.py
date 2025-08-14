from enum import Enum
from typing import Generic, Optional, TypeVar

from pydantic import BaseModel
from sqlmodel import SQLModel

APIDataType = TypeVar("APIDataType")


class APIResponse(SQLModel, Generic[APIDataType]):
    data: list[APIDataType]
    total: int
    page: int
    page_size: int


class Geography(SQLModel):
    id: int


class GeographyDocumentCount(SQLModel):
    alpha3: str
    name: str
    count: int


class RegionType(Enum):
    WORLD_BANK_REGION = "World Bank Region"
    CPR_CUSTOM_REGION = "Other"


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


class CountryStatisticsResponse(BaseModel):
    name: str
    legislative_process: str
    federal: bool
    federal_details: str
    political_groups: str
    global_emissions_percent: str
    climate_risk_index: str
    worldbank_income_group: str
    visibility_status: str
