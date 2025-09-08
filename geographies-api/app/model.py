from enum import Enum
from typing import Generic, Literal, Optional, TypeVar

from pydantic import BaseModel, Field, computed_field
from slugify import slugify

APIDataType = TypeVar("APIDataType")


class APIResponse(BaseModel, Generic[APIDataType]):
    data: list[APIDataType]
    total: int
    page: int
    page_size: int


class GeographyBase(BaseModel):
    id: int


class GeographyDocumentCount(BaseModel):
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


# V2 - as per https://www.notion.so/climatepolicyradar/RFC-Geography-API-model-and-route-simplification-24f9109609a4803e8018e1509c86e270GeographyType = Literal["region", "country", "subdivision"]
APIDataType = TypeVar("APIDataType")


class APIListResponse(BaseModel, Generic[APIDataType]):
    data: list[APIDataType]
    total: int
    page: int
    page_size: int


class APIItemResponse(BaseModel, Generic[APIDataType]):
    data: APIDataType


class GeographyType(str, Enum):
    region = "region"
    country = "country"
    subdivision = "subdivision"


Subconcept = TypeVar("Subconcept", bound="GeographyV2Base")


class GeographyV2Base(BaseModel, Generic[Subconcept]):
    id: str
    name: str
    statistics: CountryStatisticsResponse | None

    @property
    def subconcept_of(self) -> list[Subconcept]:
        return []  # default initialisation

    # these are currently different / type while we work out how we want to standardise on this
    @computed_field
    @property
    def slug(self) -> str:
        return f"{self.id.lower()}--{slugify(self.name)}"


class Region(GeographyV2Base):
    type: Literal["region"] = "region"

    @computed_field
    @property
    def slug(self) -> str:
        return f"{slugify(self.name)}"


class Country(GeographyV2Base[Region]):
    type: Literal["country"] = "country"

    alpha_2: str = Field(exclude=True)

    @computed_field
    @property
    def slug(self) -> str:
        return f"{slugify(self.name)}"


class Subdivision(GeographyV2Base[Country]):
    type: Literal["subdivision"] = "subdivision"

    country_code: str = Field(exclude=True)

    @computed_field
    @property
    def slug(self) -> str:
        return f"{slugify(self.id)}"


Geography = Region | Country | Subdivision
