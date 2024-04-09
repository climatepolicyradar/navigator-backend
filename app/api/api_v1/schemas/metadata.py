from typing import Mapping, Sequence, Union

from pydantic import BaseModel

TaxonomyData = Mapping[str, Mapping[str, Union[bool, Sequence[str]]]]


class OrganisationConfig(BaseModel):
    """Definition of stats used on homepage"""

    taxonomy: TaxonomyData
    total: int
    count_by_category: Mapping[str, int]


class ApplicationConfig(BaseModel):
    """Definition of the new Config which just includes taxonomy."""

    geographies: Sequence[dict]
    organisations: Mapping[str, OrganisationConfig]
    languages: Mapping[str, str]
    document_roles: Sequence[str]
    document_types: Sequence[str]
    document_variants: Sequence[str]
