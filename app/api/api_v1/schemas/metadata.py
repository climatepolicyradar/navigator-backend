from typing import Mapping, Sequence, Union

from pydantic import BaseModel


TaxonomyData = Mapping[str, Mapping[str, Union[bool, Sequence[str]]]]


class ApplicationConfig(BaseModel):
    """Definition of the new Config which just includes taxonomy."""

    geographies: Sequence[dict]
    taxonomies: Mapping[str, TaxonomyData]
    languages: Mapping[str, str]
    document_roles: Sequence[str]
    document_types: Sequence[str]
    document_variants: Sequence[str]
