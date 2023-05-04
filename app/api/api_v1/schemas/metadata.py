from typing import Mapping, Sequence, Union

from pydantic import BaseModel


TaxonomyData = Mapping[str, Mapping[str, Union[str, Sequence[str]]]]


class ApplicationConfig(BaseModel):
    """Definition of the new Config which just includes taxonomy."""

    geographies: list[dict]
    taxonomies: Mapping[str, TaxonomyData]
