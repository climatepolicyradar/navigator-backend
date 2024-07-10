from typing import Mapping, Sequence

from db_client.functions.corpus_helpers import TaxonomyData
from pydantic import BaseModel


class CorpusData(BaseModel):
    """Contains the Corpus and CorpusType info"""

    corpus_import_id: str
    title: str
    description: str
    corpus_type: str
    corpus_type_description: str
    taxonomy: TaxonomyData
    text: str
    image_url: str


class OrganisationConfig(BaseModel):
    """Definition of stats used on homepage"""

    corpora: Sequence[CorpusData]
    total: int
    count_by_category: Mapping[str, int]


class ApplicationConfig(BaseModel):
    """Definition of the new Config which just includes taxonomy."""

    geographies: Sequence[dict]
    organisations: Mapping[str, OrganisationConfig]
    languages: Mapping[str, str]
    document_types: Sequence[str]
    document_variants: Sequence[str]
