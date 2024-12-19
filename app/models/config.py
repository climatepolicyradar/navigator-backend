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


class CorpusConfig(BaseModel):
    """Contains the Corpus and Organisation info as well as stats used on homepage"""

    # From corpus
    corpus_import_id: str
    title: str
    description: str
    image_url: str
    text: str
    # From organisation
    organisation_name: str
    organisation_id: int
    # No of families in corpus
    total: int
    count_by_category: Mapping[str, int]


class CorpusTypeConfig(BaseModel):
    """Contains the CorpusType info as well as data of any corpora of that type"""

    corpus_type_name: str
    corpus_type_description: str
    taxonomy: TaxonomyData
    corpora: Sequence[CorpusConfig]


class ApplicationConfig(BaseModel):
    """Definition of the new Config which just includes taxonomy."""

    geographies: Sequence[dict]
    organisations: Mapping[str, OrganisationConfig]
    languages: Mapping[str, str]
    document_variants: Sequence[str]
    corpus_types: Mapping[str, CorpusTypeConfig]
