"""Shared polyfactory definitions for data-in-pipeline tests.

Use ModelFactory.build(...) with overrides for specific test data; factories
fill the rest with sensible defaults.
"""

from data_in_models.models import DocumentWithoutRelationships
from polyfactory.factories.pydantic_factory import ModelFactory

from app.extract.connectors import (
    NavigatorCollection,
    NavigatorCorpus,
    NavigatorCorpusType,
    NavigatorDocument,
    NavigatorEvent,
    NavigatorFamily,
    NavigatorOrganisation,
    PageFetchFailure,
)
from app.models import ExtractedMetadata


class NavigatorEventFactory(ModelFactory[NavigatorEvent]):
    """Factory for NavigatorEvent. Override import_id, event_type, date as needed."""


class NavigatorDocumentFactory(ModelFactory[NavigatorDocument]):
    """Factory for NavigatorDocument. Override import_id, title, events, etc. as needed."""


class NavigatorCorpusTypeFactory(ModelFactory[NavigatorCorpusType]):
    """Factory for NavigatorCorpusType. Override name as needed."""


class NavigatorOrganisationFactory(ModelFactory[NavigatorOrganisation]):
    """Factory for NavigatorOrganisation. Override id, name as needed."""


class NavigatorCorpusFactory(ModelFactory[NavigatorCorpus]):
    """Factory for NavigatorCorpus. Override import_id, corpus_type, organisation."""


class NavigatorCollectionFactory(ModelFactory[NavigatorCollection]):
    """Factory for NavigatorCollection. Override import_id, title, description."""


class NavigatorFamilyFactory(ModelFactory[NavigatorFamily]):
    """Factory for NavigatorFamily. Override import_id, title, documents, corpus, etc."""


class PageFetchFailureFactory(ModelFactory[PageFetchFailure]):
    """Factory for PageFetchFailure. Override page, error, task_run_id as needed."""


class ExtractedMetadataFactory(ModelFactory[ExtractedMetadata]):
    """Factory for ExtractedMetadata. Override endpoint, http_status as needed."""


class DocumentWithoutRelationshipsFactory(ModelFactory[DocumentWithoutRelationships]):
    """Factory for DocumentWithoutRelationships. Override id, title, labels, items."""
