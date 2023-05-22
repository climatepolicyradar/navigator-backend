from dataclasses import dataclass
from typing import Optional, Callable


@dataclass
class IngestParameters:
    """Agnostic parameters for any ingest."""

    create_collections: bool
    add_metadata: Callable
    source_url: Optional[str]  # get_first_url()
    document_id: str
    collection_name: str
    collection_summary: str
    document_title: str
    family_name: str
    family_summary: str
    document_role: str
    document_variant: str
    geography_iso: str
    documents: str
    category: str
    document_type: str
    language: list[str]
    geography: str
    cpr_document_id: str
    cpr_family_id: str
    cpr_collection_id: str
    cpr_family_slug: str
    cpr_document_slug: str
