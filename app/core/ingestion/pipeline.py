from datetime import datetime, timezone
from typing import Any, Sequence, Tuple, cast
import logging


from sqlalchemy.orm import Session

from app.api.api_v1.schemas.document import DocumentParserInput
from db_client.models.organisation import Organisation
from db_client.models.dfce.family import (
    Family,
    FamilyDocument,
    FamilyOrganisation,
    Geography,
)
from db_client.models.dfce.metadata import FamilyMetadata


_LOGGER = logging.getLogger(__name__)


def generate_pipeline_ingest_input(db: Session) -> Sequence[DocumentParserInput]:
    """Generates a complete view of the current document database as pipeline input"""
    _LOGGER.info("Running pipeline family query")
    query = (
        db.query(Family, FamilyDocument, FamilyMetadata, Geography, Organisation)
        .join(Family, Family.import_id == FamilyDocument.family_import_id)
        .join(
            FamilyOrganisation, FamilyOrganisation.family_import_id == Family.import_id
        )
        .join(FamilyMetadata, Family.import_id == FamilyMetadata.family_import_id)
        .join(Organisation, Organisation.id == FamilyOrganisation.organisation_id)
        .join(Geography, Geography.id == Family.geography_id)
    )

    query_result = cast(
        Sequence[
            Tuple[Family, FamilyDocument, FamilyMetadata, Geography, Organisation]
        ],
        query.all(),
    )
    fallback_date = datetime(1900, 1, 1, tzinfo=timezone.utc)
    _LOGGER.info("Running pipeline document query")
    documents: Sequence[DocumentParserInput] = [
        DocumentParserInput(
            name=cast(str, family.title),  # All documents in a family indexed by title
            description=cast(str, family.description),
            category=str(family.family_category),
            publication_ts=family.published_date or fallback_date,
            import_id=cast(str, family_document.import_id),
            slug=cast(str, family_document.slugs[-1].name),
            family_import_id=cast(str, family.import_id),
            family_slug=cast(str, family.slugs[-1].name),
            source_url=(
                cast(str, family_document.physical_document.source_url)
                if family_document.physical_document is not None
                else None
            ),
            download_url=None,
            type=cast(str, family_document.document_type or ""),
            source=cast(str, organisation.name),
            geography=cast(str, geography.value),
            languages=[
                cast(str, lang.name)
                for lang in (
                    family_document.physical_document.languages
                    if family_document.physical_document is not None
                    else []
                )
            ],
            metadata=cast(dict[str, Any], family_metadata.value),
        )
        for (
            family,
            family_document,
            family_metadata,
            geography,
            organisation,
        ) in query_result
    ]

    return documents
