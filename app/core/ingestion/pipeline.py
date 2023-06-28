from datetime import datetime, timezone
from typing import Sequence, Tuple, cast

from sqlalchemy.orm import Session

from app.api.api_v1.schemas.document import DocumentParserInput
from app.db.models.app.users import Organisation
from app.db.models.law_policy.family import (
    Family,
    FamilyDocument,
    FamilyOrganisation,
    Geography,
)


def generate_pipeline_ingest_input(db: Session) -> Sequence[DocumentParserInput]:
    """Generates a complete view of the current document database as pipeline input"""
    query = (
        db.query(Family, FamilyDocument, Geography, Organisation)
        .join(Family, Family.import_id == FamilyDocument.family_import_id)
        .join(
            FamilyOrganisation, FamilyOrganisation.family_import_id == Family.import_id
        )
        .join(Organisation, Organisation.id == FamilyOrganisation.organisation_id)
        .join(Geography, Geography.id == Family.geography_id)
    )

    query_result = cast(
        Sequence[Tuple[Family, FamilyDocument, Geography, Organisation]], query.all()
    )
    fallback_date = datetime(1900, 1, 1, tzinfo=timezone.utc)
    documents: Sequence[DocumentParserInput] = [
        DocumentParserInput(
            name=cast(str, family.title),  # All documents in a family indexed by title
            description=cast(str, family.description),
            category=str(family.family_category),
            publication_ts=family.published_date or fallback_date,
            import_id=cast(str, family_document.import_id),
            source_url=(
                cast(str, family_document.physical_document.source_url)
                if family_document.physical_document is not None
                else None
            ),
            download_url=None,
            type=cast(str, family_document.document_type or ""),
            source=cast(str, organisation.name),
            slug=cast(str, family_document.slugs[-1].name),
            geography=cast(str, geography.value),
            languages=[
                cast(str, lang.name)
                for lang in (
                    family_document.physical_document.languages
                    if family_document.physical_document is not None
                    else []
                )
            ],
            # TODO: the following are not used & should be removed
            events=[],
            frameworks=[],
            hazards=[],
            instruments=[],
            keywords=[],
            postfix=None,
            sectors=[],
            topics=[],
        )
        for family, family_document, geography, organisation in query_result
    ]

    return documents
