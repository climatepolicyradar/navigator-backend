from sqlalchemy.orm import Session

from app.db.models.law_policy.metadata import FamilyMetadata, MetadataOrganisation, MetadataTaxonomy
from scripts.ingest_dfc.utils import DfcRow


def add_metadata(db: Session, family_import_id: str, taxonomy: dict, taxonomy_name: str, row: DfcRow):
    metadata = {}
    
    db.add(
        FamilyMetadata(
            family_import_id=family_import_id,
            taxonomy_name=taxonomy_name,
            value=metadata
        )
    )