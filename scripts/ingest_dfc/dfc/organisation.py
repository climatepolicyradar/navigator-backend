from sqlalchemy.orm import Session
from app.db.models.app.users import Organisation
from app.db.models.law_policy.metadata import MetadataOrganisation, MetadataTaxonomy
from scripts.ingest_dfc.utils import get_or_create


def create_organisation(db: Session):
    def add_default_metadata(org: Organisation):
        db.add(MetadataOrganisation(taxonomy_name="default", organisation_id=org.id))

    print("- Creating organisation")
    organisation = get_or_create(
        db, Organisation, name="CCLW", after_create=add_default_metadata
    )
    return organisation


def get_organisation_taxonomy(db: Session, org_id: int):
    taxonomy = (
        db.query(MetadataTaxonomy.valid_metadata)
        .join(
            MetadataOrganisation,
            MetadataOrganisation.taxonomy_name == MetadataTaxonomy.name,
        )
        .filter_by(taxonomy_name="default", organisation_id=org_id)
        .first()
    )
    if not taxonomy:
        raise ValueError(f"Could not find a default taxonomy for organisation {org_id}")
    return taxonomy[0]
