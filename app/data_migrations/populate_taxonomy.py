from sqlalchemy.orm import Session
from app.data_migrations.taxonomy_cclw import get_cclw_taxonomy

from app.db.models.app.users import Organisation
from app.db.models.law_policy.metadata import MetadataOrganisation, MetadataTaxonomy


def populate_org_taxonomy(
    db: Session, org_name: str, org_type: str, description: str, fn_get_taxonomy
) -> None:
    """Populates the taxonomy from the data."""

    # First the org
    org = db.query(Organisation).filter(Organisation.name == org_name).one_or_none()
    if org is None:
        org = Organisation(
            name=org_name, description=description, organisation_type=org_type
        )
        db.add(org)
        db.flush()

    metadata_org = (
        db.query(MetadataOrganisation)
        .filter(MetadataOrganisation.organisation_id == org.id)
        .one_or_none()
    )
    if metadata_org is None:
        # Now add the taxonomy
        tax = MetadataTaxonomy(
            description=f"{org_name} loaded values",
            valid_metadata=fn_get_taxonomy(),
        )
        db.add(tax)
        db.flush()
        # Finally the link between the org and the taxonomy.
        db.add(
            MetadataOrganisation(
                taxonomy_id=tax.id,
                organisation_id=org.id,
            )
        )
        db.flush()


def populate_taxonomy(db: Session) -> None:
    populate_org_taxonomy(
        db,
        org_name="CCLW",
        org_type="Academic",
        description="Climate Change Laws of the World",
        fn_get_taxonomy=get_cclw_taxonomy,
    )
