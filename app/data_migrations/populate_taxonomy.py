from typing import Callable
from sqlalchemy.orm import Session
from app.data_migrations.taxonomy_cclw import get_cclw_taxonomy
from app.data_migrations.taxonomy_unf3c import get_unf3c_taxonomy

from app.db.models.app.users import Organisation
from app.db.models.law_policy.metadata import MetadataOrganisation, MetadataTaxonomy, FamilyMetadata


def populate_org_taxonomy(
    db: Session,
    org_name: str,
    org_type: str,
    description: str,
    fn_get_taxonomy: Callable,
) -> None:
    """Populates the taxonomy from the data."""

    # First the org
    org = db.query(Organisation).filter(Organisation.name == org_name).one_or_none()

    def add_org():
        new_org = Organisation(
            name=org_name, description=description, organisation_type=org_type
        )
        db.add(new_org)
        db.flush()

    if org is None:
        add_org()
    else:
        if org.organisation_type != org_type or org.description != description:
            db.delete(org)
            add_org()

    metadata_org = (
        db.query(MetadataOrganisation)
        .filter(MetadataOrganisation.organisation_id == org.id)
        .one_or_none()
    )
    metadata_taxonomy = (
        db.query(MetadataTaxonomy)
        .filter(MetadataTaxonomy.id == metadata_org.taxonomy_id)
        .one_or_none()
    )

    if metadata_org is None:
        tax = MetadataTaxonomy(
            description=f"{org_name} loaded values",
            valid_metadata=fn_get_taxonomy(),
        )
        db.add(tax)
        db.flush()

        db.add(
            MetadataOrganisation(
                taxonomy_id=tax.id,
                organisation_id=org.id,
            )
        )
        db.flush()
    else:
        if metadata_taxonomy.valid_metadata != fn_get_taxonomy():
            metadata_taxonomy.valid_metadata = fn_get_taxonomy()


def populate_taxonomy(db: Session) -> None:
    populate_org_taxonomy(
        db,
        org_name="CCLW",
        org_type="Academic",
        description="Climate Change Laws of the World",
        fn_get_taxonomy=get_cclw_taxonomy,
    )
    populate_org_taxonomy(
        db,
        org_name="UNFCCC",
        org_type="UN",
        description="United Nations Framework Convention on Climate Change",
        fn_get_taxonomy=get_unf3c_taxonomy,
    )
