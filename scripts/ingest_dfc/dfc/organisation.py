from sqlalchemy.orm import Session
from app.db.models.law_policy.metadata import MetadataOrganisation, MetadataTaxonomy


def get_organisation_taxonomy(db: Session, org_id: int) -> tuple[int, dict]:
    """
    Returns the taxonomy id and its dict representation for an organisation.

    Args:
        db (Session): connection to database
        org_id (int): organisation id

    Raises:
        ValueError: raised when taxonomy not found

    Returns:
        tuple[int, dict]: the taxonomy id and dict value
    """
    taxonomy = (
        db.query(MetadataTaxonomy.id, MetadataTaxonomy.valid_metadata)
        .join(
            MetadataOrganisation,
            MetadataOrganisation.taxonomy_id == MetadataTaxonomy.id,
        )
        .filter_by(organisation_id=org_id)
        .one()
    )
    if not taxonomy:
        raise ValueError(f"Could not find a default taxonomy for organisation {org_id}")

    return taxonomy[0], taxonomy[1]
