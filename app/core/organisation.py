from sqlalchemy.orm import Session
from app.api.api_v1.schemas.metadata import TaxonomyConfig
from app.db.models.app.users import Organisation
from app.db.models.law_policy.metadata import MetadataOrganisation, MetadataTaxonomy
from app.core.ingestion.metadata import Taxonomy, TaxonomyEntry


def get_organisation_taxonomy(db: Session, org_id: int) -> tuple[int, Taxonomy]:
    """
    Returns the taxonomy id and its dict representation for an organisation.

    Args:
        db (Session): connection to database
        org_id (int): organisation id

    Raises:
        ValueError: raised when taxonomy not found

    Returns:
        tuple[int, Taxonomy]: the taxonomy id and dict value
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
    # The above line will throw if there is no taxonomy for the organisation

    return taxonomy[0], {k: TaxonomyEntry(**v) for k, v in taxonomy[1].items()}


def get_organisation_taxonomy_by_name(db: Session, org_name: str) -> TaxonomyConfig:
    taxonomy = (
        db.query(MetadataTaxonomy.valid_metadata)
        .join(
            MetadataOrganisation,
            MetadataOrganisation.taxonomy_id == MetadataTaxonomy.id,
        )
        .join(Organisation, Organisation.id == MetadataOrganisation.organisation_id)
        .filter_by(name=org_name)
        .one()
    )
    # The above line will throw if there is no taxonomy for the organisation
    return TaxonomyConfig(
        organisation=org_name,
        taxonomy=taxonomy[0],
    )
