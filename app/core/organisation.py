from dataclasses import asdict
from sqlalchemy.orm import Session
from app.api.api_v1.schemas.metadata import TaxonomyData
from app.db.models.app.users import Organisation
from app.db.models.law_policy.family import FamilyEventType
from app.db.models.law_policy.metadata import MetadataOrganisation, MetadataTaxonomy
from app.core.ingestion.types import Taxonomy, TaxonomyEntry


def get_organisation_taxonomy(db: Session, org_id: int) -> tuple[int, Taxonomy]:
    """
    Returns the taxonomy id and its dict representation for an organisation.

    :param Session db: connection to the database
    :param int org_id: organisation id
    :return tuple[int, Taxonomy]: the taxonomy id and the Taxonomy
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


def get_organisation_taxonomy_by_name(db: Session, org_name: str) -> TaxonomyData:
    """
    Returns the TaxonomyConfig for the named organisation

    :param Session db: connection to the database
    :return TaxonomyConfig: the TaxonomyConfig from the db
    """
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
    # Augment the taxonomy with the event_types from the db -
    # TODO: in the future move these into the MetadataTaxonomy
    event_types = db.query(FamilyEventType).all()
    entry = TaxonomyEntry(
        allow_blanks=False,
        allowed_values=[r.name for r in event_types],
        allow_any=False,
    )

    # The above line will throw if there is no taxonomy for the organisation
    return {
        **taxonomy[0],
        "event_types": asdict(entry),
    }
