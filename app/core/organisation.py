from dataclasses import asdict
from typing import cast
from sqlalchemy.orm import Session
from sqlalchemy import func
from app.api.api_v1.schemas.metadata import OrganisationConfig, TaxonomyData
from db_client.models.organisation import Organisation
from db_client.models.dfce.family import (
    FamilyEventType,
    FamilyOrganisation,
    Family,
    FamilyCategory,
    FamilyCorpus,
    Corpus,
)
from db_client.models.dfce.metadata import MetadataOrganisation, MetadataTaxonomy
from db_client.models.dfce.taxonomy_entry import Taxonomy, TaxonomyEntry


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


def get_organisation_config(db: Session, org: Organisation) -> OrganisationConfig:
    total = (
        db.query(FamilyOrganisation)
        .filter(FamilyOrganisation.organisation_id == org.id)
        .count()
    )

    counts = (
        db.query(Family.family_category, func.count())
        .join(
            FamilyOrganisation,
            Family.import_id == FamilyOrganisation.family_import_id,
        )
        .filter(FamilyOrganisation.organisation_id == org.id)
        .group_by(Family.family_category)
        .all()
    )
    found_categories = {c[0].value: c[1] for c in counts}
    count_by_category = {}

    # Supply zeros when there aren't any
    for category in [e.value for e in FamilyCategory]:
        if category in found_categories.keys():
            count_by_category[category] = found_categories[category]
        else:
            count_by_category[category] = 0

    return OrganisationConfig(
        total=total,
        count_by_category=count_by_category,
        taxonomy=get_organisation_taxonomy_by_name(db, cast(str, org.name)),
    )
