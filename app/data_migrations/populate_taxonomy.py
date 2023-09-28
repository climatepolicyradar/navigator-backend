from typing import Callable
from sqlalchemy.orm import Session
from app.data_migrations.taxonomy_cclw import get_cclw_taxonomy
from app.data_migrations.taxonomy_unf3c import get_unf3c_taxonomy

from app.db.models.app.users import Organisation, OrganisationDatasource
from app.db.models.law_policy.metadata import MetadataOrganisation, MetadataTaxonomy
from sqlalchemy import update


def populate_org_taxonomy(
    db: Session,
    org_name: str,
    org_type: str,
    description: str,
    fn_get_taxonomy: Callable,
) -> Organisation:
    """Populates the taxonomy from the data."""

    # First the org
    org = db.query(Organisation).filter(Organisation.name == org_name).one_or_none()
    if org is None:
        org = Organisation(
            name=org_name, description=description, organisation_type=org_type
        )
        db.add(org)
        db.flush()
        db.commit()

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
        db.commit()
    return org


def _migrate_CCLW(db: Session, org_cclw: Organisation) -> None:
    # Change org CCLW to LSE, and create CCLW as a datasource
    result = db.execute(
        update(Organisation)
        .where(Organisation.id == org_cclw.id)
        .values(
            name="LSE",
            description="London School of Economics",
        )
    )
    # Raise if we didn't update
    if result.rowcount == 0:  # type: ignore
        raise RuntimeError("Expected to be able to update CCLW org")

    # Now add the datasource
    db.add(
        OrganisationDatasource(
            organisation_id=org_cclw.id,
            description="Climate Change Laws of the World",
            prefix="CCLW",
        )
    )


def _migrate_UNFCCC(db: Session, org_unfcc: Organisation) -> None:
    # Change org CCLW to LSE, and create CCLW as a datasource
    result = db.execute(
        update(Organisation)
        .where(Organisation.id == org_unfcc.id)
        .values(name="CPR", description="Climate Policy Radar", organisation_type="CIC")
    )
    # Raise if we didn't update
    if result.rowcount == 0:  # type: ignore
        raise RuntimeError("Expected to be able to update UNFCCC org")

    # Now add the datasource
    db.add(
        OrganisationDatasource(
            organisation_id=org_unfcc.id,
            description="United Nations Framework Convention on Climate Change",
            prefix="UNFCC",
        )
    )


def populate_taxonomy(db: Session) -> None:
    # First check if there is an org of LSE and CPR - if there is then we're done
    lse = db.query(Organisation).filter(Organisation.name == "LSE").one_or_none()

    if lse is None:
        org_cclw = populate_org_taxonomy(
            db,
            org_name="CCLW",
            org_type="Academic",
            description="Climate Change Laws of the World",
            fn_get_taxonomy=get_cclw_taxonomy,
        )
        _migrate_CCLW(db, org_cclw)

    cpr = db.query(Organisation).filter(Organisation.name == "CPR").one_or_none()
    if cpr is None:
        org_unfcc = populate_org_taxonomy(
            db,
            org_name="UNFCCC",
            org_type="UN",
            description="United Nations Framework Convention on Climate Change",
            fn_get_taxonomy=get_unf3c_taxonomy,
        )

        _migrate_UNFCCC(db, org_unfcc)

    db.commit()
