from app.db.models.law_policy.slug import Slug
from sqlalchemy.orm import Session
from app.db.session import Base
from scripts.ingest_dfc.dfc_row.dfc_row import DfcRow

from scripts.ingest_dfc.utils import get_or_create, to_dict
from app.db.models.law_policy import (
    Family,
    FamilyCategory,
    FamilyDocument,
    FamilyOrganisation,
    FamilyType,
    DocumentStatus,
    Variant,
    FamilyDocumentType,
    Geography,
)


def family_from_row(db: Session, org_id: int, row: DfcRow, phys_doc_id: int) -> dict:
    """_summary_

    Args:
        db (Session): connection to the database.
        org_id (int): the organisation id associated with this row.
        row (DfcRow): the row built from the CSV.
        phys_doc_id (int): the physical document id associated with this row.

    Raises:
        TypeError: This is raised when the geography associated with this row cannot be found in the database.

    Returns:
        dict : a created dictionary to describe what was created.
    """

    result = {}
    def create_family_slug(family: Family):
        print(f"- Creating family slug for import {import_id}")
        family_slug = Slug(
            name=row.cpr_family_slug,
            family_id=family.id
        )

        db.add(family_slug)
        db.commit()
        result["family_slug"] = to_dict(family_slug),

        print(f"- Creating family organisation for import {import_id}")
        family_organisation = FamilyOrganisation(
        family_id=family.id,
        organisation_id=org_id 
        )
        db.add(family_organisation)
        db.commit()
        result["family_organisation"] = to_dict(family_organisation)
    
    import_id = row.cpr_document_id
    category_name = get_or_create(db, FamilyCategory, category_name=row.category, extra={"description": ""}).category_name
    family_type = get_or_create(db, FamilyType, type_name=row.document_type, extra={"description": ""}).type_name
    print(f"- Getting Geography for {row.geography_iso}")
    geography = db.query(Geography).filter(Geography.value == row.geography_iso).first()
    if geography is None:
        raise TypeError(f"Geography value of {row.geography_iso} does not exist in the database.")
    geography_id = geography.id

    family = get_or_create(db, Family,
        title=row.family_name,
        extra={
            "geography_id":geography_id,
            "category_name":category_name,
            "family_type":family_type,
            "description":row.family_summary,
            "import_id":import_id,
        },
        after_create=create_family_slug
    )
    db.add(family)
    db.commit()
    result["family"] = to_dict(family)

    print(f"- Creating family document for import {import_id}")
    variant_name = get_or_create( db, Variant, variant_name=row.document_role, extra={"description": ""}).variant_name
    document_type = get_or_create(db, FamilyDocumentType, name=row.document_type, extra={"description": ""}).name

    family_document = FamilyDocument(
        family_id=family.id,
        physical_document_id=phys_doc_id,
        cdn_url=row.documents,
        import_id=import_id,
        variant_name=variant_name,
        document_status=DocumentStatus.PUBLISHED,
        document_type=document_type,
    )

    db.add(family_document)
    db.commit()
    result["family_document"] = to_dict(family_document)

    print(f"- Creating slugs for import {import_id}")
    result["slugs"] = _add_slugs(db, row, family, family_document)

    return result


def _add_slugs(db: Session, row: DfcRow, family: Family, family_document:FamilyDocument) -> dict:
    """Adds the suits for the family and family_document.

    Args:
        db (Session): connection to the database.
        row (DfcRow): the row built from the CSV.
        family (Family): the family associated with this row.
        family_document the family document associated with this row.

    Returns:
        dict : a created dictionary to describe what was created.
    """
    family_document_slug = Slug(
       name=row.cpr_document_slug,
       family_document_id=family_document.physical_document_id
    ) 

    db.add(family_document_slug)
    db.commit()
    return {
        "document_slug": to_dict(family_document_slug)
    }
