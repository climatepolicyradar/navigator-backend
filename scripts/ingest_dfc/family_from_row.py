from app.db.models.law_policy.slug import Slug
from dfc_csv_reader import Row
from sqlalchemy.orm import Session

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


def family_from_row(db: Session, org_id: int, row: Row, phys_doc_id: int):
    result = {}
    import_id = row.cpr_document_id
    category_name = get_or_create(db, FamilyCategory, category_name=row.category, extra={"description": ""}).category_name
    family_type = get_or_create(db, FamilyType, type_name=row.document_type, extra={"description": ""}).type_name
    print(f"- Getting Geography for {row.geography_iso}")
    geography_id = db.query(Geography).filter(Geography.value == row.geography_iso).first().id

    family = Family(
        title=row.family_name,
        import_id=import_id,
        description=row.family_summary,
        geography_id=geography_id,
        category_name=category_name,
        family_type=family_type,
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

    print(f"- Creating family organisation for import {import_id}")
    family_organisation = FamilyOrganisation(
       family_id=family.id,
       organisation_id=org_id 
    )
    db.add(family_organisation)
    db.commit()
    result["family_organisation"] = to_dict(family_organisation)

    print(f"- Creating slugs for import {import_id}")
    result["slugs"] = _add_slugs(db, row, family, family_document)

    return result

def _add_slugs(db: Session, row: Row, family: Family, family_document:FamilyDocument):
    family_slug = Slug(
        name=row.cpr_family_slug,
        family_id=family.id
    )
    family_document_slug = Slug(
       name=row.cpr_document_slug,
       family_document_id=family_document.physical_document_id
    ) 

    db.add(family_slug, family_document_slug)
    db.commit()
    return {
        "family_slug": to_dict(family_slug),
        "document_slug": to_dict(family_document_slug)
    }
