from datetime import datetime
from pprint import pprint
from app.db.models.app.users import Organisation
from app.db.models.document import PhysicalDocument
from app.db.models.law_policy import (
    Family,
    FamilyCategory,
    FamilyDocument,
    FamilyType,
    DocumentStatus,
    Variant,
    FamilyDocumentType,
    Geography,
)
from app.db.models.law_policy.family import FamilyOrganisation

from dfc_csv_reader import Row
from sqlalchemy.orm import Session

from utils import get_or_create, to_dict, DEFAULT_POLICY_DATE
"""
if year is missing - you the default with has 9999
if only a year - then use without 99999

"""
def ingest_row(db: Session, row: Row) -> dict:
    """Creates a PhysicalDocument and a FamilyDocument together."""
    result = {}
    import_id = row.cpr_document_id

    print("- Creating organisation")
    result["organisation"] = to_dict(get_or_create(db, Organisation, name="CCLW"))
    
    print(f"- Creating physical document for import {import_id}")
    result["doc_schema"] = document_schema_from_row(db, row)

    print(f"- Creating family for import {import_id}")
    result["family_schema"] = family_from_row(db, result["organisation"]["id"], row, result["doc_schema"]["physical_document"]["id"])

    return result

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

    # TODO: Add to the database
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

    return result

def document_schema_from_row(db: Session, row: Row) -> dict:
    result = {}
    physical_document = PhysicalDocument(
        title=row.document_title,
        source_url=row.documents,
        date=datetime(row.year, 1, 1) if row.year else DEFAULT_POLICY_DATE ,
    )
    # TODO: Add languages
    #print(row.language)

    db.add(physical_document)
    db.commit()
    result["physical_document"] = to_dict(physical_document)
    return result
