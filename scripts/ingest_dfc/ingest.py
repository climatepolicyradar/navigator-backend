import sys
import enum
from datetime import datetime
from app.db.models.document import PhysicalDocument
from app.db.models.law_policy import (
    Family,
    FamilyCategory,
    FamilyDocument,
    FamilyType,
    DocumentStatus,
    Variant,
    DocumentType,
)

from dfc_csv_reader import Row
from sqlalchemy.orm import Session

from utils import get_or_create

class PublicationDateAccuracy(enum.Enum):
    SECOND_ACCURACY = 100000,
    MINUTE_ACCURACY = 200000,
    HOUR_ACCURACY = 300000,
    DAY_ACCURACY = 400000,
    MONTH_ACCURACY = 500000,
    YEAR_ACCURACY = 600000,
    NOT_DEFINED = 999999,


DEFAULT_POLICY_DATE = datetime(1900, 1, 1, 0, 0, 0, 999999)

"""
if year is missing - you the default with has 9999
if only a year - then use without 99999

"""

def ingest_document(db: Session, row: Row):
    """Creates a PhysicalDocument and a FamilyDocument together."""

    import_id = row.cpr_document_id
    print(f"  Creating physical document for import {import_id}")
    new_phys_doc = PhysicalDocument(
        id=row.cpr_document_id,
        title=row.document_title,
        source_url=row.documents,
        date=datetime(row.year, 1, 1) if row.year else DEFAULT_POLICY_DATE ,
    )

    # TODO: Add to the database
    db.add(new_phys_doc)
    db.commit()

    print(f"  Creating family for import {import_id}")

    category_name = get_or_create(db, FamilyCategory, category_name=row.category, extra={"description": ""}).category_name
    family_type = get_or_create(db, FamilyType, type_name=row.document_type, extra={"description": ""}).type_name

    new_family = Family(
        id=row.cpr_family_id,
        title=row.family_name,
        import_id=import_id,
        description=row.family_summary,
        geography_id=doc.geography_id,
        category_name=category_name,
        family_type=family_type,
    )
    db.add(new_family)

    print(f"  Creating family document for import {import_id}")
    variant_name = get_or_create( db, Variant, variant_name=row.document_role, extra={"description": ""}).variant_name
    document_type_id = get_or_create(db, DocumentType, name=row.document_type).id

    new_fam_doc = FamilyDocument(
        family_id=row.cpr_family_id,
        physical_document_id=new_phys_doc.id,
        cdn_url=row.documents,
        import_id=import_id,
        variant_name=variant_name,
        document_status=DocumentStatus.PUBLISHED,
        document_type_id=document_type_id,
    )

    # TODO: Add to the database
    db.add(new_fam_doc)

    print(f"Langs = {row.language}")

    return new_phys_doc, new_fam_doc
