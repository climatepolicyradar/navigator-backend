from app.db.models.deprecated import Document
from app.db.models.document import PhysicalDocument
from app.db.models.lawpolicy import FamilyDocument, DocumentStatus, Variant, DocumentType

from dfc_csv_reader import Row
from sqlalchemy.orm import Session

from utils import get_or_create


def ingest_document(db: Session, row: Row):
    """Creates a PhysicalDocument and a FamilyDocument together."""

    import_id = row.cpr_document_id
    print(f"Checking for a pre-existing document for import {import_id}")
    doc: Document = db.query(Document).filter(Document.import_id == import_id).first()
    assert doc

    print(f"Creating physical document for import {import_id}")
    new_phys_doc = PhysicalDocument(
        id=row.row_number,
        title=doc.name,
        md5_sum=doc.md5_sum,
        source_url=doc.source_url,
        date=doc.publication_ts,
        format=doc.content_type,
    )
    
    # TODO: Add to the database
    # db.add(new_phys_doc)

    print(f"Creating family document for import {import_id}")
    new_fam_doc = FamilyDocument(
        family_id=row.cpr_family_id,
        physical_document_id=new_phys_doc.id,
        cdn_url=row.documents,
        import_id=import_id,
        variant_id=get_or_create(db, Variant, variant=row.document_role),
        document_status=DocumentStatus.PUBLISHED,
        document_type_id=get_or_create(db, DocumentType, type_name=row.document_type),
    )

    # TODO: Add to the database
    # db.add(new_fam_doc)

    print(f"Langs = {row.language}")

    return new_phys_doc, new_fam_doc
