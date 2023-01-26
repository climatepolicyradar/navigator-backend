from pprint import pprint
from app.db.models.deprecated import Document
from app.db.models.document import PhysicalDocument
from dfc_csv_reader import Row
from sqlalchemy.orm import Session


def ingest_physical_document(db: Session, row: Row):
    import_id = row.cpr_document_id
    pprint(f"Checking for a physical document for import {import_id}")
    doc: Document = db.query(Document).filter(Document.import_id == import_id).first()
    assert doc

    print(f"Creating physical document for import {doc.id} {import_id}")
    new_doc = PhysicalDocument(
        id=row.row_number,
        title=doc.name,
        md5_sum=doc.md5_sum,
        source_url=doc.source_url,
        date=doc.publication_ts,
        format=doc.content_type,
    )
    print(f"Langs = {row.language}")

    # TODO: Add to the database
    # db.add(new_doc)

    return new_doc
