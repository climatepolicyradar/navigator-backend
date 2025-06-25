from typing import Dict

from db_client.models.dfce import FamilyDocument, FamilyGeography
from sqlalchemy.orm import Session
from tests.non_search.setup_helpers import setup_with_documents_large_with_families

from app.models.document import FamilyDocumentWithContextResponse
from app.repository.document import get_family_document_and_context


def test_get_family_document_and_context(
    documents_large: list[Dict],
    data_db: Session,
):
    setup_with_documents_large_with_families(documents_large, data_db)
    doc = (
        data_db.query(FamilyDocument)
        .filter(FamilyDocument.import_id == "CCLW.document.i00000192.n0000")
        .one()
    )

    response: FamilyDocumentWithContextResponse = get_family_document_and_context(
        data_db, doc.import_id
    )

    assert response.family.import_id == doc.family_import_id
    assert response.document.import_id == doc.import_id


def test_get_family_document_and_context_extra_geog(
    documents_large: list[Dict],
    data_db: Session,
):
    setup_with_documents_large_with_families(documents_large, data_db)
    doc = (
        data_db.query(FamilyDocument)
        .filter(FamilyDocument.import_id == "CCLW.document.i00000192.n0000")
        .one()
    )
    # add an extra geography
    data_db.add(
        FamilyGeography(
            family_import_id=doc.family_import_id,
            geography_id=17,
        )
    )
    data_db.commit()
    response: FamilyDocumentWithContextResponse = get_family_document_and_context(
        data_db, doc.import_id
    )

    assert response.family.import_id == doc.family_import_id
    assert response.document.import_id == doc.import_id
