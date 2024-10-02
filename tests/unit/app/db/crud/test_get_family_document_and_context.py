from typing import Dict

from db_client.models.dfce import FamilyGeography
from sqlalchemy.orm import Session

from app.api.api_v1.schemas.document import FamilyDocumentWithContextResponse
from app.db.crud.document import get_family_document_and_context
from tests.non_search.setup_helpers import setup_with_documents_large_with_families


def test_get_family_document_and_context(
    documents_large: list[Dict],
    data_db: Session,
):
    setup_with_documents_large_with_families(documents_large, data_db)
    response: FamilyDocumentWithContextResponse = get_family_document_and_context(
        data_db, "CCLW.document.i00000192.n0000"
    )

    assert response.family.import_id == "CCLW.family.1001.0"
    assert response.document.import_id == "CCLW.document.i00000192.n0000"


def test_get_family_document_and_context_extra_geog(
    documents_large: list[Dict],
    data_db: Session,
):
    setup_with_documents_large_with_families(documents_large, data_db)
    # add an extra geography
    data_db.add(
        FamilyGeography(
            family_import_id="CCLW.family.1001.0",
            geography_id=2,
        )
    )
    data_db.commit()
    response: FamilyDocumentWithContextResponse = get_family_document_and_context(
        data_db, "CCLW.document.i00000192.n0000"
    )

    assert response.family.import_id == "CCLW.family.1001.0"
    assert response.document.import_id == "CCLW.document.i00000192.n0000"
