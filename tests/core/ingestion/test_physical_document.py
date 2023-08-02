from typing import cast
from sqlalchemy.orm import Session

from app.core.ingestion.cclw.ingest_row_cclw import CCLWDocumentIngestRow
from app.core.ingestion.physical_document import (
    create_physical_document_from_params,
    update_physical_document_languages,
)
from app.core.ingestion.processor import build_params_from_cclw
from app.db.models.document import PhysicalDocument
from app.db.models.document.physical_document import (
    LanguageSource,
    PhysicalDocumentLanguage,
)
from tests.core.ingestion.helpers import (
    DOCUMENT_TITLE,
    get_doc_ingest_row_data,
    populate_for_ingest,
)


def _get_all_phys_docs(test_db: Session, phys_doc_id: int):
    return (
        test_db.query(PhysicalDocumentLanguage)
        .filter_by(document_id=phys_doc_id)
        .filter_by(source=LanguageSource.USER)
        .filter_by(visible=True)
        .all()
    )


def _create_physical_document(
    test_db: Session, langs: list[str]
) -> tuple[PhysicalDocument, dict]:
    populate_for_ingest(test_db)
    row = CCLWDocumentIngestRow.from_row(1, get_doc_ingest_row_data(0))
    row.language = langs
    result = {}

    phys_doc = create_physical_document_from_params(
        test_db, build_params_from_cclw(row), result
    )
    test_db.flush()
    return phys_doc, result


def test_physical_document_from_row(test_db: Session):
    phys_doc, result = _create_physical_document(test_db, ["English", "German"])

    assert phys_doc
    assert len(phys_doc.languages) == 2
    assert set([lang.language_code for lang in phys_doc.languages]) == {"eng", "deu"}

    # Now check db
    actual_keys = set(result.keys())
    expected_keys = set(
        [
            "physical_document",
            "physical_document_language",
            "language",
        ]
    )
    assert actual_keys.symmetric_difference(expected_keys) == set([])
    assert test_db.query(PhysicalDocument).filter_by(title=DOCUMENT_TITLE).one()
    assert len(_get_all_phys_docs(test_db, cast(int, phys_doc.id))) == 2


def test_update_physical_document_idempotent_language(test_db: Session):
    result = {}
    phys_doc, _ = _create_physical_document(test_db, ["English", "German"])

    update_physical_document_languages(test_db, ["English", "German"], result, phys_doc)
    test_db.flush()

    assert phys_doc
    assert len(phys_doc.languages) == 2
    assert set([lang.language_code for lang in phys_doc.languages]) == {
        "eng",
        "deu",
    }

    # Now check db
    actual_keys = set(result.keys())
    expected_keys = set([])
    assert actual_keys.symmetric_difference(expected_keys) == set([])
    assert test_db.query(PhysicalDocument).filter_by(title=DOCUMENT_TITLE).one()
    assert len(_get_all_phys_docs(test_db, cast(int, phys_doc.id))) == 2


def test_update_physical_document_adds_language(test_db: Session):
    result = {}
    phys_doc, _ = _create_physical_document(test_db, ["English", "German"])

    update_physical_document_languages(
        test_db, ["English", "German", "Spanish"], result, phys_doc
    )
    test_db.flush()

    assert phys_doc
    assert len(phys_doc.languages) == 3
    assert set([lang.language_code for lang in phys_doc.languages]) == {
        "eng",
        "deu",
        "spa",
    }

    # Now check db
    actual_keys = set(result.keys())
    expected_keys = set(
        [
            "physical_document_language",
            "language",
        ]
    )
    assert actual_keys.symmetric_difference(expected_keys) == set([])
    assert test_db.query(PhysicalDocument).filter_by(title=DOCUMENT_TITLE).one()
    assert len(_get_all_phys_docs(test_db, cast(int, phys_doc.id))) == 3


def test_update_physical_document_removes_language(test_db: Session):
    result = {}
    phys_doc, _ = _create_physical_document(test_db, ["English", "German"])

    update_physical_document_languages(test_db, ["English"], result, phys_doc)
    test_db.flush()

    assert phys_doc
    assert len(phys_doc.languages) == 1
    assert set([lang.language_code for lang in phys_doc.languages]) == {
        "eng",
    }

    # Now check db
    actual_keys = set(result.keys())
    expected_keys = set([])
    assert actual_keys.symmetric_difference(expected_keys) == set([])
    assert test_db.query(PhysicalDocument).filter_by(title=DOCUMENT_TITLE).one()
    assert len(_get_all_phys_docs(test_db, cast(int, phys_doc.id))) == 1
