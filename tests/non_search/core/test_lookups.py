import pytest
from db_client.models.dfce import FamilyDocument

from app.repository.lookups import doc_type_from_family_document_metadata


@pytest.mark.parametrize(
    ("metadata", "expected"),
    (
        ({"role": ["MAIN"], "type": ["Law"]}, "Law"),
        ({"role": ["MAIN"], "type": ["Law", "Executive"]}, "Law"),
        ({"role": ["MAIN"], "type": ["Law", None]}, "Law"),
        ({"role": ["MAIN"], "type": [None]}, ""),
        ({"role": ["MAIN"], "type": [None, "Law"]}, ""),
        ({"role": ["MAIN"], "type": []}, ""),
        ({"role": ["MAIN"], "type": None}, ""),
        ({"role": ["MAIN"]}, ""),
        ({}, ""),
    ),
)
def test_doc_type_from_family_document_metadata(metadata, expected):
    family_document = FamilyDocument(
        family_import_id="test.1",
        physical_document_id="test.1",
        import_id="test.1",
        variant_name=None,
        document_status="Published",
        valid_metadata=metadata,
    )
    result = doc_type_from_family_document_metadata(family_document)
    assert result == expected
