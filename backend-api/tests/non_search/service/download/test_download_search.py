from unittest.mock import patch

from db_client.models.dfce import FamilyDocument, Slug
from db_client.models.document import PhysicalDocument

from app.models.search import (
    SearchResponseDocumentPassage,
    SearchResponseFamily,
    SearchResponseFamilyDocument,
)
from app.service.download import process_result_into_csv


@patch("app.service.download._get_extra_csv_info")
def test_process_result_into_csv_returns_correct_data_for_CPR_search_in_csv_format(
    mock_get_extra_csv_info,
):
    mock_get_extra_csv_info.return_value = {
        "metadata": {"cpr-test-family": {}},
        "source": {"cpr-test-family": "CPR"},
        "documents": {
            "cpr-test-family": [
                FamilyDocument(
                    slugs=[Slug(name="cpr-test-document")],
                    physical_document=PhysicalDocument(
                        source_url="www.cpr-test-document.pdf",
                        title="CPR Test Document",
                        content_type="application/pdf",
                    ),
                    valid_metadata={},
                )
            ]
        },
        "collection": {"cpr-test-family": {}},
        "document_events": {},
    }

    families = [
        SearchResponseFamily(
            family_slug="cpr-test-family",
            family_name="CPR Test Family",
            family_description="CPR Test Family Description",
            family_category="Legislative",
            family_date="2025-01-01",
            family_source="CPR",  # ORG
            corpus_import_id="Test.CPR.corpus.0",
            corpus_type_name="Laws and Policies",
            family_geographies=["BRA"],
            family_metadata={},  # TODO fix this
            family_title_match=True,
            family_description_match=False,
            total_passage_hits=1,
            family_documents=[
                SearchResponseFamilyDocument(
                    document_title="CPR Test Document",
                    document_slug="cpr-test-document",
                    document_type="Test",
                    document_source_url="www.cpr-test-document.pdf",
                    document_url="test.com/documents/cpr-test-document",
                    document_content_type="application/pdf",
                    document_passage_matches=[
                        SearchResponseDocumentPassage(text="test", text_block_id="0")
                    ],
                )
            ],
            continuation_token=None,
            prev_continuation_token=None,
            metadata=None,
        )
    ]

    expected_search_csv = (
        # Column headings
        "Collection Name,"
        "Collection Summary,"
        "Family Name,"
        "Family Summary,"
        "Family Publication Date,"
        "Family URL,"
        "Document Title,"
        "Document URL,"
        "Document Content URL,"
        "Document Type,"
        "Document Content Matches Search Phrase,"
        "Geographies,"
        "Category,"
        "Languages,"
        "Source\r\n"
        # Corresponding values
        ","
        ","
        "CPR Test Family,"
        "CPR Test Family Description,"
        "2025-01-01,"
        "https://test.com/document/cpr-test-family,"
        "CPR Test Document,"
        "https://test.com/documents/cpr-test-document,"
        "www.cpr-test-document.pdf,"
        ","
        "Yes,"
        "BRA,"
        "Legislative,"
        ","
        "CPR\r\n"
    )

    assert (
        process_result_into_csv(
            db=None,
            search_response_families=families,
            base_url="test.com",
            is_browse=False,
            theme="CPR",
        )
        == expected_search_csv
    )
