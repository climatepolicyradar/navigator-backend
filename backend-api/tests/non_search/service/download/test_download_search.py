from datetime import datetime
from unittest.mock import patch

from db_client.models.dfce import FamilyDocument, FamilyEvent, Slug
from db_client.models.dfce.collection import Collection
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
    mock_organisation = "CPR"

    mock_family_slug = "cpr-test-family"

    mock_document_slug = "cpr-test-document"
    mock_document_source_url = "www.cpr-test-document.pdf"
    mock_document_title = "CPR Test Document"
    mock_document_content_type = "application/pdf"

    mock_get_extra_csv_info.return_value = {
        "metadata": {mock_family_slug: {}},
        "source": {mock_family_slug: mock_organisation},
        "documents": {
            mock_family_slug: [
                FamilyDocument(
                    slugs=[Slug(name=mock_document_slug)],
                    physical_document=PhysicalDocument(
                        source_url=mock_document_source_url,
                        title=mock_document_title,
                        content_type=mock_document_content_type,
                    ),
                    valid_metadata={},
                )
            ]
        },
        "collection": {mock_family_slug: {}},
        "document_events": {},
    }

    families = [
        SearchResponseFamily(
            family_slug=mock_family_slug,
            family_name=f"{mock_organisation} Test Family",
            family_description=f"{mock_organisation} Test Family Description",
            family_category="Legislative",
            family_date="2025-01-01",
            family_source=mock_organisation,
            corpus_import_id=f"Test.{mock_organisation}.corpus.0",
            corpus_type_name="Laws and Policies",
            family_geographies=["BRA"],
            family_metadata={},
            family_title_match=True,
            family_description_match=False,
            total_passage_hits=1,
            family_documents=[
                SearchResponseFamilyDocument(
                    document_title=mock_document_title,
                    document_slug=mock_document_slug,
                    document_type="Test",
                    document_source_url=mock_document_source_url,
                    document_url=f"https://test.com/documents/{mock_document_slug}",
                    document_content_type=mock_document_content_type,
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
        "Collection Name,"  # 1
        "Collection Summary,"  # 2
        "Family Name,"  # 3
        "Family Summary,"  # 4
        "Family Publication Date,"  # 5
        "Family URL,"  # 6
        "Document Title,"  # 7
        "Document URL,"  # 8
        "Document Content URL,"  # 9
        "Document Type,"  # 10
        "Document Content Matches Search Phrase,"  # 11
        "Geographies,"  # 12
        "Category,"  # 13
        "Languages,"  # 14
        "Source\r\n"  # 15
        # Corresponding values
        ","  # 1
        ","  # 2
        "CPR Test Family,"  # 3
        "CPR Test Family Description,"  # 4
        "2025-01-01,"  # 5
        "https://test.com/document/cpr-test-family,"  # 6
        "CPR Test Document,"  # 7
        "https://test.com/documents/cpr-test-document,"  # 8
        "www.cpr-test-document.pdf,"  # 9
        ","  # 10
        "Yes,"  # 11
        "BRA,"  # 12
        "Legislative,"  # 13
        ","  # 14
        "CPR\r\n"  # 15
    )

    assert (
        process_result_into_csv(
            db=None,  # type: ignore
            search_response_families=families,
            base_url="test.com",
            is_browse=False,
            theme=mock_organisation,
        )
        == expected_search_csv
    )


@patch("app.service.download._get_extra_csv_info")
def test_process_result_into_csv_returns_correct_data_for_CCC_search_in_csv_format(
    mock_get_extra_csv_info,
):
    mock_organisation = "CCC"

    mock_family_slug_us = "ccc-test-family-us"

    mock_document_slug_us = "ccc-test-document-us"
    mock_document_source_url_us = "www.ccc-test-document-us.pdf"
    mock_document_title_us = "CCC Test Document US"
    mock_document_import_id_us = "Test.CCC.document.0"

    mock_family_slug_global = "ccc-test-family-global"

    mock_document_slug_global = "ccc-test-document-global"
    mock_document_source_url_global = "www.ccc-test-document-global.pdf"
    mock_document_title_global = "CCC Test Document Global"
    mock_document_import_id_global = "Test.CCC.document.1"

    mock_document_content_type = "application/pdf"

    mock_get_extra_csv_info.return_value = {
        "metadata": {
            mock_family_slug_us: {
                "case_number": ["00-1111"],
                "status": ["Decided"],
                "concept_preferred_label": [
                    "category/Test Category US",
                    "principal_law/Test Principal Law US",
                    "jurisdiction/Test Jurisdiction US",
                ],
            },
            mock_family_slug_global: {
                "case_number": ["00-2222"],
                "status": ["Filed"],
                "core_object": ["Test Core Object"],
                "original_case_name": ["Test Non-English Case Name"],
                "concept_preferred_label": [
                    "category/Test Category Global",
                    "principal_law/Test Principal Law Global",
                    "jurisdiction/Test Jurisdiction Global",
                ],
            },
        },
        "source": {mock_family_slug_us: mock_organisation},
        "documents": {
            mock_family_slug_us: [
                FamilyDocument(
                    import_id=mock_document_import_id_us,
                    slugs=[Slug(name=mock_document_slug_us)],
                    physical_document=PhysicalDocument(
                        source_url=mock_document_source_url_us,
                        title=mock_document_title_us,
                        content_type=mock_document_content_type,
                    ),
                    valid_metadata={},
                )
            ],
            mock_family_slug_global: [
                FamilyDocument(
                    import_id=mock_document_import_id_global,
                    slugs=[Slug(name=mock_document_slug_global)],
                    physical_document=PhysicalDocument(
                        source_url=mock_document_source_url_global,
                        title=mock_document_title_global,
                        content_type=mock_document_content_type,
                    ),
                    valid_metadata={},
                )
            ],
        },
        "collection": {
            mock_family_slug_us: Collection(
                import_id=f"Test.{mock_organisation}.collection.0",
                title=f"Test {mock_organisation} Collection",
                description=f"Test {mock_organisation} Description",
                valid_metadata={"id": ["111111"]},
            )
        },
        "document_events": {
            mock_document_import_id_us: [
                FamilyEvent(
                    valid_metadata={
                        "event_type": ["Test Type US"],
                        "description": ["Test Description US"],
                    },
                    date=datetime(2025, 2, 2),
                )
            ],
            mock_document_import_id_global: [
                FamilyEvent(
                    valid_metadata={
                        "event_type": ["Test Type Global"],
                        "description": ["Test Description Global"],
                    },
                    date=datetime(2024, 2, 2),
                )
            ],
        },
    }

    families = [
        SearchResponseFamily(
            family_slug=mock_family_slug_us,
            family_name=f"{mock_organisation} Test Family US",
            family_description=f"{mock_organisation} Test Family Description US",
            family_category="Litigation",
            family_date="2025-01-01",
            family_source=mock_organisation,
            corpus_import_id=f"Test.{mock_organisation}.corpus.0",
            corpus_type_name="Litigation",
            family_geographies=["USA"],
            family_metadata={},
            family_title_match=True,
            family_description_match=False,
            total_passage_hits=1,
            family_documents=[
                SearchResponseFamilyDocument(
                    document_title=mock_document_title_us,
                    document_slug=mock_document_slug_us,
                    document_type="Test US",
                    document_source_url=mock_document_source_url_us,
                    document_url=f"https://test.com/documents/{mock_document_slug_us}",
                    document_content_type=mock_document_content_type,
                    document_passage_matches=[
                        SearchResponseDocumentPassage(text="test", text_block_id="0")
                    ],
                )
            ],
            continuation_token=None,
            prev_continuation_token=None,
            metadata=None,
        ),
        SearchResponseFamily(
            family_slug=mock_family_slug_global,
            family_name=f"{mock_organisation} Test Family Global",
            family_description=f"{mock_organisation} Test Family Description Global",
            family_category="Litigation",
            family_date="2024-01-01",
            family_source=mock_organisation,
            corpus_import_id=f"Test.{mock_organisation}.corpus.0",
            corpus_type_name="Litigation",
            family_geographies=["BRA"],
            family_metadata={},
            family_title_match=True,
            family_description_match=False,
            total_passage_hits=1,
            family_documents=[
                SearchResponseFamilyDocument(
                    document_title=mock_document_title_global,
                    document_slug=mock_document_slug_global,
                    document_type="Test Global",
                    document_source_url=mock_document_source_url_global,
                    document_url=f"https://test.com/documents/{mock_document_slug_global}",
                    document_content_type=mock_document_content_type,
                    document_passage_matches=[
                        SearchResponseDocumentPassage(text="test", text_block_id="0")
                    ],
                )
            ],
            continuation_token=None,
            prev_continuation_token=None,
            metadata=None,
        ),
    ]

    expected_search_csv = (
        # Column headings
        "Bundle ID,"  # 1
        "Bundle Name,"  # 2
        "Bundle URL,"  # 3
        "Case ID,"  # 4
        "Case Name,"  # 5
        "Non-English Case Name,"  # 6
        "Case URL,"  # 7
        "At Issue,"  # 8
        "Case Summary,"  # 9
        "Case Number,"  # 10
        "Case Filing Year for Action,"  # 11
        "Status,"  # 12
        "Jurisdictions,"  # 13
        "Case Categories,"  # 14
        "Principal Laws,"  # 15
        "Document Title,"  # 16
        "Document URL,"  # 17
        "Document Content URL,"  # 18
        "Document Type,"  # 19
        "Document Filing Date,"  # 20
        "Document Summary,"  # 21
        "Geographies,"  # 22
        "Document Content Matches Search Phrase\r\n"  # 23
        # Row 1 - Corresponding values - US family
        "Test.CCC.collection.0,"  # 1
        "Test CCC Collection,"  # 2
        "https://test.com/collection/Test.CCC.collection.0,"  # 3
        "ccc-test-family-us,"  # 4
        "CCC Test Family US,"  # 5
        ","  # 6 GLOBAL field only - no value for US families
        "https://test.com/document/ccc-test-family-us,"  # 7
        "Test CCC Description,"  # 8
        "CCC Test Family Description US,"  # 9
        "00-1111,"  # 10
        "2025,"  # 11
        "Decided,"  # 12
        "Test Jurisdiction US,"  # 13
        "Test Category US,"  # 14
        "Test Principal Law US,"  # 15
        ","  # 16
        "CCC Test Document US,"  # 17
        "https://test.com/documents/ccc-test-document-us,"  # 18
        "www.ccc-test-document-us.pdf,"  # 19
        "Test Type US,"  # 20
        "2025-02-02T00:00:00,"  # 21
        "Test Description US,"  # 22
        "USA,"  # 23
        "Yes\r\n"  # 24
        # Row 2 - Corresponding values - GLOBAL family
        ","  # 1
        ","  # 2
        ","  # 3
        "ccc-test-family-global,"  # 4
        "CCC Test Family Global,"  # 5
        "Test Non-English Case Name,"  # 6
        "https://test.com/document/ccc-test-family-global,"  # 7
        "Test Core Object,"  # 8
        "CCC Test Family Description Global,"  # 9
        "00-2222,"  # 10
        "2024,"  # 11
        "Filed,"  # 12
        "Test Jurisdiction Global,"  # 13
        "Test Category Global,"  # 14
        "Test Principal Law Global,"  # 15
        ","  # 16
        "CCC Test Document Global,"  # 17
        "https://test.com/documents/ccc-test-document-global,"  # 18
        "www.ccc-test-document-global.pdf,"  # 19
        "Test Type Global,"  # 20
        "2024-02-02T00:00:00,"  # 21
        "Test Description Global,"  # 22
        "BRA,"  # 23
        "Yes\r\n"  # 24
    )

    assert (
        process_result_into_csv(
            db=None,  # type: ignore
            search_response_families=families,
            base_url="test.com",
            is_browse=False,
            theme="CCC",
        )
        == expected_search_csv
    )
