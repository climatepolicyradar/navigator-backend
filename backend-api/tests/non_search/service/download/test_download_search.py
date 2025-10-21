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
            family_source=mock_organisation,  # ORG
            corpus_import_id=f"Test.{mock_organisation}.corpus.0",
            corpus_type_name="Laws and Policies",
            family_geographies=["BRA"],
            family_metadata={},  # TODO fix this
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
            db=None,
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

    mock_family_slug = "ccc-test-family"

    mock_document_slug = "ccc-test-document"
    mock_document_source_url = "www.ccc-test-document.pdf"
    mock_document_title = "CCC Test Document"
    mock_document_content_type = "application/pdf"
    mock_document_import_id = "Test.CCC.document.0"

    mock_get_extra_csv_info.return_value = {
        "source": {mock_family_slug: mock_organisation},
        "documents": {
            mock_family_slug: [
                FamilyDocument(
                    import_id=mock_document_import_id,
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
        "collection": {
            mock_family_slug: Collection(
                import_id=f"Test.{mock_organisation}.collection.0",
                title=f"Test {mock_organisation} Collection",
                description=f"Test {mock_organisation} Description",
                valid_metadata={"id": ["111111"]},
            )
        },
        "document_events": {
            mock_document_import_id: [
                FamilyEvent(
                    valid_metadata={
                        "event_type": ["Test Type"],
                        "description": ["Test Description"],
                    },
                    date=datetime(2025, 2, 2),
                )
            ]
        },
    }

    families = [
        SearchResponseFamily(
            family_slug=mock_family_slug,
            family_name=f"{mock_organisation} Test Family",
            family_description=f"{mock_organisation} Test Family Description",
            family_category="Litigation",
            family_date="2025-01-01",
            family_source=mock_organisation,  # ORG
            corpus_import_id=f"Test.{mock_organisation}.corpus.0",
            corpus_type_name="Litigation",
            family_geographies=["USA"],
            family_metadata={
                "case_number": ["00-1111"],
                "status": ["Decided"],
                "concept_preferred_label": [
                    "category/Test Category",
                    "principal_law/Test Principal Law",
                    "jurisdiction/Test Jurisdiction",
                ],
            },
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
        "Bundle ID,"  # 1
        "Bundle Name,"  # 2
        "Bundle URL,"  # 3
        "Case ID,"  # 4
        "Case Name,"  # 5
        "Non-English Case Name,"  # 6 metadata -> original_case_name GLOBAL
        "Case URL,"  # 7
        "At Issue,"  # 8
        "Case Summary,"  # 9
        "Case Number,"  # 10
        "Case Filing Year for Action,"  # 11
        "Status,"  # 12
        "Jurisdictions,"  # 13
        "Case Categories,"  # 14
        "Principal Laws,"  # 15
        "Court Number,"  # 16 ?????????????? that's not a thing...
        "Document Title,"  # 17
        "Document URL,"  # 18
        "Document Content URL,"  # 19
        "Document Type,"  # 20
        "Document Filing Date,"  # 21
        "Document Summary,"  # 22
        "Geographies,"  # 23
        "Document Content Matches Search Phrase\r\n"  # 24
        # Row 1 - Corresponding values - US family
        "Test.CCC.collection.0,"  # 1
        "Test CCC Collection,"  # 2
        "https://test.com/collection/Test.CCC.collection.0,"  # 3
        "ccc-test-family,"  # 4
        "CCC Test Family,"  # 5
        ","  # 6 GLOBAL field - no value for US families
        "https://test.com/document/ccc-test-family,"  # 7
        "Test CCC Description,"  # 8
        "CCC Test Family Description,"  # 9
        "00-1111,"  # 10
        "2025,"  # 11
        "Decided,"  # 12 metadata
        "Test Jurisdiction,"  # 13
        "Test Category,"  # 14
        "Test Principal Law,"  # 15
        ","  # 16 court number is not a thing
        "CCC Test Document,"  # 17
        "https://test.com/documents/ccc-test-document,"  # 18
        "www.ccc-test-document.pdf,"  # 19
        "Test Type,"  # 20
        "2025-02-02T00:00:00,"  # 21
        "Test Description,"  # 22
        "USA,"  # 23
        "Yes\r\n"  # 24
        # Row 2 - Corresponding values - GLOBAL family
    )

    assert (
        process_result_into_csv(
            db=None,
            search_response_families=families,
            base_url="test.com",
            is_browse=False,
            theme="CCC",
        )
        == expected_search_csv
    )


# def test_process_result_into_csv_returns_correct_data_for_mcf_in_csv_format(data_db):
#     families = [
#         SearchResponseFamily(
#             family_slug="mcf-test-family",
#             family_name="MCF Test Family",
#             family_description="MCF Test Family Description",
#             family_category="MCF",
#             family_date="2025-01-01",
#             family_source="GCF",
#             corpus_import_id="Test.MCF.corpus.0",
#             corpus_type_name="MCF",
#             family_geographies=["BRA"],
#             family_metadata={
#                 "region": ["Latin America & Caribbean"],
#                 "sector": ["Test Sector"],
#                 "status": ["Project Completed"],
#                 "project_id": ["000000"],
#                 "project_url": ["https://www.test.org/project/mcf-test-family/"],
#                 "implementing_agency": ["Test Agency"],
#                 "project_value_fund_spend": ["1000000"],
#                 "project_value_co_financing": ["0"],
#                 "theme": ["Test Theme"],
#                 "result_area": ["Test Result Area"],
#                 "result_type": ["Test"],
#                 "approved_ref": ["FP123"],
#                 "focal_area": ["Climate Change"],
#             },
#             family_title_match=False,
#             family_description_match=False,
#             total_passage_hits=1,
#             family_documents=[],
#             continuation_token=None,
#             prev_continuation_token=None,
#             metadata=None,
#         )
#     ]

#     expected_mcf_search_csv = "Document ID,"
# Status,Implementing Agency,Sector,Result Area,Result Type,Focal Area,Theme,Approved Ref,External Project ID,Project URL,Project Value $ (Co-financing),Project Value $ (Fund Spend),,,,,,,,,,,,,MCF,,,BRA"
# "Document ID,"
# "Document Title",
# "Family ID",
# "Family Title",
# "Family Summary",
# "Collection Title(s)",
# "Collection Description(s)",
# "Document Variant",
# "Document Content URL",
# "Language",
# "Source",
# "Type", # Guidance or Project
# "Geographies",
# "Geography ISOs",
# "First event in timeline",
# "Last event in timeline",
# "Full timeline of events (types)",
# "Full timeline of events (dates)",
# "Date Added to System",
# "Last Modified on System",
# "Internal Document ID",
# "Internal Family ID",
# "Internal Corpus ID",
# "Internal Collection ID(s)",
# "Family URL",
# "Document URL",
# "Document Type", # from document metadata where present
# "Category",

# "Region",
# "Status",
# "Implementing Agency",
# "Sector",
# "Result Area",
# "Result Type",
# "Focal Area",
# "Theme",
# "Approved Ref",
# "External Project ID",
# "Project URL",
# "Project Value $ (Co-financing)",
# "Project Value $ (Fund Spend)",

# assert process_result_into_csv(
#         data_db, families, base_url="", is_browse=False, theme="MCF"
#     ) == expected_mcf_search_csv
