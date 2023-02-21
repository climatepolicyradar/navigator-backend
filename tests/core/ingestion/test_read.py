from unittest.mock import MagicMock
import pytest
from app.core.ingestion.ingest_row import IngestRow
from app.core.ingestion.reader import read
from app.core.ingestion.utils import IngestContext
from app.core.validation.types import ImportSchemaMismatchError


THREE_ROWS = """ID,Document ID,CCLW Description,Part of collection?,Create new family/ies?,Collection ID,Collection name,Collection summary,Document title,Family name,Family summary,Family ID,Document role,Applies to ID,Geography ISO,Documents,Category,Events,Sectors,Instruments,Frameworks,Responses,Natural Hazards,Document Type,Year,Language,Keywords,Geography,Parent Legislation,Comment,CPR Document ID,CPR Family ID,CPR Collection ID,CPR Family Slug,CPR Document Slug
1001,0,Test1,FALSE,FALSE,N/A,N/A,N/A,Title1,Fam1,Summary1,,MAIN,,DZA,,executive,02/02/2014|Law passed,Energy,,,Mitigation,,Order,,,Energy Supply,Algeria,,,CCLW.executive.1001.0,CCLW.family.1001.0,N/A,FamSlug1,DocSlug1
1002,0,Test2,FALSE,FALSE,N/A,N/A,N/A,Title2,Fam2,Summary2,,MAIN,,DZA,,executive,28/04/2013|Law passed||,Energy;LULUCF;Social development;Transportation;Urban;Waste,"Processes, plans and strategies|Governance",Adaptation;Mitigation,Adaptation;Mitigation,,Plan,,,Adaptation;Energy Supply;Energy Demand;Redd+ And Lulucf;Transportation,Algeria,,,CCLW.executive.1002.0,CCLW.family.1002.0,N/A,FamSlug2,DocSlug2
1003,0,Test3,FALSE,FALSE,N/A,N/A,N/A,Title3,Fam3,Summary3,,MAIN,,DZA,,executive,08/12/2011|Law passed,Energy,Subsidies|Economic,,Mitigation,,Decree,,,Research And Development;Energy Supply,Algeria,,,CCLW.executive.1003.0,CCLW.family.1003.0,N/A,FamSlug3,DocSlug3
"""

THREE_ROWS_MISSING_FIELD = """ID,Document ID,CCLW Description,Part of collection?,Create new family/ies?,Collection ID,Collection name,Collection summary,Document title,Family name,Family summary,Family ID,Document role,Applies to ID,Geography ISO,Documents,Category,Events,Sectors,Instruments,Frameworks,Responses,Natural Hazards,Document Type,Year,Language,Keywords,Geography,Parent Legislation,Comment,CPR Document ID,CPR Family ID,CPR Collection ID,CPR Family Slug
1001,0,Test1,FALSE,FALSE,N/A,N/A,N/A,Title1,Fam1,Summary1,,MAIN,,DZA,,executive,02/02/2014|Law passed,Energy,,,Mitigation,,Order,,,Energy Supply,Algeria,,,CCLW.executive.1001.0,CCLW.family.1001.0,N/A,FamSlug1
1002,0,Test2,FALSE,FALSE,N/A,N/A,N/A,Title2,Fam2,Summary2,,MAIN,,DZA,,executive,28/04/2013|Law passed||,Energy;LULUCF;Social development;Transportation;Urban;Waste,"Processes, plans and strategies|Governance",Adaptation;Mitigation,Adaptation;Mitigation,,Plan,,,Adaptation;Energy Supply;Energy Demand;Redd+ And Lulucf;Transportation,Algeria,,,CCLW.executive.1002.0,CCLW.family.1002.0,N/A,FamSlug2
1003,0,Test3,FALSE,FALSE,N/A,N/A,N/A,Title3,Fam3,Summary3,,MAIN,,DZA,,executive,08/12/2011|Law passed,Energy,Subsidies|Economic,,Mitigation,,Decree,,,Research And Development;Energy Supply,Algeria,,,CCLW.executive.1003.0,CCLW.family.1003.0,N/A,FamSlug3
"""


def test_read__raises_with_no_contents():
    context = IngestContext(org_id=1, results=[])
    process = MagicMock()
    with pytest.raises(ImportSchemaMismatchError) as e_info:
        contents = ""
        read(contents, context, process)

    assert len(context.results) == 0
    assert (
        e_info.value.message
        == "Bulk import file failed schema validation: No fields in CSV!"
    )
    assert e_info.value.details == {}


def test_read__raises_with_wrong_fields():
    context = IngestContext(org_id=1, results=[])
    process = MagicMock()
    with pytest.raises(ImportSchemaMismatchError) as e_info:
        contents = """a,b,c
        1,2,3"""
        read(contents, context, process)

    assert len(context.results) == 0
    assert (
        e_info.value.message
        == "Bulk import file failed schema validation: Field names in CSV did not validate"
    )
    assert e_info.value.details == {
        "missing": [
            "Applies to ID",
            "CCLW Description",
            "CPR Collection ID",
            "CPR Document ID",
            "CPR Document Slug",
            "CPR Family ID",
            "CPR Family Slug",
            "Category",
            "Collection ID",
            "Collection name",
            "Collection summary",
            "Comment",
            "Create new family/ies?",
            "Document ID",
            "Document Type",
            "Document role",
            "Document title",
            "Documents",
            "Events",
            "Family ID",
            "Family name",
            "Family summary",
            "Frameworks",
            "Geography",
            "Geography ISO",
            "ID",
            "Instruments",
            "Keywords",
            "Language",
            "Natural Hazards",
            "Parent Legislation",
            "Part of collection?",
            "Responses",
            "Sectors",
            "Year",
        ]
    }


def test_read__raises_with_missing_field():
    context = IngestContext(org_id=1, results=[])
    process = MagicMock()
    with pytest.raises(ImportSchemaMismatchError) as e_info:
        read(THREE_ROWS_MISSING_FIELD, context, process)

    assert len(context.results) == 0
    assert (
        e_info.value.message
        == "Bulk import file failed schema validation: Field names in CSV did not validate"
    )
    assert e_info.value.details == {"missing": ["CPR Document Slug"]}


def test_read__processes_all_rows():
    context = IngestContext(org_id=1, results=[])
    process = MagicMock()
    read(THREE_ROWS, context, process)

    expected_rows = 3
    assert process.call_count == expected_rows

    start_n = 1
    n = 1
    for args in process.call_args_list:
        context_arg: IngestContext
        row_arg: IngestRow
        context_arg, row_arg = args[0]
        assert context_arg == context
        assert row_arg.row_number == n
        assert row_arg.document_title == f"Title{n}"
        n = n + 1

    assert n - start_n == expected_rows
