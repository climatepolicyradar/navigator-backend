from app.api.api_v1.routers.admin import _start_ingest
from app.data_migrations import populate_taxonomy


ONE_DFC_ROW = """ID,Document ID,CCLW Description,Part of collection?,Create new family/ies?,Collection ID,Collection name,Collection summary,Document title,Family name,Family summary,Family ID,Document role,Applies to ID,Geography ISO,Documents,Category,Events,Sectors,Instruments,Frameworks,Responses,Natural Hazards,Document Type,Year,Language,Keywords,Geography,Parent Legislation,Comment,CPR Document ID,CPR Family ID,CPR Collection ID,CPR Family Slug,CPR Document Slug
1001,0,Test1,FALSE,FALSE,N/A,Collection1,CollectionSummary1,Title1,Fam1,Summary1,,MAIN,,GEO,http://somewhere|en,executive,02/02/2014|Law passed,Energy,,,Mitigation,,Order,,,Energy Supply,Algeria,,,CCLW.executive.1.2,CCLW.family.1001.0,CPR.Collection.1,FamSlug1,DocSlug1
"""

TWO_EVENT_ROWS = """Id,Eventable type,Eventable Id,Eventable name,Event type,Title,Description,Date,Url,CPR Event ID,CPR Family ID,Event Status
1101,Legislation,1001,Title1,Passed/Approved,Published,,2019-12-25,,CCLW.legislation_event.1101.0,CCLW.family.1001.0,OK
1102,Legislation,1001,Title1,Entered Into Force,Entered into force,,2018-01-01,,CCLW.legislation_event.1102.1,CCLW.family.1001.0,DUPLICATED
"""


def setup_with_docs(test_db, mocker):
    mock_s3 = mocker.patch("app.core.aws.S3Client")

    populate_taxonomy(db=test_db)

    _start_ingest(test_db, mock_s3, "s3_prefix", ONE_DFC_ROW, TWO_EVENT_ROWS)


def test(client, test_db, mocker):
    setup_with_docs(test_db, mocker)

    # Test associations
    get_detail_response_1 = client.get(
        "/api/v1/documents/CCLW.executive.1.2?group_documents=True",
    )
    assert get_detail_response_1.status_code == 200
