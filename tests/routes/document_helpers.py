from app.api.api_v1.routers.cclw_ingest import _start_ingest
from app.data_migrations import (
    populate_document_role,
    populate_document_type,
    populate_document_variant,
    populate_event_type,
    populate_geography,
    populate_language,
    populate_taxonomy,
)


ONE_DFC_ROW = """ID,Document ID,CCLW Description,Part of collection?,Create new family/ies?,Collection ID,Collection name,Collection summary,Document title,Family name,Family summary,Family ID,Document role,Applies to ID,Geography ISO,Documents,Category,Events,Sectors,Instruments,Frameworks,Responses,Natural Hazards,Document Type,Year,Language,Keywords,Geography,Parent Legislation,Comment,CPR Document ID,CPR Family ID,CPR Collection ID,CPR Family Slug,CPR Document Slug,Document variant,CPR Document Status
1001,0,Test1,FALSE,FALSE,N/A,Collection1,CollectionSummary1,Title1,Fam1,Summary1,,MAIN,,GBR,http://somewhere|en,executive,02/02/2014|Law passed,Energy,,,Mitigation,,Order,,,Energy Supply,Algeria,,,CCLW.executive.1.2,CCLW.family.1001.0,CPR.Collection.1,FamSlug1,DocSlug1,Translation,PUBLISHED
"""

ONE_EVENT_ROW = """Id,Eventable type,Eventable Id,Eventable name,Event type,Title,Description,Date,Url,CPR Event ID,CPR Family ID,Event Status
1101,Legislation,1001,Title1,Passed/Approved,Published,,2019-12-25,,CCLW.legislation_event.1101.0,CCLW.family.1001.0,OK
"""

TWO_DFC_ROW = """ID,Document ID,CCLW Description,Part of collection?,Create new family/ies?,Collection ID,Collection name,Collection summary,Document title,Family name,Family summary,Family ID,Document role,Applies to ID,Geography ISO,Documents,Category,Events,Sectors,Instruments,Frameworks,Responses,Natural Hazards,Document Type,Year,Language,Keywords,Geography,Parent Legislation,Comment,CPR Document ID,CPR Family ID,CPR Collection ID,CPR Family Slug,CPR Document Slug,Document variant,CPR Document Status
1001,0,Test1,FALSE,FALSE,N/A,Collection1,CollectionSummary1,Title1,Fam1,Summary1,,MAIN,,GBR,http://somewhere|en,executive,02/02/2014|Law passed,Energy,,,Mitigation,,Order,,,Energy Supply,Algeria,,,CCLW.executive.1.2,CCLW.family.1001.0,CPR.Collection.1,FamSlug1,DocSlug1,,PUBLISHED
2002,0,Test2,FALSE,FALSE,N/A,Collection1,CollectionSummary1,Title2,Fam2,Summary2,,MAIN,,GBR,http://another_somewhere|en,executive,03/03/2024|Law passed,Energy,,,Mitigation,,Order,,,Energy Supply,Algeria,,,CCLW.executive.2.2,CCLW.family.2002.0,CPR.Collection.1,FamSlug2,DocSlug2,,PUBLISHED
"""

TWO_DFC_ROW_DIFFERENT_ORG = """ID,Document ID,CCLW Description,Part of collection?,Create new family/ies?,Collection ID,Collection name,Collection summary,Document title,Family name,Family summary,Family ID,Document role,Applies to ID,Geography ISO,Documents,Category,Events,Sectors,Instruments,Frameworks,Responses,Natural Hazards,Document Type,Year,Language,Keywords,Geography,Parent Legislation,Comment,CPR Document ID,CPR Family ID,CPR Collection ID,CPR Family Slug,CPR Document Slug,Document variant,CPR Document Status
1001,0,Test1,FALSE,FALSE,N/A,Collection1,CollectionSummary1,Title1,Fam1,Summary1,,MAIN,,GBR,http://somewhere|en,executive,02/02/2014|Law passed,Energy,,,Mitigation,,Order,,,Energy Supply,Algeria,,,CCLW.executive.1.2,CCLW.family.1001.0,CPR.Collection.1,FamSlug1,DocSlug1,,PUBLISHED
2002,0,Test2,FALSE,FALSE,N/A,Collection1,CollectionSummary1,Title2,Fam2,Summary2,,MAIN,,GBR,http://another_somewhere|en,executive,03/03/2024|Law passed,Energy,,,Mitigation,,Order,,,Energy Supply,Algeria,,,UNFCCC.non-party.2.2,CCLW.family.2002.0,CPR.Collection.1,FamSlug2,DocSlug2,,PUBLISHED
"""

TWO_DFC_ROW_NON_MATCHING_IDS = """ID,Document ID,CCLW Description,Part of collection?,Create new family/ies?,Collection ID,Collection name,Collection summary,Document title,Family name,Family summary,Family ID,Document role,Applies to ID,Geography ISO,Documents,Category,Events,Sectors,Instruments,Frameworks,Responses,Natural Hazards,Document Type,Year,Language,Keywords,Geography,Parent Legislation,Comment,CPR Document ID,CPR Family ID,CPR Collection ID,CPR Family Slug,CPR Document Slug,Document variant,CPR Document Status
1001,0,Test1,FALSE,FALSE,N/A,Collection1,CollectionSummary1,Title1,Fam1,Summary1,,MAIN,,GBR,http://somewhere|en,executive,02/02/2014|Law passed,Energy,,,Mitigation,,Order,,,Energy Supply,Algeria,,,CCLW.executive.12,CCLW.family.1001.0,CPR.Collection.1,FamSlug1,DocSlug1,,PUBLISHED
2002,0,Test2,FALSE,FALSE,N/A,Collection1,CollectionSummary1,Title2,Fam2,Summary2,,MAIN,,GBR,http://another_somewhere|en,executive,03/03/2024|Law passed,Energy,,,Mitigation,,Order,,,Energy Supply,Algeria,,,UNFCCC.s.ill.y.2.2,CCLW.family.2002.0,CPR.Collection.1,FamSlug2,DocSlug2,,PUBLISHED
"""

ONE_DFC_ROW_TWO_LANGUAGES = """ID,Document ID,CCLW Description,Part of collection?,Create new family/ies?,Collection ID,Collection name,Collection summary,Document title,Family name,Family summary,Family ID,Document role,Applies to ID,Geography ISO,Documents,Category,Events,Sectors,Instruments,Frameworks,Responses,Natural Hazards,Document Type,Year,Language,Keywords,Geography,Parent Legislation,Comment,CPR Document ID,CPR Family ID,CPR Collection ID,CPR Family Slug,CPR Document Slug,Document variant,CPR Document Status
1001,0,Test1,FALSE,FALSE,N/A,Collection1,CollectionSummary1,Title1,Fam1,Summary1,,MAIN,,GBR,http://somewhere|en,executive,02/02/2014|Law passed,Energy,,,Mitigation,,Order,,French;English,Energy Supply,Algeria,,,CCLW.executive.1.2,CCLW.family.1001.0,CPR.Collection.1,FamSlug1,DocSlug1,Translation,PUBLISHED
"""

ONE_EVENT_ROW = """Id,Eventable type,Eventable Id,Eventable name,Event type,Title,Description,Date,Url,CPR Event ID,CPR Family ID,Event Status
1101,Legislation,1001,Title1,Passed/Approved,Published,,2019-12-25,,CCLW.legislation_event.1101.0,CCLW.family.1001.0,OK
"""

TWO_DFC_ROW_ONE_LANGUAGE = """ID,Document ID,CCLW Description,Part of collection?,Create new family/ies?,Collection ID,Collection name,Collection summary,Document title,Family name,Family summary,Family ID,Document role,Applies to ID,Geography ISO,Documents,Category,Events,Sectors,Instruments,Frameworks,Responses,Natural Hazards,Document Type,Year,Language,Keywords,Geography,Parent Legislation,Comment,CPR Document ID,CPR Family ID,CPR Collection ID,CPR Family Slug,CPR Document Slug,Document variant,CPR Document Status
1001,0,Test1,FALSE,FALSE,N/A,Collection1,CollectionSummary1,Title1,Fam1,Summary1,,MAIN,,GBR,http://somewhere|en,executive,02/02/2014|Law passed,Energy,,,Mitigation,,Order,,English,Energy Supply,Algeria,,,CCLW.executive.1.2,CCLW.family.1001.0,CPR.Collection.1,FamSlug1,DocSlug1,,PUBLISHED
2002,0,Test2,FALSE,FALSE,N/A,Collection2,CollectionSummary2,Title2,Fam2,Summary2,,MAIN,,GBR,http://another_somewhere|en,executive,03/03/2024|Law passed,Energy,,,Mitigation,,Order,,,Energy Supply,Algeria,,,CCLW.executive.2.2,CCLW.family.2002.0,CPR.Collection.2,FamSlug2,DocSlug2,,PUBLISHED
"""

TWO_EVENT_ROWS = """Id,Eventable type,Eventable Id,Eventable name,Event type,Title,Description,Date,Url,CPR Event ID,CPR Family ID,Event Status
1101,Legislation,1001,Title1,Passed/Approved,Published,,2019-12-25,,CCLW.legislation_event.1101.0,CCLW.family.1001.0,OK
2202,Legislation,2002,Title2,Passed/Approved,Published,,2019-12-25,,CCLW.legislation_event.2202.0,CCLW.family.2002.0,OK
"""


def setup_with_docs(test_db, mocker):
    mock_s3 = mocker.patch("app.core.aws.S3Client")

    populate_geography(test_db)
    populate_taxonomy(test_db)
    populate_event_type(test_db)
    populate_document_type(test_db)
    populate_document_role(test_db)
    populate_document_variant(test_db)
    populate_language(test_db)
    test_db.commit()

    _start_ingest(test_db, mock_s3, "s3_prefix", ONE_DFC_ROW, ONE_EVENT_ROW)
    test_db.commit()


def setup_with_two_docs(test_db, mocker):
    setup_with_multiple_docs(
        test_db, mocker, doc_data=TWO_DFC_ROW, event_data=TWO_EVENT_ROWS
    )


def setup_with_multiple_docs(test_db, mocker, doc_data, event_data):
    mock_s3 = mocker.patch("app.core.aws.S3Client")

    populate_geography(test_db)
    populate_taxonomy(test_db)
    populate_event_type(test_db)
    populate_document_type(test_db)
    populate_document_role(test_db)
    populate_document_variant(test_db)
    populate_language(test_db)
    test_db.commit()

    _start_ingest(test_db, mock_s3, "s3_prefix", doc_data, event_data)
    test_db.commit()
