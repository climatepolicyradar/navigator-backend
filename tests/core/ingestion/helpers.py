import csv
from io import StringIO
from sqlalchemy.orm import Session
from datetime import datetime
from app.data_migrations import (
    populate_category,
    populate_document_role,
    populate_document_variant,
    populate_document_type,
    populate_event_type,
    populate_geography,
    populate_source,
    populate_taxonomy,
    populate_language,
)
from app.db.models.deprecated.document import Document
from app.db.models.law_policy.family import Slug

THREE_DOC_ROWS = """ID,Document ID,CCLW Description,Part of collection?,Create new family/ies?,Collection ID,Collection name,Collection summary,Document title,Family name,Family summary,Family ID,Document role,Applies to ID,Geography ISO,Documents,Category,Events,Sectors,Instruments,Frameworks,Responses,Natural Hazards,Document Type,Year,Language,Keywords,Geography,Parent Legislation,Comment,CPR Document ID,CPR Family ID,CPR Collection ID,CPR Family Slug,CPR Document Slug
1001,0,Test1,FALSE,FALSE,N/A,Collection1,CollectionSummary1,Title1,Fam1,Summary1,,MAIN,,DZA,http://place1|en,executive,02/02/2014|Law passed,Energy,,,Mitigation,,Order,,,Energy Supply,Algeria,,,CCLW.executive.1001.0,CCLW.family.1001.0,CPR.Collection.1,FamSlug1,DocSlug1
1002,0,Test2,FALSE,FALSE,N/A,N/A,N/A,Title2,Fam2,Summary2,,MAIN,,DZA,http://place1|en;http://place2|fr,executive,28/04/2013|Law passed||,Energy;LULUCF;Social development;Transportation;Urban;Waste,"Processes, plans and strategies|Governance",Adaptation;Mitigation,Adaptation;Mitigation,,Plan,,,Adaptation;Energy Supply;Energy Demand;Redd+ And Lulucf;Transportation,Algeria,,,CCLW.executive.1002.0,CCLW.family.1002.0,N/A,FamSlug2,DocSlug2
1003,0,Test3,FALSE,FALSE,N/A,N/A,N/A,Title3,Fam3,Summary3,,MAIN,,DZA,,executive,08/12/2011|Law passed,Energy,Subsidies|Economic,,Mitigation,,Decree,,,Research And Development;Energy Supply,Algeria,,,CCLW.executive.1003.0,CCLW.family.1003.0,N/A,FamSlug3,DocSlug3
"""

THREE_DOC_ROWS_MISSING_FIELD = """ID,Document ID,CCLW Description,Part of collection?,Create new family/ies?,Collection ID,Collection name,Collection summary,Document title,Family name,Family summary,Family ID,Document role,Applies to ID,Geography ISO,Documents,Category,Events,Sectors,Instruments,Frameworks,Responses,Natural Hazards,Document Type,Year,Language,Keywords,Geography,Parent Legislation,Comment,CPR Document ID,CPR Family ID,CPR Collection ID,CPR Family Slug
1001,0,Test1,FALSE,FALSE,N/A,N/A,N/A,Title1,Fam1,Summary1,,MAIN,,DZA,,executive,02/02/2014|Law passed,Energy,,,Mitigation,,Order,,,Energy Supply,Algeria,,,CCLW.executive.1001.0,CCLW.family.1001.0,N/A,FamSlug1
1002,0,Test2,FALSE,FALSE,N/A,N/A,N/A,Title2,Fam2,Summary2,,MAIN,,DZA,,executive,28/04/2013|Law passed||,Energy;LULUCF;Social development;Transportation;Urban;Waste,"Processes, plans and strategies|Governance",Adaptation;Mitigation,Adaptation;Mitigation,,Plan,,,Adaptation;Energy Supply;Energy Demand;Redd+ And Lulucf;Transportation,Algeria,,,CCLW.executive.1002.0,CCLW.family.1002.0,N/A,FamSlug2
1003,0,Test3,FALSE,FALSE,N/A,N/A,N/A,Title3,Fam3,Summary3,,MAIN,,DZA,,executive,08/12/2011|Law passed,Energy,Subsidies|Economic,,Mitigation,,Decree,,,Research And Development;Energy Supply,Algeria,,,CCLW.executive.1003.0,CCLW.family.1003.0,N/A,FamSlug3
"""

THREE_DOC_ROWS_BAD_META = """ID,Document ID,CCLW Description,Part of collection?,Create new family/ies?,Collection ID,Collection name,Collection summary,Document title,Family name,Family summary,Family ID,Document role,Applies to ID,Geography ISO,Documents,Category,Events,Sectors,Instruments,Frameworks,Responses,Natural Hazards,Document Type,Year,Language,Keywords,Geography,Parent Legislation,Comment,CPR Document ID,CPR Family ID,CPR Collection ID,CPR Family Slug,CPR Document Slug
1001,0,Test1,FALSE,FALSE,N/A,Collection1,CollectionSummary1,Title1,Fam1,Summary1,,MAIN,,DZA,http://place1|en;http://place2|fr,executive,02/02/2014|Law passed,Medical,,,Mitigation,,Order,,,Energy Supply,Algeria,,,CCLW.executive.1001.0,CCLW.family.1001.0,CPR.Collection.1,FamSlug1,DocSlug1
1002,0,Test2,FALSE,FALSE,N/A,N/A,N/A,Title2,Fam2,Summary2,,MAIN,,DZA,,executive,28/04/2013|Law passed||,Energy;LULUCF;Social development;Transportation;Urban;Waste,Oboe,Adaptation;Mitigation,Adaptation;Mitigation,,Plan,,,Adaptation;Energy Supply;Energy Demand;Redd+ And Lulucf;Transportation,Algeria,,,CCLW.executive.1002.0,CCLW.family.1002.0,N/A,FamSlug2,DocSlug2
1003,0,Test3,FALSE,FALSE,N/A,N/A,N/A,Title3,Fam3,Summary3,,MAIN,,DZA,,executive,08/12/2011|Law passed,Energy,Subsidies|Economic,,Hilarity,,Decree,,,Research And Development;Energy Supply,Algeria,,,CCLW.executive.1003.0,CCLW.family.1003.0,N/A,FamSlug3,DocSlug3
"""

ALPHABETICAL_DOC_COLUMNS = [
    "CPR Collection ID",
    "CPR Document ID",
    "CPR Document Slug",
    "CPR Family ID",
    "CPR Family Slug",
    "Category",
    "Collection name",
    "Collection summary",
    "Document ID",
    "Document Type",
    "Document role",
    "Document title",
    "Documents",
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
    "Responses",
    "Sectors",
]

SLUG_FAMILY_NAME = "FamSlug1"
FAMILY_IMPORT_ID = "CCLW.family.1001.0"
EVENT_IMPORT_ID = "CCLW.legislation_event.1101.0"
DOCUMENT_TITLE = "Title1"
DOCUMENT_IMPORT_ID = "CCLW.executive.1001.0"
SLUG_DOCUMENT_NAME = "DocSlug1"
COLLECTION_IMPORT_ID = "CPR.Collection.1"

FOUR_EVENT_ROWS = """Id,Eventable type,Eventable Id,Eventable name,Event type,Title,Description,Date,Url,CPR Event ID,CPR Family ID,Event Status
1101,Legislation,1001,Title1,Passed/Approved,Published,,2019-12-25,,CCLW.legislation_event.1101.0,CCLW.family.1001.0,OK
1102,Legislation,1001,Title1,Entered Into Force,Entered into force,,2018-01-01,,CCLW.legislation_event.1102.1,CCLW.family.1001.0,DUPLICATED
1103,Legislation,1002,Title2,Passed/Approved,Approved,,2022-06-01,,CCLW.legislation_event.1103.0,CCLW.family.1002.0,OK
1104,Legislation,1003,Title3,Passed/Approved,launched,,2018-05-18,,CCLW.legislation_event.1104.0,CCLW.family.1003.0,OK
"""

ALPHABETICAL_EVENT_COLUMNS = [
    "CPR Event ID",
    "CPR Family ID",
    "Date",
    "Description",
    "Event Status",
    "Event type",
    "Eventable Id",
    "Eventable name",
    "Eventable type",
    "Id",
    "Title",
    "Url",
]


def _get_csv_row_data(num: int, contents: str) -> dict[str, str]:
    reader = csv.DictReader(StringIO(initial_value=contents))
    for i, row in enumerate(reader):
        if i == num:
            return row
    return {}


def get_doc_ingest_row_data(num: int, contents: str = THREE_DOC_ROWS) -> dict[str, str]:
    """
    Get the document ingest data at the indexed row.

    :param [int] num: Index into the THREE_ROWS string
    :param [str] contents: string to interpret as the content of a document ingest file
    :return [dict[str, str]]: the DocumentIngestRow at this index
    """
    return _get_csv_row_data(num, contents)


def get_event_ingest_row_data(
    num: int, contents: str = FOUR_EVENT_ROWS
) -> dict[str, str]:
    """
    Gets the event ingest data at the indexed row.

    :param [int] num: Index into the THREE_ROWS string
    :param [str] contents: string to interpret as the content of a document ingest file
    :return [dict[str, str]]: the DocumentIngestRow at this index
    """
    return _get_csv_row_data(num, contents)


def init_for_ingest(test_db: Session):
    populate_taxonomy(test_db)
    populate_geography(test_db)
    populate_source(test_db)
    populate_category(test_db)
    populate_document_type(test_db)
    populate_event_type(test_db)
    populate_language(test_db)
    populate_document_role(test_db)
    populate_document_variant(test_db)
    test_db.flush()
    test_db.add(
        Document(
            id=1,
            publication_ts=datetime.now(),
            name="test",
            description="",
            source_id=1,
            slug="slug1",
            import_id=DOCUMENT_IMPORT_ID,
            geography_id=2,
            type_id=1,
            category_id=1,
        )
    )
    test_db.flush()


def add_a_slug_for_family1_and_flush(db):
    slug = Slug(
        name="title_adb4",
        family_import_id=FAMILY_IMPORT_ID,
        family_document_import_id=None,
    )
    db.add(slug)
    db.flush()
    # NOTE: Creating the Slug is part of test init,
    #      as we need a Slug to query for the Family.
    slug = db.query(Slug).one()
    assert slug
