import csv
from io import StringIO
from sqlalchemy.orm import Session
from datetime import datetime
from app.data_migrations import (
    populate_category,
    populate_document_type,
    populate_geography,
    populate_source,
    populate_taxonomy,
    populate_language,
)
from app.db.models.deprecated.document import Document
from app.db.models.law_policy.family import Slug


THREE_ROWS = """ID,Document ID,CCLW Description,Part of collection?,Create new family/ies?,Collection ID,Collection name,Collection summary,Document title,Family name,Family summary,Family ID,Document role,Applies to ID,Geography ISO,Documents,Category,Events,Sectors,Instruments,Frameworks,Responses,Natural Hazards,Document Type,Year,Language,Keywords,Geography,Parent Legislation,Comment,CPR Document ID,CPR Family ID,CPR Collection ID,CPR Family Slug,CPR Document Slug
1001,0,Test1,FALSE,FALSE,N/A,Collection1,CollectionSummary1,Title1,Fam1,Summary1,,MAIN,,DZA,http://place1|en,executive,02/02/2014|Law passed,Energy,,,Mitigation,,Order,,,Energy Supply,Algeria,,,CCLW.executive.1001.0,CCLW.family.1001.0,CPR.Collection.1,FamSlug1,DocSlug1
1002,0,Test2,FALSE,FALSE,N/A,N/A,N/A,Title2,Fam2,Summary2,,MAIN,,DZA,http://place1|en;http://place2|fr,executive,28/04/2013|Law passed||,Energy;LULUCF;Social development;Transportation;Urban;Waste,"Processes, plans and strategies|Governance",Adaptation;Mitigation,Adaptation;Mitigation,,Plan,,,Adaptation;Energy Supply;Energy Demand;Redd+ And Lulucf;Transportation,Algeria,,,CCLW.executive.1002.0,CCLW.family.1002.0,N/A,FamSlug2,DocSlug2
1003,0,Test3,FALSE,FALSE,N/A,N/A,N/A,Title3,Fam3,Summary3,,MAIN,,DZA,,executive,08/12/2011|Law passed,Energy,Subsidies|Economic,,Mitigation,,Decree,,,Research And Development;Energy Supply,Algeria,,,CCLW.executive.1003.0,CCLW.family.1003.0,N/A,FamSlug3,DocSlug3
"""

THREE_ROWS_MISSING_FIELD = """ID,Document ID,CCLW Description,Part of collection?,Create new family/ies?,Collection ID,Collection name,Collection summary,Document title,Family name,Family summary,Family ID,Document role,Applies to ID,Geography ISO,Documents,Category,Events,Sectors,Instruments,Frameworks,Responses,Natural Hazards,Document Type,Year,Language,Keywords,Geography,Parent Legislation,Comment,CPR Document ID,CPR Family ID,CPR Collection ID,CPR Family Slug
1001,0,Test1,FALSE,FALSE,N/A,N/A,N/A,Title1,Fam1,Summary1,,MAIN,,DZA,,executive,02/02/2014|Law passed,Energy,,,Mitigation,,Order,,,Energy Supply,Algeria,,,CCLW.executive.1001.0,CCLW.family.1001.0,N/A,FamSlug1
1002,0,Test2,FALSE,FALSE,N/A,N/A,N/A,Title2,Fam2,Summary2,,MAIN,,DZA,,executive,28/04/2013|Law passed||,Energy;LULUCF;Social development;Transportation;Urban;Waste,"Processes, plans and strategies|Governance",Adaptation;Mitigation,Adaptation;Mitigation,,Plan,,,Adaptation;Energy Supply;Energy Demand;Redd+ And Lulucf;Transportation,Algeria,,,CCLW.executive.1002.0,CCLW.family.1002.0,N/A,FamSlug2
1003,0,Test3,FALSE,FALSE,N/A,N/A,N/A,Title3,Fam3,Summary3,,MAIN,,DZA,,executive,08/12/2011|Law passed,Energy,Subsidies|Economic,,Mitigation,,Decree,,,Research And Development;Energy Supply,Algeria,,,CCLW.executive.1003.0,CCLW.family.1003.0,N/A,FamSlug3
"""

THREE_ROWS_BAD_META = """ID,Document ID,CCLW Description,Part of collection?,Create new family/ies?,Collection ID,Collection name,Collection summary,Document title,Family name,Family summary,Family ID,Document role,Applies to ID,Geography ISO,Documents,Category,Events,Sectors,Instruments,Frameworks,Responses,Natural Hazards,Document Type,Year,Language,Keywords,Geography,Parent Legislation,Comment,CPR Document ID,CPR Family ID,CPR Collection ID,CPR Family Slug,CPR Document Slug
1001,0,Test1,FALSE,FALSE,N/A,Collection1,CollectionSummary1,Title1,Fam1,Summary1,,MAIN,,DZA,http://place1|en;http://place2|fr,executive,02/02/2014|Law passed,Medical,,,Mitigation,,Order,,,Energy Supply,Algeria,,,CCLW.executive.1001.0,CCLW.family.1001.0,CPR.Collection.1,FamSlug1,DocSlug1
1002,0,Test2,FALSE,FALSE,N/A,N/A,N/A,Title2,Fam2,Summary2,,MAIN,,DZA,,executive,28/04/2013|Law passed||,Energy;LULUCF;Social development;Transportation;Urban;Waste,Oboe,Adaptation;Mitigation,Adaptation;Mitigation,,Plan,,,Adaptation;Energy Supply;Energy Demand;Redd+ And Lulucf;Transportation,Algeria,,,CCLW.executive.1002.0,CCLW.family.1002.0,N/A,FamSlug2,DocSlug2
1003,0,Test3,FALSE,FALSE,N/A,N/A,N/A,Title3,Fam3,Summary3,,MAIN,,DZA,,executive,08/12/2011|Law passed,Energy,Subsidies|Economic,,Hilarity,,Decree,,,Research And Development;Energy Supply,Algeria,,,CCLW.executive.1003.0,CCLW.family.1003.0,N/A,FamSlug3,DocSlug3
"""

ALPHABETICAL_COLUMNS = [
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

SLUG_FAMILY_NAME = "FamSlug1"
FAMILY_IMPORT_ID = "CCLW.family.1001.0"
DOCUMENT_TITLE = "Title1"
DOCUMENT_IMPORT_ID = "CCLW.executive.1001.0"
SLUG_DOCUMENT_NAME = "DocSlug1"
COLLECTION_IMPORT_ID = "CPR.Collection.1"


def get_ingest_row_data(num: int, contents: str = THREE_ROWS) -> dict[str, str]:
    """
    Gets the indexed row data.

    Args:
        num (int): Index into the THREE_ROWS string

    Returns:
        dict[str, str]: the IngestRow at this index
    """
    reader = csv.DictReader(StringIO(initial_value=contents))
    for i, row in enumerate(reader):
        if i == num:
            return row
    return {}


def init_for_ingest(test_db: Session):
    populate_taxonomy(test_db)
    populate_geography(test_db)
    populate_source(test_db)
    populate_category(test_db)
    populate_document_type(test_db)
    populate_language(test_db)
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
