from sqlalchemy.orm import Session

from tests.routes.dfce_helpers import (
    add_collections,
    add_families,
    link_collection_family,
)


# ONE_DFC_ROW = """ID,Document ID,CCLW Description,Part of collection?,Create new family/ies?,Collection ID,Collection name,Collection summary,Document title,Family name,Family summary,Family ID,Document role,Applies to ID,Geography ISO,Documents,Category,Events,Sectors,Instruments,Frameworks,Responses,Natural Hazards,Document Type,Year,Language,Keywords,Geography,Parent Legislation,Comment,CPR Document ID,CPR Family ID,CPR Collection ID,CPR Family Slug,CPR Document Slug,Document variant,CPR Document Status
# 1001,0,Test1,FALSE,FALSE,N/A,Collection1,CollectionSummary1,Title1,Fam1,Summary1,,MAIN,,GBR,http://somewhere|en,executive,02/02/2014|Law passed,Energy,,,Mitigation,,Order,,,Energy Supply,Algeria,,,CCLW.executive.1.2,CCLW.family.1001.0,CPR.Collection.1,FamSlug1,DocSlug1,Translation,PUBLISHED
# """

# ONE_EVENT_ROW = """Id,Eventable type,Eventable Id,Eventable name,Event type,Title,Description,Date,Url,CPR Event ID,CPR Family ID,Event Status
# 1101,Legislation,1001,Title1,Passed/Approved,Published,,2019-12-25,,CCLW.legislation_event.1101.0,CCLW.family.1001.0,OK
# """

# TWO_DFC_ROW = """ID,Document ID,CCLW Description,Part of collection?,Create new family/ies?,Collection ID,Collection name,Collection summary,Document title,Family name,Family summary,Family ID,Document role,Applies to ID,Geography ISO,Documents,Category,Events,Sectors,Instruments,Frameworks,Responses,Natural Hazards,Document Type,Year,Language,Keywords,Geography,Parent Legislation,Comment,CPR Document ID,CPR Family ID,CPR Collection ID,CPR Family Slug,CPR Document Slug,Document variant,CPR Document Status
# 1001,0,Test1,FALSE,FALSE,N/A,Collection1,CollectionSummary1,Title1,Fam1,Summary1,,MAIN,,GBR,http://somewhere|en,executive,02/02/2014|Law passed,Energy,,,Mitigation,,Order,,,Energy Supply,Algeria,,,CCLW.executive.1.2,CCLW.family.1001.0,CPR.Collection.1,FamSlug1,DocSlug1,,PUBLISHED
# 2002,0,Test2,FALSE,FALSE,N/A,Collection1,CollectionSummary1,Title2,Fam2,Summary2,,MAIN,,GBR,http://another_somewhere|en,executive,03/03/2024|Law passed,Energy,,,Mitigation,,Order,,,Energy Supply,Algeria,,,CCLW.executive.2.2,CCLW.family.2002.0,CPR.Collection.1,FamSlug2,DocSlug2,,PUBLISHED
# """

# TWO_DOCS_ONE_FAM = """ID,Document ID,CCLW Description,Part of collection?,Create new family/ies?,Collection ID,Collection name,Collection summary,Document title,Family name,Family summary,Family ID,Document role,Applies to ID,Geography ISO,Documents,Category,Events,Sectors,Instruments,Frameworks,Responses,Natural Hazards,Document Type,Year,Language,Keywords,Geography,Parent Legislation,Comment,CPR Document ID,CPR Family ID,CPR Collection ID,CPR Family Slug,CPR Document Slug,Document variant,CPR Document Status
# 1001,0,Test1,FALSE,FALSE,N/A,Collection1,CollectionSummary1,Title1,Fam1,Summary1,,MAIN,,GBR,http://somewhere|en,executive,02/02/2014|Law passed,Energy,,,Mitigation,,Order,,,Energy Supply,Algeria,,,CCLW.executive.1.2,CCLW.family.1001.0,CPR.Collection.1,FamSlug1,DocSlug1,,PUBLISHED
# 2002,0,Test2,FALSE,FALSE,N/A,Collection1,CollectionSummary1,Title2,Fam1,Summary2,,MAIN,,GBR,http://another_somewhere|en,executive,03/03/2024|Law passed,Energy,,,Mitigation,,Order,,,Energy Supply,Algeria,,,CCLW.executive.2.2,CCLW.family.1001.0,CPR.Collection.1,FamSlug1,DocSlug2,,PUBLISHED
# """

# TWO_DFC_ROW_DIFFERENT_ORG = """ID,Document ID,CCLW Description,Part of collection?,Create new family/ies?,Collection ID,Collection name,Collection summary,Document title,Family name,Family summary,Family ID,Document role,Applies to ID,Geography ISO,Documents,Category,Events,Sectors,Instruments,Frameworks,Responses,Natural Hazards,Document Type,Year,Language,Keywords,Geography,Parent Legislation,Comment,CPR Document ID,CPR Family ID,CPR Collection ID,CPR Family Slug,CPR Document Slug,Document variant,CPR Document Status
# 1001,0,Test1,FALSE,FALSE,N/A,Collection1,CollectionSummary1,Title1,Fam1,Summary1,,MAIN,,GBR,http://somewhere|en,executive,02/02/2014|Law passed,Energy,,,Mitigation,,Order,,,Energy Supply,Algeria,,,CCLW.executive.1.2,CCLW.family.1001.0,CPR.Collection.1,FamSlug1,DocSlug1,,PUBLISHED
# 2002,0,Test2,FALSE,FALSE,N/A,Collection1,CollectionSummary1,Title2,Fam2,Summary2,,MAIN,,GBR,http://another_somewhere|en,executive,03/03/2024|Law passed,Energy,,,Mitigation,,Order,,,Energy Supply,Algeria,,,UNFCCC.non-party.2.2,CCLW.family.2002.0,CPR.Collection.1,FamSlug2,DocSlug2,,PUBLISHED
# """

# TWO_DFC_ROW_NON_MATCHING_IDS = """ID,Document ID,CCLW Description,Part of collection?,Create new family/ies?,Collection ID,Collection name,Collection summary,Document title,Family name,Family summary,Family ID,Document role,Applies to ID,Geography ISO,Documents,Category,Events,Sectors,Instruments,Frameworks,Responses,Natural Hazards,Document Type,Year,Language,Keywords,Geography,Parent Legislation,Comment,CPR Document ID,CPR Family ID,CPR Collection ID,CPR Family Slug,CPR Document Slug,Document variant,CPR Document Status
# 1001,0,Test1,FALSE,FALSE,N/A,Collection1,CollectionSummary1,Title1,Fam1,Summary1,,MAIN,,GBR,http://somewhere|en,executive,02/02/2014|Law passed,Energy,,,Mitigation,,Order,,,Energy Supply,Algeria,,,CCLW.executive.12,CCLW.family.1001.0,CPR.Collection.1,FamSlug1,DocSlug1,,PUBLISHED
# 2002,0,Test2,FALSE,FALSE,N/A,Collection1,CollectionSummary1,Title2,Fam2,Summary2,,MAIN,,GBR,http://another_somewhere|en,executive,03/03/2024|Law passed,Energy,,,Mitigation,,Order,,,Energy Supply,Algeria,,,UNFCCC.s.ill.y.2.2,CCLW.family.2002.0,CPR.Collection.1,FamSlug2,DocSlug2,,PUBLISHED
# """

# ONE_DFC_ROW_TWO_LANGUAGES = """ID,Document ID,CCLW Description,Part of collection?,Create new family/ies?,Collection ID,Collection name,Collection summary,Document title,Family name,Family summary,Family ID,Document role,Applies to ID,Geography ISO,Documents,Category,Events,Sectors,Instruments,Frameworks,Responses,Natural Hazards,Document Type,Year,Language,Keywords,Geography,Parent Legislation,Comment,CPR Document ID,CPR Family ID,CPR Collection ID,CPR Family Slug,CPR Document Slug,Document variant,CPR Document Status
# 1001,0,Test1,FALSE,FALSE,N/A,Collection1,CollectionSummary1,Title1,Fam1,Summary1,,MAIN,,GBR,http://somewhere|en,executive,02/02/2014|Law passed,Energy,,,Mitigation,,Order,,French;English,Energy Supply,Algeria,,,CCLW.executive.1.2,CCLW.family.1001.0,CPR.Collection.1,FamSlug1,DocSlug1,Translation,PUBLISHED
# """

# ONE_EVENT_ROW = """Id,Eventable type,Eventable Id,Eventable name,Event type,Title,Description,Date,Url,CPR Event ID,CPR Family ID,Event Status
# 1101,Legislation,1001,Title1,Passed/Approved,Published,,2019-12-25,,CCLW.legislation_event.1101.0,CCLW.family.1001.0,OK
# """

# TWO_DFC_ROW_ONE_LANGUAGE = """ID,Document ID,CCLW Description,Part of collection?,Create new family/ies?,Collection ID,Collection name,Collection summary,Document title,Family name,Family summary,Family ID,Document role,Applies to ID,Geography ISO,Documents,Category,Events,Sectors,Instruments,Frameworks,Responses,Natural Hazards,Document Type,Year,Language,Keywords,Geography,Parent Legislation,Comment,CPR Document ID,CPR Family ID,CPR Collection ID,CPR Family Slug,CPR Document Slug,Document variant,CPR Document Status
# 1001,0,Test1,FALSE,FALSE,N/A,Collection1,CollectionSummary1,Title1,Fam1,Summary1,,MAIN,,GBR,http://somewhere|en,executive,02/02/2014|Law passed,Energy,,,Mitigation,,Order,,English,Energy Supply,Algeria,,,CCLW.executive.1.2,CCLW.family.1001.0,CPR.Collection.1,FamSlug1,DocSlug1,,PUBLISHED
# 2002,0,Test2,FALSE,FALSE,N/A,Collection2,CollectionSummary2,Title2,Fam2,Summary2,,MAIN,,GBR,http://another_somewhere|en,executive,03/03/2024|Law passed,Energy,,,Mitigation,,Order,,,Energy Supply,Algeria,,,CCLW.executive.2.2,CCLW.family.2002.0,CPR.Collection.2,FamSlug2,DocSlug2,,PUBLISHED
# """

# TWO_EVENT_ROWS = """Id,Eventable type,Eventable Id,Eventable name,Event type,Title,Description,Date,Url,CPR Event ID,CPR Family ID,Event Status
# 1101,Legislation,1001,Title1,Passed/Approved,Published,,2019-12-25,,CCLW.legislation_event.1101.0,CCLW.family.1001.0,OK
# 2202,Legislation,2002,Title2,Passed/Approved,Published,,2019-12-25,,CCLW.legislation_event.2202.0,CCLW.family.2002.0,OK
# """

collection1 = {
    "import_id": "CPR.Collection.1.0",
    "title": "Collection1",
    "description": "CollectionSummary1",
}

collection2 = {
    "import_id": "CPR.Collection.2.0",
    "title": "Collection2",
    "description": "CollectionSummary2",
}

event1 = {
    "import_id": "CPR.Event.1.0",
    "title": "Published",
    "date": "2019-12-25",
    "type": "Passed/Approved",
    "status": "OK",
}

event2 = {
    "import_id": "CPR.Event.2.0",
    "title": "Published",
    "date": "2019-12-25",
    "type": "Passed/Approved",
    "status": "OK",
}

document1 = {
    "title": "Document1",
    "slug": "DocSlug1",
    "md5_sum": "111",
    "url": "http://somewhere1",
    "content_type": "application/pdf",
    "import_id": "CCLW.executive.1.2",
    "language_variant": "Original Language",
    "status": "PUBLISHED",
    "role": "MAIN",
    "type": "Plan",
    "languages": ["eng"],
    "events": [event1],
}

document2 = {
    "title": "Document2",
    "slug": "DocSlug2",
    "md5_sum": None,
    "url": "http://another_somewhere",
    "content_type": None,
    "import_id": "CCLW.executive.2.2",
    "language_variant": None,
    "status": "PUBLISHED",
    "role": "MAIN",
    "type": "Order",
    "languages": [],
    "events": [event2],
}


family1 = {
    "import_id": "CCLW.family.1001.0",
    "title": "Fam1",
    "slug": "FamSlug1",
    "description": "Summary1",
    "geography_id": 1,
    "category": "Executive",
    "documents": [document1],
}

family2 = {
    "import_id": "CCLW.family.2002.0",
    "title": "Fam2",
    "slug": "FamSlug2",
    "description": "Summary2",
    "geography_id": 1,
    "category": "Executive",
    "documents": [document2],
}


def setup_with_docs(db: Session):
    # _start_ingest(test_db, ONE_DFC_ROW, ONE_EVENT_ROW)
    # Collection
    add_collections(db, collections=[collection1])

    # Family + Document + events
    add_families(db, families=[family1])

    # Collection - Family
    link_collection_family(
        db,
        [
            (collection1["import_id"], family1["import_id"]),
        ],
    )


def setup_with_two_docs(db: Session):
    """Creates 2 DFCEs"""

    # setup_with_multiple_docs(test_db, doc_data=TWO_DFC_ROW, event_data=TWO_EVENT_ROWS)

    # Collection
    add_collections(db, collections=[collection1])

    # Family + Document + events
    add_families(db, families=[family1, family2])

    # Collection - Family
    link_collection_family(
        db,
        [
            (collection1["import_id"], family1["import_id"]),
            (collection1["import_id"], family2["import_id"]),
        ],
    )


def setup_with_two_docs_one_family(db: Session):
    # setup_with_multiple_docs(
    #     test_db, doc_data=TWO_DOCS_ONE_FAM, event_data=ONE_EVENT_ROW
    # )
    # Collection
    add_collections(db, collections=[collection1])

    # Family + Document + events
    family1["documents"] = [document1, document2]
    add_families(db, families=[family1])

    # Collection - Family
    link_collection_family(
        db,
        [
            (collection1["import_id"], family1["import_id"]),
        ],
    )


def setup_with_two_docs_multiple_languages(db: Session):
    document1["languages"] = ["fra", "eng"]
    # Collection
    add_collections(db, collections=[collection1])

    # Family + Document + events
    add_families(db, families=[family1, family2])

    # Collection - Family
    link_collection_family(
        db,
        [
            (collection1["import_id"], family1["import_id"]),
            (collection1["import_id"], family2["import_id"]),
        ],
    )


def setup_with_two_docs_bad_ids(db: Session):
    document1["import_id"] = "CCLW.executive.12"
    document2["import_id"] = "UNFCCC.s.ill.y.2.2"
    # Collection
    add_collections(db, collections=[collection1])

    # Family + Document + events
    add_families(db, families=[family1, family2])

    # Collection - Family
    link_collection_family(
        db,
        [
            (collection1["import_id"], family1["import_id"]),
            (collection1["import_id"], family2["import_id"]),
        ],
    )


def setup_with_two_unpublished_docs(db: Session):
    document1["status"] = "CREATED"
    document2["status"] = "DELETED"
    # Collection
    add_collections(db, collections=[collection1])

    # Family + Document + events
    add_families(db, families=[family1, family2])

    # Collection - Family
    link_collection_family(
        db,
        [
            (collection1["import_id"], family1["import_id"]),
            (collection1["import_id"], family2["import_id"]),
        ],
    )


def setup_docs_with_two_orgs(db: Session):
    document2["import_id"] = "UNFCCC.non-party.2.2"

    # Family + Document + events
    add_families(db, families=[family1], org_id=1)
    add_families(db, families=[family2], org_id=2)


def setup_docs_with_two_orgs_no_langs(db: Session):
    document2["import_id"] = "UNFCCC.non-party.2.2"
    document1["languages"] = []
    document2["languages"] = []

    # Family + Document + events
    add_families(db, families=[family1], org_id=1)
    add_families(db, families=[family2], org_id=2)
