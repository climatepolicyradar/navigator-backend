from datetime import datetime
from typing import Callable, Generator
from sqlalchemy.orm import Session

from fastapi.testclient import TestClient
from pytest_mock import MockerFixture
from app.api.api_v1.routers.admin import _start_ingest
from app.data_migrations import populate_event_type, populate_taxonomy
from app.db.models.deprecated.document import (
    Category,
    Document,
    DocumentType,
    Framework,
    Hazard,
    Instrument,
    Keyword,
    Response,
    Sector,
)
from app.db.models.deprecated.source import Source
from app.db.models.document.physical_document import Language, PhysicalDocument
from app.db.models.law_policy.family import Family, FamilyEvent
from app.db.models.law_policy.geography import Geography


ONE_DFC_ROW = """ID,Document ID,CCLW Description,Part of collection?,Create new family/ies?,Collection ID,Collection name,Collection summary,Document title,Family name,Family summary,Family ID,Document role,Applies to ID,Geography ISO,Documents,Category,Events,Sectors,Instruments,Frameworks,Responses,Natural Hazards,Document Type,Year,Language,Keywords,Geography,Parent Legislation,Comment,CPR Document ID,CPR Family ID,CPR Collection ID,CPR Family Slug,CPR Document Slug
1001,0,Test1,FALSE,FALSE,N/A,Collection1,CollectionSummary1,Title1,Fam1,Summary1,,MAIN,,GEO,http://somewhere|en,executive,02/02/2014|Law passed,Energy,,,Mitigation,,Order,,,Energy Supply,Algeria,,,CCLW.executive.1.2,CCLW.family.1001.0,CPR.Collection.1,FamSlug1,DocSlug1
"""

TWO_DFC_ROW = """ID,Document ID,CCLW Description,Part of collection?,Create new family/ies?,Collection ID,Collection name,Collection summary,Document title,Family name,Family summary,Family ID,Document role,Applies to ID,Geography ISO,Documents,Category,Events,Sectors,Instruments,Frameworks,Responses,Natural Hazards,Document Type,Year,Language,Keywords,Geography,Parent Legislation,Comment,CPR Document ID,CPR Family ID,CPR Collection ID,CPR Family Slug,CPR Document Slug
1001,0,Test1,FALSE,FALSE,N/A,Collection1,CollectionSummary1,Title1,Fam1,Summary1,,MAIN,,GEO,http://somewhere|en,executive,02/02/2014|Law passed,Energy,,,Mitigation,,Order,,,Energy Supply,Algeria,,,CCLW.executive.1.2,CCLW.family.1001.0,CPR.Collection.1,FamSlug1,DocSlug1
2002,0,Test2,FALSE,FALSE,N/A,Collection2,CollectionSummary2,Title2,Fam2,Summary2,,MAIN,,GEO,http://another_somewhere|en,executive,03/03/2024|Law passed,Energy,,,Mitigation,,Order,,,Energy Supply,Algeria,,,CCLW.executive.2.2,CCLW.family.2002.0,CPR.Collection.2,FamSlug2,DocSlug2
"""

TWO_EVENT_ROWS = """Id,Eventable type,Eventable Id,Eventable name,Event type,Title,Description,Date,Url,CPR Event ID,CPR Family ID,Event Status
1101,Legislation,1001,Title1,Passed/Approved,Published,,2019-12-25,,CCLW.legislation_event.1101.0,CCLW.family.1001.0,OK
"""


def setup_with_docs(test_db, mocker):
    mock_s3 = mocker.patch("app.core.aws.S3Client")

    populate_taxonomy(test_db)
    populate_event_type(test_db)
    test_db.commit()

    populate_old_documents(test_db)

    _start_ingest(test_db, mock_s3, "s3_prefix", ONE_DFC_ROW, TWO_EVENT_ROWS)
    test_db.commit()


def setup_with_two_docs(test_db, mocker):
    mock_s3 = mocker.patch("app.core.aws.S3Client")

    populate_taxonomy(test_db)
    populate_event_type(test_db)
    test_db.commit()

    populate_old_documents(test_db)

    _start_ingest(test_db, mock_s3, "s3_prefix", TWO_DFC_ROW, TWO_EVENT_ROWS)
    test_db.commit()


def populate_old_documents(test_db):
    test_db.add(Source(name="CCLW"))
    test_db.add(
        Geography(
            display_value="geography", slug="geography", value="GEO", type="country"
        )
    )
    test_db.add(DocumentType(name="doctype", description="doctype"))
    test_db.add(Language(language_code="LAN", name="language"))
    test_db.add(Category(name="Policy", description="Policy"))
    test_db.add(Keyword(name="keyword1", description="keyword1"))
    test_db.add(Keyword(name="keyword2", description="keyword2"))
    test_db.add(Hazard(name="hazard1", description="hazard1"))
    test_db.add(Hazard(name="hazard2", description="hazard2"))
    test_db.add(Response(name="topic", description="topic"))
    test_db.add(Framework(name="framework", description="framework"))

    test_db.commit()
    existing_doc_import_id = "CCLW.executive.1.2"
    test_db.add(Instrument(name="instrument", description="instrument", source_id=1))
    test_db.add(Sector(name="sector", description="sector", source_id=1))
    test_db.add(
        Document(
            publication_ts=datetime(year=2014, month=1, day=1),
            name="test",
            description="test description",
            source_url="http://somewhere",
            source_id=1,
            url="",
            cdn_object="",
            md5_sum=None,
            content_type=None,
            slug="geography_2014_test_1_2",
            import_id=existing_doc_import_id,
            geography_id=1,
            type_id=1,
            category_id=1,
        )
    )
    test_db.commit()
    test_db.add(
        Document(
            publication_ts=datetime(year=2014, month=1, day=1),
            name="test",
            description="test description",
            source_url="http://another_somewhere",
            source_id=1,
            url="",
            cdn_object="",
            md5_sum=None,
            content_type=None,
            slug="geography_2014_test_1_2",
            import_id="CCLW.executive.2.2",
            geography_id=1,
            type_id=1,
            category_id=1,
        )
    )
    test_db.commit()


def test_documents_with_preexisting_objects_not_found(
    client: TestClient,
    test_db: Session,
    mocker: Callable[..., Generator[MockerFixture, None, None]],
):
    setup_with_two_docs(test_db, mocker)
    assert test_db.query(Family).count() == 1
    assert test_db.query(FamilyEvent).count() == 1

    # Test associations
    response = client.get(
        "/api/v1/documents/FamSlug100?group_documents=True",
    )
    assert response.status_code == 404


def test_documents_with_preexisting_objects(
    client: TestClient,
    test_db: Session,
    mocker: Callable[..., Generator[MockerFixture, None, None]],
):
    setup_with_two_docs(test_db, mocker)
    assert test_db.query(PhysicalDocument).count() == 2
    assert test_db.query(Family).count() == 2
    assert test_db.query(FamilyEvent).count() == 1

    # Test associations
    response = client.get(
        "/api/v1/documents/FamSlug1?group_documents=True",
    )
    json_response = response.json()
    assert response.status_code == 200
    assert len(json_response) == 13
    assert json_response["organisation"] == "CCLW"
    assert json_response["title"] == "Fam1"
    assert json_response["summary"] == "Summary1"
    assert json_response["geography"] == "GEO"
    assert json_response["category"] == "Executive"
    assert json_response["status"] == "Published"
    assert json_response["published_date"] == "2019-12-25T00:00:00+00:00"
    assert json_response["last_updated_date"] == "2019-12-25T00:00:00+00:00"

    assert len(json_response["metadata"]) == 7
    assert json_response["metadata"]["keyword"] == ["Energy Supply"]

    assert len(json_response["slugs"]) == 1
    assert json_response["slugs"][0] == "FamSlug1"

    assert len(json_response["events"]) == 1
    assert json_response["events"][0]["title"] == "Published"

    assert len(json_response["documents"]) == 1
    assert json_response["documents"][0]["title"] == "Title1"
    assert json_response["documents"][0]["slugs"] == ["DocSlug1"]

    assert len(json_response["collections"]) == 1
    assert json_response["collections"][0]["title"] == "Collection1"

    # Test associations
    response = client.get(
        "/api/v1/documents/FamSlug2?group_documents=True",
    )
    json_response = response.json()
    assert response.status_code == 200
    assert len(json_response) == 13
    assert json_response["organisation"] == "CCLW"
    assert json_response["title"] == "Fam2"
    assert json_response["summary"] == "Summary2"
