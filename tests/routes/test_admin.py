from io import BytesIO

import pytest  # noqa: F401

from app.data_migrations import (
    populate_document_role,
    populate_document_type,
    populate_document_variant,
    populate_geography,
    populate_taxonomy,
)


def test_unauthenticated_ingest(client):
    response = client.post("/api/v1/admin/bulk-ingest/cclw/law-policy")
    assert response.status_code == 401


def test_unauthorized_ingest(client):
    response = client.post(
        "/api/v1/admin/bulk-ingest/cclw/law-policy",
    )
    assert response.status_code == 401


ONE_DFC_ROW = """ID,Document ID,CCLW Description,Part of collection?,Create new family/ies?,Collection ID,Collection name,Collection summary,Document title,Family name,Family summary,Family ID,Document role,Applies to ID,Geography ISO,Documents,Category,Events,Sectors,Instruments,Frameworks,Responses,Natural Hazards,Document Type,Year,Language,Keywords,Geography,Parent Legislation,Comment,CPR Document ID,CPR Family ID,CPR Collection ID,CPR Family Slug,CPR Document Slug,Document variant
1001,0,Test1,FALSE,FALSE,N/A,Collection1,CollectionSummary1,Title1,Fam1,Summary1,,MAIN,,GEO,http://somewhere|en,executive,02/02/2014|Law passed,Energy,,,Mitigation,,Order,,,Energy Supply,Algeria,,,CCLW.executive.1.2,CCLW.family.1001.0,CPR.Collection.1,FamSlug1,DocSlug1,Translation
"""

TWO_EVENT_ROWS = """Id,Eventable type,Eventable Id,Eventable name,Event type,Title,Description,Date,Url,CPR Event ID,CPR Family ID,Event Status
1101,Legislation,1001,Title1,Passed/Approved,Published,,2019-12-25,,CCLW.legislation_event.1101.0,CCLW.family.1001.0,OK
1102,Legislation,1001,Title1,Entered Into Force,Entered into force,,2018-01-01,,CCLW.legislation_event.1102.1,CCLW.family.1001.0,DUPLICATED
"""


def test_validate_bulk_ingest_cclw_law_policy(
    client,
    superuser_token_headers,
    test_db,
):
    populate_taxonomy(db=test_db)
    populate_geography(test_db)
    populate_document_type(db=test_db)
    populate_document_role(db=test_db)
    populate_document_variant(db=test_db)
    test_db.commit()
    law_policy_csv_file = BytesIO(ONE_DFC_ROW.encode("utf8"))
    files = {
        "law_policy_csv": (
            "valid_law_policy.csv",
            law_policy_csv_file,
            "text/csv",
            {"Expires": "0"},
        ),
    }
    response = client.post(
        "/api/v1/admin/bulk-ingest/validate/cclw/law-policy",
        files=files,
        headers=superuser_token_headers,
    )
    assert response.status_code == 200
    response_json = response.json()
    assert (
        response_json["message"]
        == "Law & Policy validation result: 1 Rows, 0 Failures, 0 Resolved"
    )
    assert len(response_json["errors"]) == 0


def test_bulk_ingest_cclw_law_policy(
    client,
    superuser_token_headers,
    test_db,
    mocker,
):
    mock_start_import = mocker.patch("app.api.api_v1.routers.admin._start_ingest")
    mock_write_csv_to_s3 = mocker.patch("app.api.api_v1.routers.admin.write_csv_to_s3")

    populate_geography(db=test_db)
    populate_taxonomy(db=test_db)
    populate_document_type(db=test_db)
    populate_document_role(db=test_db)
    populate_document_variant(db=test_db)
    test_db.commit()

    law_policy_csv_file = BytesIO(ONE_DFC_ROW.encode("utf8"))
    events_csv_file = BytesIO(TWO_EVENT_ROWS.encode("utf8"))
    files = {
        "law_policy_csv": (
            "valid_law_policy.csv",
            law_policy_csv_file,
            "text/csv",
            {"Expires": "0"},
        ),
        "events_csv": (
            "valid_events.csv",
            events_csv_file,
            "text/csv",
            {"Expires": "0"},
        ),
    }
    response = client.post(
        "/api/v1/admin/bulk-ingest/cclw/law-policy",
        files=files,
        headers=superuser_token_headers,
    )
    assert response.status_code == 202
    response_json = response.json()
    assert response_json["detail"] is None  # Not yet implemented

    mock_start_import.assert_called_once()

    assert mock_write_csv_to_s3.call_count == 2  # write docs & events csvs
    call0 = mock_write_csv_to_s3.mock_calls[0]
    assert len(call0.kwargs["file_contents"]) == law_policy_csv_file.getbuffer().nbytes
    call1 = mock_write_csv_to_s3.mock_calls[1]
    assert len(call1.kwargs["file_contents"]) == events_csv_file.getbuffer().nbytes
