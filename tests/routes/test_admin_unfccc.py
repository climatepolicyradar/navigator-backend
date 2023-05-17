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


ONE_DFC_ROW = """id,md5sum,Submission type,Collection name,Collection ID,Family name,Document title,Documents,Author,Author type,Geography,Geography ISO,Date,Document role,Document variant,Language
1,00254c407297fbb50a77d748b817ee5c,Synthesis Report,,,Nationally determined contributions under the Paris Agreement. Revised note by the secretariat,Nationally determined contributions under the Paris Agreement. Revised note by the secretariat,https://unfccc.int/sites/default/files/resource/cma2021_08r01_S.pdf,UNFCCC Secretariat,Party,UK,GBR,2021-10-25T12:00:00Z,,,
"""

TWO_EVENT_ROWS = """Id,Eventable type,Eventable Id,Eventable name,Event type,Title,Description,Date,Url,CPR Event ID,CPR Family ID,Event Status
1101,Legislation,1001,Title1,Passed/Approved,Published,,2019-12-25,,CCLW.legislation_event.1101.0,CCLW.family.1001.0,OK
1102,Legislation,1001,Title1,Entered Into Force,Entered into force,,2018-01-01,,CCLW.legislation_event.1102.1,CCLW.family.1001.0,DUPLICATED
"""


def test_validate_bulk_ingest_unfccc_law_policy(
    client,
    superuser_token_headers,
    test_db,
):
    populate_taxonomy(test_db)
    populate_geography(test_db)
    populate_document_type(test_db)
    populate_document_role(test_db)
    populate_document_variant(test_db)
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
        "/api/v1/admin/bulk-ingest/validate/unfccc/law-policy",
        files=files,
        headers=superuser_token_headers,
    )
    assert response.status_code == 200
    response_json = response.json()
    assert len(response_json["errors"]) == 0
    assert (
        response_json["message"]
        == "Law & Policy validation result: 1 Rows, 0 Failures, 0 Resolved"
    )


# def test_bulk_ingest_unfccc_law_policy(
#     client,
#     superuser_token_headers,
#     test_db,
#     mocker,
# ):
#     mock_start_import = mocker.patch(
#         "app.api.api_v1.routers.unfccc_ingest._start_ingest"
#     )
#     mock_write_csv_to_s3 = mocker.patch(
#         "app.api.api_v1.routers.unfccc_ingest.write_csv_to_s3"
#     )

#     populate_geography(test_db)
#     populate_taxonomy(test_db)
#     populate_document_type(test_db)
#     populate_document_role(test_db)
#     populate_document_variant(test_db)
#     test_db.commit()

#     law_policy_csv_file = BytesIO(ONE_DFC_ROW.encode("utf8"))
#     events_csv_file = BytesIO(TWO_EVENT_ROWS.encode("utf8"))
#     files = {
#         "law_policy_csv": (
#             "valid_law_policy.csv",
#             law_policy_csv_file,
#             "text/csv",
#             {"Expires": "0"},
#         ),
#         "events_csv": (
#             "valid_events.csv",
#             events_csv_file,
#             "text/csv",
#             {"Expires": "0"},
#         ),
#     }
#     response = client.post(
#         "/api/v1/admin/bulk-ingest/unfccc/law-policy",
#         files=files,
#         headers=superuser_token_headers,
#     )
#     assert response.status_code == 202
#     response_json = response.json()
#     assert response_json["detail"] is None  # Not yet implemented

#     mock_start_import.assert_called_once()

#     assert mock_write_csv_to_s3.call_count == 2  # write docs & events csvs
#     call0 = mock_write_csv_to_s3.mock_calls[0]
#     assert len(call0.kwargs["file_contents"]) == law_policy_csv_file.getbuffer().nbytes
#     call1 = mock_write_csv_to_s3.mock_calls[1]
#     assert len(call1.kwargs["file_contents"]) == events_csv_file.getbuffer().nbytes
