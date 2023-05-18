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
    response = client.post("/api/v1/admin/bulk-ingest/unfccc")
    assert response.status_code == 401


def test_unauthorized_validation(client):
    response = client.post(
        "/api/v1/admin/bulk-ingest/validate/unfccc",
    )
    assert response.status_code == 401


MISSING_COLL_UNFCCC_ROW = """id,md5sum,Submission type,Collection ID,Family name,Document title,Documents,Author,Author type,Geography,Geography ISO,Date,Document role,Document variant,Language
1,00254c407297fbb50a77d748b817ee5c,Synthesis Report,Coll2,Nationally determined contributions under the Paris Agreement. Revised note by the secretariat,Nationally determined contributions under the Paris Agreement. Revised note by the secretariat,https://unfccc.int/sites/default/files/resource/cma2021_08r01_S.pdf,UNFCCC Secretariat,Party,UK,GBR,2021-10-25T12:00:00Z,,,
"""

ONE_UNFCCC_ROW = """id,md5sum,Submission type,Collection ID,Family name,Document title,Documents,Author,Author type,Geography,Geography ISO,Date,Document role,Document variant,Language
1,00254c407297fbb50a77d748b817ee5c,Synthesis Report,Coll1,Nationally determined contributions under the Paris Agreement. Revised note by the secretariat,Nationally determined contributions under the Paris Agreement. Revised note by the secretariat,https://unfccc.int/sites/default/files/resource/cma2021_08r01_S.pdf,UNFCCC Secretariat,Party,UK,GBR,2021-10-25T12:00:00Z,,,
"""

ZERO_COLLECTION_ROW = """Collection ID,Collection Name,Collection Summary
"""

ONE_COLLECTION_ROW = """Collection ID,Collection Name,Collection Summary
Coll1,Collection One,Everything to do with testing
"""


def test_validate_unfccc_works(
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
    unfccc_data_csv = BytesIO(ONE_UNFCCC_ROW.encode("utf8"))
    collection_csv = BytesIO(ONE_COLLECTION_ROW.encode("utf8"))
    files = {
        "unfccc_data_csv": (
            "unfccc_data_csv.csv",
            unfccc_data_csv,
            "text/csv",
            {"Expires": "0"},
        ),
        "collection_csv": (
            "collection_csv.csv",
            collection_csv,
            "text/csv",
            {"Expires": "0"},
        ),
    }
    response = client.post(
        "/api/v1/admin/bulk-ingest/validate/unfccc",
        files=files,
        headers=superuser_token_headers,
    )
    assert response.status_code == 200
    response_json = response.json()
    assert len(response_json["errors"]) == 0
    assert (
        response_json["message"]
        == "UNFCCC validation result: 1 Rows, 0 Failures, 0 Resolved"
    )


def test_validate_unfccc_fails_missing_defined_collection(
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
    unfccc_data_csv = BytesIO(ONE_UNFCCC_ROW.encode("utf8"))
    collection_csv = BytesIO(ZERO_COLLECTION_ROW.encode("utf8"))
    files = {
        "unfccc_data_csv": (
            "unfccc_data_csv.csv",
            unfccc_data_csv,
            "text/csv",
            {"Expires": "0"},
        ),
        "collection_csv": (
            "collection_csv.csv",
            collection_csv,
            "text/csv",
            {"Expires": "0"},
        ),
    }
    response = client.post(
        "/api/v1/admin/bulk-ingest/validate/unfccc",
        files=files,
        headers=superuser_token_headers,
    )
    assert response.status_code == 200
    response_json = response.json()
    assert len(response_json["errors"]) == 1
    assert response_json["errors"][0] == {
        "details": "The following Collection IDs were referenced and not defined: ['Coll1']",
        "type": "Error",
    }
    assert (
        response_json["message"]
        == "UNFCCC validation result: 1 Rows, 1 Failures, 0 Resolved"
    )


def test_validate_unfccc_fails_missing_referenced_collection(
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
    unfccc_data_csv = BytesIO(MISSING_COLL_UNFCCC_ROW.encode("utf8"))
    collection_csv = BytesIO(ZERO_COLLECTION_ROW.encode("utf8"))
    files = {
        "unfccc_data_csv": (
            "unfccc_data_csv.csv",
            unfccc_data_csv,
            "text/csv",
            {"Expires": "0"},
        ),
        "collection_csv": (
            "collection_csv.csv",
            collection_csv,
            "text/csv",
            {"Expires": "0"},
        ),
    }
    response = client.post(
        "/api/v1/admin/bulk-ingest/validate/unfccc",
        files=files,
        headers=superuser_token_headers,
    )
    assert response.status_code == 200
    response_json = response.json()
    assert len(response_json["errors"]) == 1
    assert response_json["errors"][0] == {
        "details": "The following Collection IDs were referenced and not defined: ['Coll2']",
        "type": "Error",
    }
    assert (
        response_json["message"]
        == "UNFCCC validation result: 1 Rows, 1 Failures, 0 Resolved"
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

#     law_policy_csv_file = BytesIO(ONE_UNFCCC_ROW.encode("utf8"))
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
#         "/api/v1/admin/bulk-ingest/unfccc",
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
