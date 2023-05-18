from io import BytesIO

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


MISSING_COLL_UNFCCC_ROW = """Category,md5sum,Submission type,Family name,Document title,Documents,Author,Author type,Geography,Geography ISO,Date,Document role,Document variant,Language,CPR Collection ID,CPR Document ID,CPR Document Slug,CPR Family ID,CPR Family Slug
Cat1,00254c407297fbb50a77d748b817ee5c,Synthesis Report,Nationally determined contributions under the Paris Agreement. Revised note by the secretariat,Nationally determined contributions under the Paris Agreement. Revised note by the secretariat,https://unfccc.int/sites/default/files/resource/cma2021_08r01_S.pdf,UNFCCC Secretariat,Party,UK,GBR,2021-10-25T12:00:00Z,,,en,UNFCCC.Collection.1,UNFCCC.Document.1,Doc-slug,UNFCCC.family.1,Family-slug
"""

ONE_UNFCCC_ROW = """Category,md5sum,Submission type,Family name,Document title,Documents,Author,Author type,Geography,Geography ISO,Date,Document role,Document variant,Language,CPR Collection ID,CPR Document ID,CPR Document Slug,CPR Family ID,CPR Family Slug
Cat1,00254c407297fbb50a77d748b817ee5c,Synthesis Report,Nationally determined contributions under the Paris Agreement. Revised note by the secretariat,Nationally determined contributions under the Paris Agreement. Revised note by the secretariat,https://unfccc.int/sites/default/files/resource/cma2021_08r01_S.pdf,UNFCCC Secretariat,Party,UK,GBR,2021-10-25T12:00:00Z,,,en,UNFCCC.Collection.Found,UNFCCC.Document.1,Doc-slug,UNFCCC.family.1,Family-slug
"""

ZERO_COLLECTION_ROW = """CPR Collection ID,Collection name,Collection summary
"""

ONE_COLLECTION_ROW = """CPR Collection ID,Collection name,Collection summary
UNFCCC.Collection.Found,Collection One,Everything to do with testing
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
        "details": "The following Collection IDs were referenced and not defined: ['UNFCCC.Collection.Found']",
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
        "details": "The following Collection IDs were referenced and not defined: ['UNFCCC.Collection.1']",
        "type": "Error",
    }
    assert (
        response_json["message"]
        == "UNFCCC validation result: 1 Rows, 1 Failures, 0 Resolved"
    )


# def test_ingest_unfccc_works(
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
#     populate_taxonomy(test_db)
#     populate_geography(test_db)
#     populate_document_type(test_db)
#     populate_document_role(test_db)
#     populate_document_variant(test_db)
#     test_db.commit()
#     unfccc_data_csv = BytesIO(ONE_UNFCCC_ROW.encode("utf8"))
#     collection_csv = BytesIO(ONE_COLLECTION_ROW.encode("utf8"))
#     files = {
#         "unfccc_data_csv": (
#             "unfccc_data_csv.csv",
#             unfccc_data_csv,
#             "text/csv",
#             {"Expires": "0"},
#         ),
#         "collection_csv": (
#             "collection_csv.csv",
#             collection_csv,
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

#     mock_start_import.assert_called_once()

#     assert mock_write_csv_to_s3.call_count == 2  # write docs & events csvs
#     call0 = mock_write_csv_to_s3.mock_calls[0]
#     assert len(call0.kwargs["file_contents"]) == unfccc_data_csv.getbuffer().nbytes
#     call1 = mock_write_csv_to_s3.mock_calls[1]
#     assert len(call1.kwargs["file_contents"]) == collection_csv.getbuffer().nbytes
