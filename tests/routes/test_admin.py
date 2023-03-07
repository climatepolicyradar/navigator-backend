import datetime
from io import BytesIO
from unittest.mock import patch

import pytest

from app.api.api_v1.routers.admin import ACCOUNT_ACTIVATION_EXPIRE_MINUTES
from app.data_migrations.populate_taxonomy import populate_taxonomy
from app.db.models.deprecated import (
    Source,
    Document,
    DocumentType,
    Language,
    Sector,
    Response,
    Hazard,
    Framework,
    Instrument,
    Category,
    Keyword,
    User,
    PasswordResetToken,
)
from app.db.models.law_policy import Geography
from tests.core.validation.cclw.test_law_policy import (
    INVALID_FILE_1,
    INVALID_CSV_MIXED_ERRORS,
    VALID_FILE_1,
)


def test_get_users(client, test_superuser, superuser_token_headers):
    response = client.get("/api/v1/admin/users", headers=superuser_token_headers)
    assert response.status_code == 200
    assert response.json() == [
        {
            "id": test_superuser.id,
            "email": test_superuser.email,
            "is_active": test_superuser.is_active,
            "is_superuser": test_superuser.is_superuser,
        }
    ]
    assert response.headers.get("Cache-Control") == "no-cache, no-store, private"


def test_deactivate_user(client, test_superuser, test_db, superuser_token_headers):
    response = client.delete(
        f"/api/v1/admin/users/{test_superuser.id}",
        headers=superuser_token_headers,
    )
    assert response.status_code == 200
    assert response.json() == {
        "id": test_superuser.id,
        "email": test_superuser.email,
        "is_active": False,
        "is_superuser": test_superuser.is_superuser,
    }

    user_in_db = test_db.query(User).first()
    assert not user_in_db.is_active


def test_delete_user_not_found(client, superuser_token_headers):
    response = client.delete(
        "/api/v1/admin/users/4321", headers=superuser_token_headers
    )
    assert response.status_code == 404


# TODO: re-enable when account updated email is available
# @patch("app.api.api_v1.routers.admin.send_email")
def test_edit_user(client, test_superuser, superuser_token_headers):
    new_user = {
        "email": "newemail@email.com",
        "is_active": False,
        "is_superuser": True,
        "names": "Joe Smith",
    }

    response = client.put(
        f"/api/v1/admin/users/{test_superuser.id}",
        json=new_user,
        headers=superuser_token_headers,
    )
    assert response.status_code == 200
    new_user["id"] = test_superuser.id
    assert response.json() == new_user
    assert response.headers.get("Cache-Control") == "no-cache, no-store, private"
    # mock_send_email.assert_called_with(EmailType.account_changed, test_superuser)


# TODO: re-enable when account updated email is available
# @patch("app.api.api_v1.routers.admin.send_email")
def test_edit_other_user(
    client,
    superuser_token_headers,
    test_user,
):
    old_is_active = test_user.is_active

    response = client.put(
        f"/api/v1/admin/users/{test_user.id}",
        json={
            "email": test_user.email,
            "is_active": not test_user.is_active,
        },
        headers=superuser_token_headers,
    )
    assert response.status_code == 200
    assert test_user.is_active is not old_is_active
    assert response.headers.get("Cache-Control") == "no-cache, no-store, private"
    # mock_send_email.assert_called_with(EmailType.account_changed, test_user)


def test_edit_user_not_found(client, superuser_token_headers):
    new_user = {
        "email": "newemail@email.com",
        "is_active": False,
        "is_superuser": False,
        "password": "new_password",
    }
    response = client.put(
        "/api/v1/admin/users/1234",
        json=new_user,
        headers=superuser_token_headers,
    )
    assert response.status_code == 404


def test_get_user(
    client,
    test_user,
    superuser_token_headers,
):
    response = client.get(
        f"/api/v1/admin/users/{test_user.id}", headers=superuser_token_headers
    )
    assert response.status_code == 200
    assert response.json() == {
        "id": test_user.id,
        "email": test_user.email,
        "is_active": bool(test_user.is_active),
        "is_superuser": test_user.is_superuser,
    }
    assert response.headers.get("Cache-Control") == "no-cache, no-store, private"


def test_user_not_found(client, superuser_token_headers):
    response = client.get("/api/v1/admin/users/123", headers=superuser_token_headers)
    assert response.status_code == 404


def test_unauthenticated_routes(client):
    response = client.get("/api/v1/admin/users")
    assert response.status_code == 401
    response = client.get("/api/v1/admin/users/123")
    assert response.status_code == 401
    response = client.put("/api/v1/admin/users/123")
    assert response.status_code == 401
    response = client.delete("/api/v1/admin/users/123")
    assert response.status_code == 401
    response = client.post("/api/v1/admin/bulk-imports/cclw/law-policy")
    assert response.status_code == 401


def test_unauthorized_routes(client):
    response = client.get(
        "/api/v1/admin/users",
    )
    assert response.status_code == 401
    response = client.get(
        "/api/v1/admin/users/123",
    )
    assert response.status_code == 401
    response = client.post(
        "/api/v1/admin/bulk-imports/cclw/law-policy",
    )
    assert response.status_code == 401


@patch("app.db.crud.password_reset.get_password_reset_token_expiry_ts")
@patch("app.api.api_v1.routers.admin.send_new_account_email")
def test_create_user(
    mock_send_email,
    mock_get_password_reset_token_expiry_ts,
    client,
    superuser_token_headers,
    test_db,
):
    mock_get_password_reset_token_expiry_ts.return_value = datetime.datetime(2099, 1, 1)
    new_user = {
        "email": "newemail@email.com",
        "is_active": False,
        "is_superuser": True,
        "names": "Joe Smith",
        "password": "new_password",
    }

    response = client.post(
        "/api/v1/admin/users",
        json=new_user,
        headers=superuser_token_headers,
    )
    assert response.status_code == 200
    assert response.json() == {
        "id": 2,
        "is_active": False,
        "is_superuser": True,
        "names": new_user["names"],
        "email": new_user["email"],
    }

    prt: PasswordResetToken = test_db.query(PasswordResetToken).first()
    assert prt.user_id == 2
    assert prt.expiry_ts == datetime.datetime(2099, 1, 1)
    assert not prt.is_redeemed
    mock_get_password_reset_token_expiry_ts.assert_called_once_with(
        minutes=ACCOUNT_ACTIVATION_EXPIRE_MINUTES
    )

    db_user = test_db.query(User).filter(User.id == 2).first()
    mock_send_email.assert_called_once_with(db_user, prt)


@patch("app.db.crud.password_reset.get_password_reset_token_expiry_ts")
@patch("app.api.api_v1.routers.admin.send_password_reset_email")
def test_reset_password(
    mock_send_email,
    mock_get_password_reset_token_expiry_ts,
    client,
    superuser_token_headers,
    test_db,
    test_user,
):
    mock_get_password_reset_token_expiry_ts.return_value = datetime.datetime(2099, 1, 1)

    response = client.post(
        f"/api/v1/admin/password-reset/{test_user.id}",
        headers=superuser_token_headers,
    )
    assert response.status_code == 200
    assert response.json()

    prt: PasswordResetToken = test_db.query(PasswordResetToken).first()
    assert prt.user_id == test_user.id
    assert prt.expiry_ts == datetime.datetime(2099, 1, 1)
    assert not prt.is_redeemed
    assert not prt.is_cancelled

    mock_send_email.assert_called_with(test_user, prt)

    # initiating a new request will cancel the existing token, create a new token, and send a new email
    response = client.post(
        f"/api/v1/admin/password-reset/{test_user.id}",
        headers=superuser_token_headers,
    )
    assert response.status_code == 200
    assert response.json()

    prt_old: PasswordResetToken = (
        test_db.query(PasswordResetToken)
        .filter(PasswordResetToken.id == prt.id)
        .first()
    )
    assert prt_old.is_cancelled

    prt_new: PasswordResetToken = (
        test_db.query(PasswordResetToken)
        .filter(PasswordResetToken.id == prt.id + 1)
        .first()
    )
    assert prt_new.user_id == test_user.id
    assert prt_new.expiry_ts == datetime.datetime(2099, 1, 1)
    assert not prt_new.is_redeemed
    assert not prt_new.is_cancelled

    assert mock_get_password_reset_token_expiry_ts.call_count == 2
    mock_send_email.assert_called_with(test_user, prt_new)
    assert mock_send_email.call_count == 2


ONE_DFC_ROW = """ID,Document ID,CCLW Description,Part of collection?,Create new family/ies?,Collection ID,Collection name,Collection summary,Document title,Family name,Family summary,Family ID,Document role,Applies to ID,Geography ISO,Documents,Category,Events,Sectors,Instruments,Frameworks,Responses,Natural Hazards,Document Type,Year,Language,Keywords,Geography,Parent Legislation,Comment,CPR Document ID,CPR Family ID,CPR Collection ID,CPR Family Slug,CPR Document Slug
1001,0,Test1,FALSE,FALSE,N/A,Collection1,CollectionSummary1,Title1,Fam1,Summary1,,MAIN,,GEO,http://somewhere|en,executive,02/02/2014|Law passed,Energy,,,Mitigation,,Order,,,Energy Supply,Algeria,,,CCLW.executive.1.2,CCLW.family.1001.0,CPR.Collection.1,FamSlug1,DocSlug1
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


def test_bulk_ingest_cclw_law_policy_preexisting_db_objects(
    client,
    superuser_token_headers,
    test_db,
    mocker,
):
    mock_start_import = mocker.patch("app.api.api_v1.routers.admin._start_ingest")
    mock_write_csv_to_s3 = mocker.patch("app.api.api_v1.routers.admin.write_csv_to_s3")

    populate_taxonomy(db=test_db)
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
            publication_ts=datetime.datetime(year=2014, month=1, day=1),
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


# TODO: The following tests are for the old import endpoint & should be removed
#       when no longer required
def test_bulk_import_cclw_law_policy_valid(
    client,
    superuser_token_headers,
    test_db,
    mocker,
):
    mock_start_import = mocker.patch("app.api.api_v1.routers.admin.start_import")
    mock_write_csv_to_s3 = mocker.patch("app.api.api_v1.routers.admin.write_csv_to_s3")

    test_db.add(Source(name="CCLW"))
    test_db.add(
        Geography(
            display_value="geography", slug="geography", value="GEO", type="country"
        )
    )
    test_db.add(DocumentType(name="doctype", description="doctype"))
    test_db.add(Language(language_code="LAN", name="language"))
    test_db.add(Category(name="Policy", description="policy"))
    test_db.add(Keyword(name="keyword1", description="keyword1"))
    test_db.add(Keyword(name="keyword2", description="keyword2"))
    test_db.add(Hazard(name="hazard1", description="hazard1"))
    test_db.add(Hazard(name="hazard2", description="hazard2"))
    test_db.add(Response(name="topic", description="topic"))
    test_db.add(Framework(name="framework", description="framework"))

    test_db.commit()

    test_db.add(Instrument(name="instrument", description="instrument", source_id=1))
    test_db.add(Sector(name="sector", description="sector", source_id=1))

    test_db.commit()

    csv_file = BytesIO(VALID_FILE_1.encode("utf8"))
    files = {"law_policy_csv": ("valid.csv", csv_file, "text/csv", {"Expires": "0"})}
    response = client.post(
        "/api/v1/admin/bulk-imports/cclw/law-policy",
        files=files,
        headers=superuser_token_headers,
    )
    assert response.status_code == 202
    response_json = response.json()
    assert response_json["detail"]["document_count"] == 2
    assert response_json["detail"]["document_skipped_count"] == 0
    assert response_json["detail"]["document_skipped_ids"] == []

    mock_start_import.assert_called_once()
    call = mock_start_import.mock_calls[0]
    assert len(call.args[3]) == 2

    mock_write_csv_to_s3.assert_called_once()
    call = mock_write_csv_to_s3.mock_calls[0]
    assert len(call.kwargs["file_contents"]) == csv_file.getbuffer().nbytes


@pytest.mark.parametrize(
    "invalid_file_content,expected_status",
    [
        (INVALID_FILE_1, 422),
        (INVALID_CSV_MIXED_ERRORS, 400),
    ],
)
def test_bulk_import_cclw_law_policy_invalid(
    invalid_file_content,
    expected_status,
    client,
    superuser_token_headers,
    test_db,
    mocker,
):
    mock_start_import = mocker.patch("app.api.api_v1.routers.admin.start_import")
    mock_write_csv_to_s3 = mocker.patch("app.api.api_v1.routers.admin.write_csv_to_s3")

    test_db.add(Source(name="CCLW"))
    test_db.add(
        Geography(
            display_value="geography", slug="geography", value="GEO", type="country"
        )
    )
    test_db.add(DocumentType(name="doctype", description="doctype"))
    test_db.add(Language(language_code="LAN", name="language"))
    test_db.add(Category(name="executive", description="executive"))
    test_db.add(Keyword(name="keyword1", description="keyword1"))
    test_db.add(Keyword(name="keyword2", description="keyword2"))
    test_db.add(Hazard(name="hazard1", description="hazard1"))
    test_db.add(Hazard(name="hazard2", description="hazard2"))
    test_db.add(Response(name="topic", description="topic"))
    test_db.add(Framework(name="framework", description="framework"))

    test_db.commit()

    test_db.add(Instrument(name="instrument", description="instrument", source_id=1))
    test_db.add(Sector(name="sector", description="sector", source_id=1))

    test_db.commit()

    csv_file = BytesIO(invalid_file_content.encode("utf8"))
    files = {"law_policy_csv": ("invalid.csv", csv_file, "text/csv", {"Expires": "0"})}
    response = client.post(
        "/api/v1/admin/bulk-imports/cclw/law-policy",
        files=files,
        headers=superuser_token_headers,
    )
    assert response.status_code == expected_status
    assert "detail" in response.json()
    assert response.json()["detail"]
    mock_start_import.assert_not_called()
    mock_write_csv_to_s3.assert_not_called()


def test_bulk_import_cclw_law_policy_db_objects(
    client,
    superuser_token_headers,
    test_db,
    mocker,
):
    mock_start_import = mocker.patch("app.api.api_v1.routers.admin.start_import")
    mock_write_csv_to_s3 = mocker.patch("app.api.api_v1.routers.admin.write_csv_to_s3")

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

    test_db.add(Instrument(name="instrument", description="instrument", source_id=1))
    test_db.add(Sector(name="sector", description="sector", source_id=1))

    test_db.commit()

    csv_file = BytesIO(VALID_FILE_1.encode("utf8"))
    files = {"law_policy_csv": ("valid.csv", csv_file, "text/csv", {"Expires": "0"})}
    response = client.post(
        "/api/v1/admin/bulk-imports/cclw/law-policy",
        files=files,
        headers=superuser_token_headers,
    )
    assert response.status_code == 202
    response_json = response.json()
    assert response_json["detail"]["document_count"] == 2
    assert response_json["detail"]["document_skipped_count"] == 0
    assert response_json["detail"]["document_skipped_ids"] == []

    mock_start_import.assert_called_once()
    call = mock_start_import.mock_calls[0]
    assert len(call.args[3]) == 2

    mock_write_csv_to_s3.assert_called_once()
    call = mock_write_csv_to_s3.mock_calls[0]
    assert len(call.kwargs["file_contents"]) == csv_file.getbuffer().nbytes

    # TODO: This test needs to check the db objects


def test_bulk_import_cclw_law_policy_preexisting_db_objects(
    client,
    superuser_token_headers,
    test_db,
    mocker,
):
    mock_start_import = mocker.patch("app.api.api_v1.routers.admin.start_import")
    mock_write_csv_to_s3 = mocker.patch("app.api.api_v1.routers.admin.write_csv_to_s3")

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
            publication_ts=datetime.datetime(year=2014, month=1, day=1),
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

    csv_file = BytesIO(VALID_FILE_1.encode("utf8"))
    files = {"law_policy_csv": ("valid.csv", csv_file, "text/csv", {"Expires": "0"})}
    response = client.post(
        "/api/v1/admin/bulk-imports/cclw/law-policy",
        files=files,
        headers=superuser_token_headers,
    )
    assert response.status_code == 202
    response_json = response.json()
    assert response_json["detail"]["document_count"] == 2
    assert response_json["detail"]["document_skipped_count"] == 1
    assert response_json["detail"]["document_skipped_ids"] == [existing_doc_import_id]

    mock_start_import.assert_called_once()
    call = mock_start_import.mock_calls[0]
    assert len(call.args[3]) == 1

    mock_write_csv_to_s3.assert_called_once()
    call = mock_write_csv_to_s3.mock_calls[0]
    assert len(call.kwargs["file_contents"]) == csv_file.getbuffer().nbytes
