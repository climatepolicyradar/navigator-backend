import logging
from typing import Optional, cast
from unittest.mock import patch

import jwt
import pytest
from fastapi import HTTPException, status

from app.core.custom_app import AppTokenFactory
from tests.unit.app.core.custom_app.conftest import VALID_AUDIENCE


def test_decoding_expired_token_raise_expired_signature_token_error(
    expired_token, caplog
):
    af = AppTokenFactory()
    with patch("jwt.decode", side_effect=jwt.ExpiredSignatureError), caplog.at_level(
        logging.DEBUG
    ), pytest.raises(HTTPException):
        response = cast(HTTPException, af.decode(expired_token, VALID_AUDIENCE))
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        data = response.json()  # pyright: ignore
        assert str(data["detail"]) == "Could not decode configuration token"
    # FIXME assert "Signature has expired" in caplog.text


@pytest.mark.skip("Re-implement this as part of PDCT-1509")
@pytest.mark.parametrize(
    "input_str, aud, error_msg",
    [
        ("mango,apple;subject;https://audience.com", None, "Invalid audience"),
        (
            "mango,apple;subject;https://audience.com",
            "https://audience.org",
            "Audience doesn't match",
        ),
        (
            "mango,apple;subject;https://AUDIENCE.OrG",
            "https://AUDIENCE.Com",
            "Audience doesn't match",
        ),
    ],
)
def test_decoding_token_with_invalid_aud_raises_invalid_token_error(
    input_str: str, aud: Optional[str], error_msg: str, caplog
):
    af = AppTokenFactory()
    token = af.create_configuration_token(input_str)
    with patch("jwt.decode", side_effect=jwt.InvalidTokenError), pytest.raises(
        HTTPException
    ), caplog.at_level(logging.ERROR) as error:
        af.decode(token, aud)

    assert str(error.value) == error_msg


@pytest.mark.parametrize(
    "input_str, aud",
    [
        ("mango,apple;subject;https://audience.com", None),
        ("mango,apple;subject;https://audience.com", "https://audience.org"),
        ("mango,apple;subject;https://AUDIENCE.OrG", "https://AUDIENCE.Com"),
    ],
)
def test_decoding_token_with_invalid_aud_success_in_dev_mode(
    input_str: str, aud: Optional[str]
):
    af = AppTokenFactory()
    token = af.create_configuration_token(input_str)
    token_content = af.decode(token, aud)
    assert len(token_content) > 0

    expected_num_keys = 6
    assert len(token_content) == expected_num_keys


def test_decode_configuration_token_success(valid_token):
    af = AppTokenFactory()
    token_content = af.decode(valid_token, VALID_AUDIENCE)
    assert len(token_content) > 0

    expected_num_keys = 6
    assert len(token_content) == expected_num_keys
