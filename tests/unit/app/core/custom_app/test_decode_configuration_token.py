from typing import Optional

import jwt
import pytest

from app.core.custom_app import create_configuration_token, decode_configuration_token
from tests.unit.app.core.custom_app.conftest import VALID_AUDIENCE


def test_decoding_expired_token_raise_expired_signature_token_error(expired_token):
    with pytest.raises(jwt.ExpiredSignatureError) as error:
        decode_configuration_token(expired_token, VALID_AUDIENCE)

    assert str(error.value) == "Signature has expired"


@pytest.mark.parametrize(
    "input_str, aud, error_msg",
    [
        ("mango,apple;subject;https://audience.com", None, "Invalid audience"),
        (
            "mango,apple;subject;https://audience.com",
            "https://audience.com",
            "Audience doesn't match",
        ),
        (
            "mango,apple;subject;https://AUDIENCE.OrG",
            "https://AUDIENCE.OrG",
            "Audience doesn't match",
        ),
    ],
)
def test_decoding_token_with_invalid_aud_raises_expired_signature_token_error(
    input_str: str, aud: Optional[str], error_msg: str
):
    token = create_configuration_token(input_str)
    with pytest.raises(jwt.InvalidTokenError) as error:
        decode_configuration_token(token, aud)

    assert str(error.value) == error_msg


def test_returns_invalid_token_error_for_non_existent_corpora_ids(
    valid_token, mock_false_validate_corpora_mock
):
    with pytest.raises(jwt.InvalidTokenError) as error:
        decode_configuration_token(valid_token, VALID_AUDIENCE)

    assert (
        str(error.value)
        == "One or more of the given corpora does not exist in the database"
    )


@pytest.mark.parametrize(
    "expected_allowed_corpora",
    [["apple", "mango"]],
)
def test_decode_configuration_token_success(
    valid_token,
    expected_allowed_corpora: list[str],
    mock_true_validate_corpora_mock,
):
    decoded_corpora_ids = decode_configuration_token(valid_token, VALID_AUDIENCE)
    assert decoded_corpora_ids == expected_allowed_corpora