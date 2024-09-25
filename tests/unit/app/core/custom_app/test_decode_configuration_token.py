from typing import Optional

import jwt
import pytest

from app.core.custom_app import create_configuration_token, decode_config_token
from tests.unit.app.core.custom_app.conftest import VALID_AUDIENCE


def test_decoding_expired_token_raise_expired_signature_token_error(expired_token):
    with pytest.raises(jwt.ExpiredSignatureError) as error:
        decode_config_token(expired_token, VALID_AUDIENCE)

    assert str(error.value) == "Signature has expired"


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
    input_str: str, aud: Optional[str], error_msg: str, monkeypatch
):
    monkeypatch.setattr("app.core.custom_app.DEVELOPMENT_MODE", False)
    token = create_configuration_token(input_str)
    with pytest.raises(jwt.InvalidTokenError) as error:
        decode_config_token(token, aud)

    assert str(error.value) == error_msg


def test_decode_configuration_token_success(valid_token):
    decoded_corpora_ids = decode_config_token(valid_token, VALID_AUDIENCE)
    assert len(decoded_corpora_ids) > 0

    expected_num_corpora = 2
    assert len(decoded_corpora_ids) == expected_num_corpora
