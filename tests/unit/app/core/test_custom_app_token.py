import os
from datetime import datetime, timedelta

import jwt
import pytest
from dateutil.relativedelta import relativedelta

from app.core.custom_app import create_configuration_token, decode_configuration_token

SECRET_KEY = os.environ["SECRET_KEY"]
ALGORITHM = "HS256"

EXPIRE_AFTER_DEFAULT_YEARS = 10


def timedelta_years(years, from_date=None):
    if from_date is None:
        from_date = datetime.now()
    return from_date - relativedelta(years=years)


def has_expected_keys(keys: list[str]) -> bool:
    return bool(
        set(keys)
        ^ {
            "allowed_corpora_ids",
            "exp",
            "iat",
        }
        == set()
    )


def create_expired_token() -> str:
    expire = datetime.now() - timedelta(days=2)
    to_encode = {
        "allowed_corpora_ids": "mango, pineapple",
        "exp": expire,
        "iat": datetime.timestamp(expire),
    }
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


@pytest.mark.parametrize(
    "input_str,expected_allowed_corpora",
    [
        ("apple,banana,carrot", ["apple", "banana", "carrot"]),
        ("cucumber", ["cucumber"]),
    ],
)
def test_create_configuration_token_default_expiry(
    input_str: str, expected_allowed_corpora: list[str]
):
    datetime.utcnow()
    token = create_configuration_token(input_str)
    assert token is not None
    assert isinstance(token, str)

    data = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])

    assert has_expected_keys(data)

    assert data["allowed_corpora_ids"] == expected_allowed_corpora
    assert timedelta_years(
        EXPIRE_AFTER_DEFAULT_YEARS, datetime.fromtimestamp(data["exp"])
    ) == datetime.fromtimestamp(data["iat"])


@pytest.mark.parametrize(
    "input_str,expected_allowed_corpora,expiry_years",
    [
        (
            "raspberry,strawberry,orange",
            ["orange", "raspberry", "strawberry"],
            1,
        ),
        ("grapefruit", ["grapefruit"], 5),
    ],
)
def test_create_configuration_token_specific_expiry(
    input_str: str, expected_allowed_corpora: list[str], expiry_years: int
):
    token = create_configuration_token(input_str, expiry_years)
    assert token is not None
    assert isinstance(token, str)

    data = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    assert has_expected_keys(data)

    assert data["allowed_corpora_ids"] == expected_allowed_corpora
    assert timedelta_years(
        expiry_years, datetime.fromtimestamp(data["exp"])
    ) == datetime.fromtimestamp(data["iat"])


@pytest.mark.parametrize("expired_token", [create_expired_token()])
def test_decoding_expired_token_raise_invalid_token_error(expired_token: str):
    with pytest.raises(jwt.ExpiredSignatureError) as error:
        decode_configuration_token(expired_token)

    assert str(error.value) == "Signature has expired"


def return_true_validate_corpora_mock(*args) -> bool:
    return True


def return_false_validate_corpora_mock(*args) -> bool:
    return False


@pytest.mark.parametrize(
    "token, expected_allowed_corpora",
    [
        (
            create_configuration_token("mango,apple"),
            ["apple", "mango"],
        )
    ],
)
def test_decodes_configuration_token_returns_list_of_corpora_ids(
    token: str, expected_allowed_corpora: list[str], monkeypatch
):
    monkeypatch.setattr(
        "app.core.custom_app.validate_corpora_ids", return_true_validate_corpora_mock
    )

    decoded_corpora_ids = decode_configuration_token(token)
    assert decoded_corpora_ids == expected_allowed_corpora


@pytest.mark.parametrize(
    "token",
    [create_configuration_token("mango,apple")],
)
def test_returns_invalid_token_error_for_non_existent_corpora_ids(
    token: str, monkeypatch
):
    monkeypatch.setattr(
        "app.core.custom_app.validate_corpora_ids", return_false_validate_corpora_mock
    )

    with pytest.raises(jwt.InvalidTokenError) as error:
        decode_configuration_token(token)

    assert (
        str(error.value)
        == "One or more of the given corpora does not exist in the database"
    )
