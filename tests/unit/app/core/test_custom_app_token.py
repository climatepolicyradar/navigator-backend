import os
from datetime import datetime

import jwt
import pytest
from dateutil.relativedelta import relativedelta

from app.core.custom_app import encode_configuration_token

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


@pytest.mark.parametrize(
    "input_corpora,expected_allowed_corpora",
    [
        (["apple", "carrot", "banana"], ["apple", "banana", "carrot"]),
        (["cucumber"], ["cucumber"]),
    ],
)
def test_create_configuration_token_default_expiry(
    input_corpora: list[str], expected_allowed_corpora: list[str]
):
    datetime.utcnow()
    token = encode_configuration_token(input_corpora)
    assert token is not None
    assert isinstance(token, str)

    data = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])

    assert has_expected_keys(data)

    assert data["allowed_corpora_ids"] == expected_allowed_corpora
    assert timedelta_years(
        EXPIRE_AFTER_DEFAULT_YEARS, datetime.fromtimestamp(data["exp"])
    ) == datetime.fromtimestamp(data["iat"])


@pytest.mark.parametrize(
    "input_corpora,expected_allowed_corpora,expiry_years",
    [
        (
            ["raspberry", "strawberry", "orange"],
            ["orange", "raspberry", "strawberry"],
            1,
        ),
        (["grapefruit"], ["grapefruit"], 5),
    ],
)
def test_create_configuration_token_specific_expiry(
    input_corpora: list[str], expected_allowed_corpora: list[str], expiry_years: int
):
    token = encode_configuration_token(input_corpora, expiry_years)
    assert token is not None
    assert isinstance(token, str)

    data = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    assert has_expected_keys(data)

    assert data["allowed_corpora_ids"] == expected_allowed_corpora
    assert timedelta_years(
        expiry_years, datetime.fromtimestamp(data["exp"])
    ) == datetime.fromtimestamp(data["iat"])
