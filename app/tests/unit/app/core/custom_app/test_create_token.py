from datetime import datetime

import jwt
import pytest
from dateutil.relativedelta import relativedelta
from tests.unit.app.core.custom_app.conftest import ALGORITHM, TOKEN_SECRET_KEY

from app.service.custom_app import AppTokenFactory

EXPIRE_AFTER_1_YEAR = 1
EXPIRE_AFTER_5_YEARS = 5
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
            "aud",
            "iss",
            "sub",
        }
        == set()
    )


@pytest.mark.parametrize(
    "input_str",
    [
        ("cucumber"),
        ("cucumber;potato"),
        ("cucumber;potato;"),
        ("cucumber;potato;leek;onion"),
    ],
)
def test_create_configuration_token_incorrect_num_args_in_input(input_str: str):
    af = AppTokenFactory()
    with pytest.raises(ValueError):
        token = af.create_configuration_token(input_str)
        assert token is None

        data = jwt.decode(token, TOKEN_SECRET_KEY, algorithms=[ALGORITHM])
        assert has_expected_keys(data)


@pytest.mark.parametrize(
    "input_str",
    [
        ("cabbage;@a9*7g$;"),
        ("tomato;some subject;"),
    ],
)
def test_create_configuration_token_subject_contains_special_chars(input_str: str):
    af = AppTokenFactory()
    with pytest.raises(ValueError):
        token = af.create_configuration_token(input_str)
        assert token is None

        data = jwt.decode(token, TOKEN_SECRET_KEY, algorithms=[ALGORITHM])
        assert has_expected_keys(data)


@pytest.mark.parametrize(
    "input_str,expected_allowed_corpora,expected_subject,expected_audience",
    [
        (
            "apple,banana,carrot;ORG1;ORG1.org",
            ["apple", "banana", "carrot"],
            "ORG1",
            "ORG1.org",
        ),
        ("cucumber;ORG2;ORG2.com", ["cucumber"], "ORG2", "ORG2.com"),
    ],
)
def test_create_configuration_token_default_expiry(
    input_str: str,
    expected_allowed_corpora: list[str],
    expected_subject: str,
    expected_audience: str,
):
    af = AppTokenFactory()
    token = af.create_configuration_token(input_str)
    assert token is not None
    assert isinstance(token, str)

    data = jwt.decode(
        token,
        TOKEN_SECRET_KEY,
        algorithms=[ALGORITHM],
        audience=expected_audience,
    )

    assert has_expected_keys(data)

    assert data["allowed_corpora_ids"] == expected_allowed_corpora
    assert data["iss"] == "Climate Policy Radar"
    assert data["sub"] == expected_subject
    assert data["aud"] == expected_audience
    assert timedelta_years(
        EXPIRE_AFTER_DEFAULT_YEARS, datetime.fromtimestamp(data["exp"])
    ) == datetime.fromtimestamp(data["iat"])

    assert not data["aud"].endswith("/")
    assert not data["aud"].startswith("http")


@pytest.mark.parametrize(
    "input_str,expected_allowed_corpora,expiry_years,expected_subject,expected_audience",
    [
        (
            "raspberry,strawberry,orange;ORG1;ORG1.org",
            ["orange", "raspberry", "strawberry"],
            EXPIRE_AFTER_1_YEAR,
            "ORG1",
            "ORG1.org",
        ),
        (
            "grapefruit;ORG2;ORG2.com",
            ["grapefruit"],
            EXPIRE_AFTER_5_YEARS,
            "ORG2",
            "ORG2.com",
        ),
    ],
)
def test_create_configuration_token_specific_expiry(
    input_str: str,
    expected_allowed_corpora: list[str],
    expiry_years: int,
    expected_subject: str,
    expected_audience: str,
):
    af = AppTokenFactory()
    token = af.create_configuration_token(input_str, expiry_years)
    assert token is not None
    assert isinstance(token, str)

    data = jwt.decode(
        token,
        TOKEN_SECRET_KEY,
        algorithms=[ALGORITHM],
        audience=expected_audience,
    )
    assert has_expected_keys(data)

    assert data["allowed_corpora_ids"] == expected_allowed_corpora
    assert data["iss"] == "Climate Policy Radar"
    assert data["sub"] == expected_subject
    assert data["aud"] == expected_audience
    assert timedelta_years(
        expiry_years, datetime.fromtimestamp(data["exp"])
    ) == datetime.fromtimestamp(data["iat"])

    assert not data["aud"].endswith("/")
    assert not data["aud"].startswith("http")
