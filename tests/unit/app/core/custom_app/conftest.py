import os
from datetime import datetime, timedelta

import jwt
import pytest

from app.core.custom_app import create_configuration_token

SECRET_KEY = os.environ["SECRET_KEY"]
ALGORITHM = "HS256"
VALID_AUDIENCE = "https://audience.com/"


@pytest.fixture
def expired_token() -> str:
    expire = datetime.now() - timedelta(days=2)
    to_encode = {
        "allowed_corpora_ids": "mango, pineapple",
        "exp": expire,
        "iat": datetime.timestamp(expire),
        "iss": "Climate Policy Radar",
        "aud": VALID_AUDIENCE,
        "sub": "Some subject",
    }
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


@pytest.fixture(
    scope="module",
    params=[
        "mango,apple;subject;audience",
        "mango,apple;subject;https://audience.com",
        "mango,apple;subject;https://audience.com/",
        "mango,apple;subject;https://www.audience.com",
        "mango,apple;subject;https://www.audience.com/",
        "mango,apple;subject;http://source",
    ],
)
def token_with_invalid_aud(request) -> str:
    return request.param


@pytest.fixture
def valid_token() -> str:
    return create_configuration_token(f"mango,apple;subject;{VALID_AUDIENCE}")


@pytest.fixture(scope="module", params=[True, False])
def return_validate_corpora_mock(request, monkeypatch):
    validate_corpora_mock_return_value = request.param
    monkeypatch.setattr(
        "app.core.custom_app.validate_corpora_ids", validate_corpora_mock_return_value
    )
    yield validate_corpora_mock_return_value
