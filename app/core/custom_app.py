import logging
import os
from datetime import datetime
from typing import Optional

import jwt
from dateutil.relativedelta import relativedelta

from app.api.api_v1.schemas.custom_app import CustomAppConfigDTO
from app.core import security

_LOGGER = logging.getLogger(__name__)

TOKEN_SECRET_KEY = os.environ["TOKEN_SECRET_KEY"]
ISSUER = "Climate Policy Radar"

# TODO: revisit/configure access token expiry
CUSTOM_APP_TOKEN_EXPIRE_YEARS = 10  # token valid for 10 years
EXPECTED_ARGS_LENGTH = 3


def _contains_special_chars(input: str) -> bool:
    """Check if string contains any non alpha numeric characters.

    :param str input: A string to check.
    :return bool: True if string contains special chars, False otherwise.
    """
    if any(not char.isalnum() for char in input):
        return True
    return False


def _parse_and_sort_corpora_ids(corpora_ids_str: str) -> list[str]:
    """Parse and sort the comma separated string of corpora IDs.

    :param str corpora_ids_str: A comma separated string containing the
        corpus import IDs that the custom app should show.
    :return list[str]: A list of corpora IDs sorted alphanumerically.
    """
    corpora_ids = corpora_ids_str.split(",")
    corpora_ids.sort()
    return corpora_ids


def create_configuration_token(input: str, years: Optional[int] = None) -> str:
    """Create a custom app configuration token.

    :param str input: A semi-colon delimited string containing in this
        order:
        1. A comma separated string containing the corpus import IDs
            that the custom app should show.
        2. A string containing the name of the theme.
        3. A string containing the hostname of the custom app.
    :return str: A JWT token containing the encoded allowed corpora.
    """
    expiry_years = years or CUSTOM_APP_TOKEN_EXPIRE_YEARS
    issued_at = datetime.utcnow()
    expire = issued_at + relativedelta(years=expiry_years)

    parts = input.split(";")
    if len(parts) != EXPECTED_ARGS_LENGTH or any(len(part) < 1 for part in parts):
        _LOGGER.error("Expected exactly 3 arguments")
        raise ValueError

    corpora_ids, subject, audience = parts

    config = CustomAppConfigDTO(
        allowed_corpora_ids=_parse_and_sort_corpora_ids(corpora_ids),
        subject=subject,
        issuer=ISSUER,
        audience=audience,
        expiry=expire,
        issued_at=int(
            datetime.timestamp(issued_at.replace(microsecond=0))
        ),  # No microseconds
    )

    if _contains_special_chars(config.subject):
        _LOGGER.error(
            "Subject must not contain any special characters, including spaces"
        )
        raise ValueError

    msg = "Creating custom app configuration token that expires on "
    msg += f"{expire.strftime('%a %d %B %Y at %H:%M:%S:%f')} "
    msg += f"for the following corpora: {corpora_ids}"
    print(msg)

    to_encode = {
        "allowed_corpora_ids": config.allowed_corpora_ids,
        "exp": config.expiry,
        "iat": config.issued_at,
        "iss": config.issuer,
        "sub": config.subject,
        "aud": str(config.audience),
    }
    return jwt.encode(to_encode, TOKEN_SECRET_KEY, algorithm=security.ALGORITHM)


def decode_config_token(token: str, audience: Optional[str]) -> list[str]:
    """Decodes a configuration token.

    :param str token : A JWT token that has been encoded with a list of
        allowed corpora ids that the custom app should show, an expiry
        date and an issued at date.
    :return list[str]: A decoded list of valid corpora ids.
    """
    decoded_token = jwt.decode(
        token,
        TOKEN_SECRET_KEY,
        algorithms=[security.ALGORITHM],
        issuer=ISSUER,
        audience=audience,
    )
    corpora_ids: list = decoded_token.get("allowed_corpora_ids")

    return corpora_ids
