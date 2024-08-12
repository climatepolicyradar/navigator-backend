import logging
import os
from datetime import datetime
from typing import Optional

import jwt
from dateutil.relativedelta import relativedelta

from app.db.crud.helpers.validate import validate_corpora_ids
from app.db.session import get_db

_LOGGER = logging.getLogger(__name__)

SECRET_KEY = os.environ["SECRET_KEY"]
ALGORITHM = "HS256"

# TODO: revisit/configure access token expiry
CUSTOM_APP_TOKEN_EXPIRE_YEARS = 10  # token valid for 10 years


def create_configuration_token(
    allowed_corpora: str, years: Optional[int] = None
) -> str:
    """Create a custom app configuration token.

    :param str allowed_corpora: A comma separated string containing the
        corpus import IDs that the custom app should show.
    :return str: A JWT token containing the encoded allowed corpora.
    """
    expiry_years = years or CUSTOM_APP_TOKEN_EXPIRE_YEARS
    issued_at = datetime.utcnow()
    expire = issued_at + relativedelta(years=expiry_years)

    corpora_ids = allowed_corpora.split(",")
    corpora_ids.sort()

    msg = "Creating custom app configuration token that expires on "
    msg += f"{expire.strftime('%a %d %B %Y at %H:%M:%S:%f')} "
    msg += f"for the following corpora: {corpora_ids}"
    print(msg)

    to_encode = {
        "allowed_corpora_ids": corpora_ids,
        "exp": expire,
        "iat": datetime.timestamp(issued_at.replace(microsecond=0)),  # No microseconds
    }
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def decode_configuration_token(token: str) -> list[str]:
    """Decodes a configuration token.

    :param str token : A JWT token that has been encoded with a list of allowed corpora ids that the custom app should show,
    an expiry date and an issued at date.
    :return list[str]: A decoded list of valid corpora ids.
    """

    db = next(get_db())

    try:
        decoded_token = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        corpora_ids: list = decoded_token.get("allowed_corpora_ids")

        if not validate_corpora_ids(db, corpora_ids):
            raise jwt.InvalidTokenError(
                "One or more of the given corpora does not exist in the database"
            )
    except jwt.InvalidTokenError as error:
        raise error
    return corpora_ids
