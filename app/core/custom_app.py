import logging
import os
from datetime import datetime
from typing import Optional

import jwt
from dateutil.relativedelta import relativedelta
from db_client.models.dfce.family import Corpus
from sqlalchemy.orm import Session

_LOGGER = logging.getLogger(__name__)

SECRET_KEY = os.environ["SECRET_KEY"]
ALGORITHM = "HS256"

# TODO: revisit/configure access token expiry
CUSTOM_APP_TOKEN_EXPIRE_YEARS = 10  # token valid for 10 years


def validate(db: Session, allowed_corpora_ids: list[str]) -> bool:
    """Validate whether all given corpus IDs exist in the DB.

    :param Session db: The DB session to connect to.
    :param list[str] allowed_corpora_ids: The corpus import IDs we want
        to validate.
    :return bool: Return whether or not all the corpora exist in the DB.
    """
    existing_corpora_in_db = db.query(Corpus.import_id).distinct().all()
    validate_success = all(
        corpus in existing_corpora_in_db for corpus in allowed_corpora_ids
    )
    if not validate_success:
        _LOGGER.error("One or more of the given corpora do not exist in the database.")
    return validate_success


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
