import logging
import os

import jwt
from db_client.models.dfce.family import Corpus
from sqlalchemy.orm import Session

_LOGGER = logging.getLogger(__name__)

SECRET_KEY = os.environ["SECRET_KEY"]
ALGORITHM = "HS256"


def validate(db: Session, allowed_corpora_ids: list[str]) -> bool:
    """Validate whether all given corpus IDs exist in the DB.

    :param Session db: The DB session to connect to.
    :param list[str] allowed_corpora_ids: The corpus import IDs we want
        to validate.
    :return bool: Return whether or not all the corpora exist in the DB.
    """
    existing_corpora_in_db = db.query(Corpus.import_id).distinct().all()
    return all(corpus in existing_corpora_in_db for corpus in allowed_corpora_ids)


def create_configuration_token(allowed_corpora: str) -> str:
    """Create a custom app configuration token.

    :param str allowed_corpora: A comma separated string containing the
        corpus import IDs that the custom app should show.
    :return str: A JWT token containing the encoded allowed corpora.
    """
    corpora_ids = allowed_corpora.split(",")
    msg = "Creating custom app configuration token for the following corpora:"
    msg += f" {corpora_ids}"
    _LOGGER.info(msg)
    to_encode = {"allowed_corpora_ids": corpora_ids}
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
