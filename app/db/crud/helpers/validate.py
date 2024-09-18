import logging

from db_client.models.dfce.family import Corpus
from sqlalchemy import distinct, select
from sqlalchemy.orm import Session

_LOGGER = logging.getLogger(__name__)


def validate_corpora_ids(db: Session, allowed_corpora_ids: list[str]) -> bool:
    """Validate whether all given corpus IDs exist in the DB.

    :param Session db: The DB session to connect to.
    :param list[str] allowed_corpora_ids: The corpus import IDs we want
        to validate.
    :return bool: Return whether or not all the corpora exist in the DB.
    """
    existing_corpora_in_db = db.scalars(select(distinct(Corpus.import_id))).all()
    validate_success = all(
        corpus in existing_corpora_in_db for corpus in allowed_corpora_ids
    )
    return validate_success
