import logging
from typing import cast

from db_client.models.dfce.family import Corpus
from sqlalchemy import distinct, select
from sqlalchemy.orm import Session

_LOGGER = logging.getLogger(__name__)


def verify_any_corpora_ids_in_db(db: Session, corpora_ids: list[str]) -> bool:
    """Validate given corpus IDs against the existing corpora in DB.

    :param Session db: The DB session to connect to.
    :param list[str] corpora_ids: The corpus import IDs we want to
        validate against the DB values.
    :return bool: Return whether or not all the corpora are valid.
    """
    corpora_ids_from_db = cast(
        list, db.scalars(select(distinct(Corpus.import_id))).all()
    )

    validate_success = any(corpus in corpora_ids_from_db for corpus in corpora_ids)
    if validate_success:
        not_in_db = set(corpora_ids).difference(corpora_ids_from_db)
        if not_in_db != set():
            _LOGGER.warning(
                f"Some corpora in app token {not_in_db} "
                "not available for searching against."
            )

    return validate_success


def validate_corpora_ids(corpora_ids: set[str], valid_corpora_ids: set[str]) -> bool:
    """Validate all given corpus IDs against a list of allowed corpora.

    :param set[str] corpora_ids: The corpus import IDs we want to
        validate.
    :param set[str] valid_corpora_ids: The corpus import IDs
        we want to validate against.
    :return bool: Return whether or not all the corpora are valid.
    """
    validate_success = corpora_ids.issubset(valid_corpora_ids)
    if not validate_success:
        invalid_corpora = set(corpora_ids).difference(valid_corpora_ids)
        if invalid_corpora != set():
            _LOGGER.warning(
                f"Some corpora in search request params {invalid_corpora}"
                "forbidden to search against."
            )
    return validate_success
