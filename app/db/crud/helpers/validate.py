import logging
from typing import Optional, cast

from db_client.models.dfce.family import Corpus
from sqlalchemy import distinct, select
from sqlalchemy.orm import Session

_LOGGER = logging.getLogger(__name__)


def validate_corpora_ids(
    db: Session, corpora_ids: list[str], allowed_corpora_ids: Optional[list[str]] = None
) -> bool:
    """Validate all given corpus IDs against a list of allowed corpora.

    :param Session db: The DB session to connect to.
    :param list[str] corpora_ids: The corpus import IDs we want to
        validate.
    :param Optional[list[str]] allowed_corpora_ids: The corpus import
        IDs we want to validate against.
    :return bool: Return whether or not all the corpora are valid.
    """
    if allowed_corpora_ids is None:
        allowed_corpora_ids = cast(
            list, db.scalars(select(distinct(Corpus.import_id))).all()
        )
        _LOGGER.info(allowed_corpora_ids)  # TODO remove in part 2.

    validate_success = all(corpus in allowed_corpora_ids for corpus in corpora_ids)
    return validate_success
