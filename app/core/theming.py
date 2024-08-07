import os

from db_client.models.dfce.family import Corpus
from sqlalchemy.orm import Session

from app.db.session import get_db

SECRET_KEY = os.environ["SECRET_KEY"]
ALGORITHM = "HS256"


def validate(db: Session, allowed_corpora_ids: list[str]) -> bool:
    """Validate whether a corpus with the given ID exists in the DB.

    :param Session db: The DB session to connect to.
    :param str corpus_id: The corpus import ID we want to validate.
    :return bool: Return whether or not the corpus exists in the DB.
    """
    existing_corpora_in_db = db.query(Corpus.import_id).distinct().all()
    return all(corpus in existing_corpora_in_db for corpus in allowed_corpora_ids)


def create_configuration_token(allowed_corpora: str):
    db = next(get_db())
    corpora_ids = allowed_corpora.split(",")
    print(validate(db, corpora_ids))
