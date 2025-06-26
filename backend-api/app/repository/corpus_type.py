from db_client.models.organisation import CorpusType
from sqlalchemy.orm import Session


def get(db: Session, corpus_type_name: str) -> CorpusType:
    """
    Get a CorpusType object based on its name.

    :param db: Database session
    :param corpus_type_name: The name of the corpus type
    :return: A CorpusType object
    """
    return db.query(CorpusType).filter(CorpusType.name == corpus_type_name).one()
