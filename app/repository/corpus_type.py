from db_client.models.organisation import CorpusType
from sqlalchemy.orm import Session


def get(db: Session, corpus_type_name: str) -> CorpusType:
    return db.query(CorpusType).filter(CorpusType.name == corpus_type_name).one()
