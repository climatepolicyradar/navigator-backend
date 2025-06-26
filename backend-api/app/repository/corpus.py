from db_client.models.dfce.family import Corpus, Family, FamilyCorpus
from sqlalchemy import func
from sqlalchemy.orm import Session


def get_total_families_per_corpus(db: Session, corpus_import_id: str) -> int:
    """
    Get the total number of families per corpus.

    :param db: Database session
    :param corpus_import_id: The import ID of the corpus
    :return: The total number of families per corpus
    """
    return (
        db.query(Family)
        .join(FamilyCorpus, FamilyCorpus.family_import_id == Family.import_id)
        .filter(FamilyCorpus.corpus_import_id == corpus_import_id)
        .count()
    )


def get_family_count_by_category_per_corpus(db: Session, corpus_import_id: str):
    """
    Get the count of families by category per corpus.

    :param db: Database session
    :param corpus_import_id: The import ID of the corpus
    :return: A list of tuples where each tuple contains a family category and its count
    """
    return (
        db.query(Family.family_category, func.count())
        .join(FamilyCorpus, FamilyCorpus.family_import_id == Family.import_id)
        .filter(FamilyCorpus.corpus_import_id == corpus_import_id)
        .group_by(Family.family_category)
        .all()
    )


def get_allowed_corpora(db: Session, allowed_corpora: list[str]) -> list[Corpus]:
    """
    Get the allowed corpora.

    :param db: Database session
    :param allowed_corpora: A list of allowed corpora
    :return: A list of Corpus objects that are allowed
    """
    query = db.query(Corpus)
    if allowed_corpora != []:
        query = query.filter(Corpus.import_id.in_(allowed_corpora))

    return query.all()
