from db_client.models.dfce.family import Corpus, Family, FamilyCorpus
from sqlalchemy import func
from sqlalchemy.orm import Session


def get_total_families_per_corpus(db: Session, corpus_import_id: str) -> int:
    return (
        db.query(Family)
        .join(FamilyCorpus, FamilyCorpus.family_import_id == Family.import_id)
        .filter(FamilyCorpus.corpus_import_id == corpus_import_id)
        .count()
    )


def get_family_count_by_category_per_corpus(db: Session, corpus_import_id: str):
    return (
        db.query(Family.family_category, func.count())
        .join(FamilyCorpus, FamilyCorpus.family_import_id == Family.import_id)
        .filter(FamilyCorpus.corpus_import_id == corpus_import_id)
        .group_by(Family.family_category)
        .all()
    )


def get_allowed_corpora(db: Session, allowed_corpora: list[str]) -> list[Corpus]:
    query = db.query(Corpus)
    if allowed_corpora != []:
        query = query.filter(Corpus.import_id.in_(allowed_corpora))

    return query.all()
