from typing import Optional

from db_client.models.dfce.family import (
    DocumentStatus,
    Family,
    FamilyCorpus,
    FamilyDocument,
)
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.service.util import get_latest_ingest_start


def count_families_per_category_per_corpus(
    db: Session, allowed_corpora_ids: Optional[list[str]] = None
) -> list[tuple[str, int]]:
    """
    Get the count of families by category per corpus.

    :param db: Database session
    :param allowed_corpora_ids: The import IDs of the corpora
    :return: A list of tuples where each tuple contains a family category and its count
    """
    # Subquery to find families with at least one published document
    # Avoid using calculated family_status field for performance reasons
    published_families = (
        db.query(FamilyDocument.family_import_id)
        .filter(FamilyDocument.document_status == DocumentStatus.PUBLISHED)
        .distinct()
        .subquery()
    )

    query = db.query(Family.family_category, func.count()).join(
        published_families,
        published_families.c.family_import_id == Family.import_id,
    )

    if allowed_corpora_ids is not None and allowed_corpora_ids != []:
        query = query.join(
            FamilyCorpus, FamilyCorpus.family_import_id == Family.import_id
        ).filter(FamilyCorpus.corpus_import_id.in_(allowed_corpora_ids))

    return query.group_by(Family.family_category).all()


def count_families_per_category_per_corpus_latest_ingest_cycle(
    db: Session, allowed_corpora_ids: list[str]
) -> list[tuple[str, int]]:
    """
    Get the count of families by category per corpus.

    :param db: Database session
    :param allowed_corpora_ids: The import IDs of the corpora
    :return: A list of tuples where each tuple contains a family category and its count
    """
    # Subquery to find families with at least one published document
    # Avoid using calculated family_status field for performance reasons
    published_families = (
        db.query(FamilyDocument.family_import_id)
        .filter(FamilyDocument.document_status == DocumentStatus.PUBLISHED)
        .distinct()
        .subquery()
    )

    query = db.query(Family.family_category, func.count()).join(
        published_families,
        published_families.c.family_import_id == Family.import_id,
    )

    if allowed_corpora_ids is not None and allowed_corpora_ids != []:
        query = query.join(
            FamilyCorpus, FamilyCorpus.family_import_id == Family.import_id
        ).filter(FamilyCorpus.corpus_import_id.in_(allowed_corpora_ids))

    latest_ingest_start = get_latest_ingest_start()
    if latest_ingest_start is not None:
        query = query.filter(FamilyDocument.last_modified < latest_ingest_start)

    return query.group_by(Family.family_category).all()
