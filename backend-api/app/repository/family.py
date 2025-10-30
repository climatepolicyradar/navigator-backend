from typing import Optional

from db_client.models.dfce.family import (
    DocumentStatus,
    Family,
    FamilyCategory,
    FamilyCorpus,
    FamilyDocument,
)
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.service.util import get_latest_ingest_start


def count_families_per_category_per_corpus(
    db: Session, allowed_corpora_ids: Optional[list[str]] = None
) -> list[tuple[FamilyCategory, int]]:
    """
    Get the count of families by category per corpus.

    :param db: Database session
    :param allowed_corpora_ids: The import IDs of the corpora
    :return: A list of tuples where each tuple contains a family category and its count
    """
    # Subquery to find families with at least one published document
    # Avoid using calculated family_status field for performance reasons
    published_families = (
        select(FamilyDocument.family_import_id)
        .where(FamilyDocument.document_status == DocumentStatus.PUBLISHED)
        .distinct()
        .subquery()
    )

    query = (
        select(Family.family_category, func.count())
        .select_from(Family)
        .join(
            published_families,
            published_families.c.family_import_id == Family.import_id,
        )
    )

    if allowed_corpora_ids is not None and allowed_corpora_ids != []:
        query = query.join(
            FamilyCorpus, FamilyCorpus.family_import_id == Family.import_id
        ).where(FamilyCorpus.corpus_import_id.in_(allowed_corpora_ids))

    query = query.group_by(Family.family_category)
    return [tuple(row) for row in db.execute(query).all()]


def count_families_per_category_per_corpus_latest_ingest_cycle(
    db: Session, allowed_corpora_ids: list[str]
) -> list[tuple[FamilyCategory, int]]:
    """
    Get the count of families by category per corpus.

    :param db: Database session
    :param allowed_corpora_ids: The import IDs of the corpora
    :return: A list of tuples where each tuple contains a family category and its count
    """
    # Subquery to find families with at least one published document
    # Avoid using calculated family_status field for performance reasons
    published_families = (
        select(FamilyDocument.family_import_id)
        .where(FamilyDocument.document_status == DocumentStatus.PUBLISHED)
        .distinct()
        .subquery()
    )

    query = (
        select(Family.family_category, func.count())
        .select_from(Family)
        .join(
            published_families,
            published_families.c.family_import_id == Family.import_id,
        )
        .join(FamilyDocument, FamilyDocument.family_import_id == Family.import_id)
    )

    if allowed_corpora_ids is not None and allowed_corpora_ids != []:
        query = query.join(
            FamilyCorpus, FamilyCorpus.family_import_id == Family.import_id
        ).where(FamilyCorpus.corpus_import_id.in_(allowed_corpora_ids))

    latest_ingest_start = get_latest_ingest_start()
    if latest_ingest_start is not None:
        query = query.where(FamilyDocument.last_modified < latest_ingest_start)

    query = query.group_by(Family.family_category)
    return [tuple(row) for row in db.execute(query).all()]


def _convert_to_dto(counts: list[tuple[FamilyCategory, int]]) -> dict[str, int]:
    return {count[0].value: int(count[1]) for count in counts}
