import logging
from typing import Optional

from db_client.models.dfce.family import (
    DocumentStatus,
    Family,
    FamilyCategory,
    FamilyCorpus,
    FamilyDocument,
)
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.service.util import get_latest_ingest_start

_LOGGER = logging.getLogger(__name__)


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
) -> list[tuple[FamilyCategory, int]]:
    """
    Get the count of families by category per corpus for the latest ingest cycle.

    This replicates the logic from the download query to count families that have
    published documents last modified before the latest ingest cycle start date.

    :param db: Database session
    :param allowed_corpora_ids: The import IDs of the corpora to filter by
    :return: A list of tuples where each tuple contains a family category and its count
    """
    latest_ingest_start = get_latest_ingest_start()
    _LOGGER.info(f"Latest ingest start: {latest_ingest_start}")

    # 🔍 Debug: Log the timestamp being used
    _LOGGER.info(f"Filtering documents by last_modified < {latest_ingest_start}")

    published_families_query = db.query(FamilyDocument.family_import_id).filter(
        FamilyDocument.document_status == DocumentStatus.PUBLISHED
    )

    if latest_ingest_start is not None:
        published_families_query = published_families_query.filter(
            FamilyDocument.last_modified < latest_ingest_start
        )

    published_families_latest_cycle = published_families_query.distinct().subquery()

    query = db.query(Family.family_category, func.count()).join(
        published_families_latest_cycle,
        published_families_latest_cycle.c.family_import_id == Family.import_id,
    )
    if allowed_corpora_ids is not None and allowed_corpora_ids != []:
        query = query.join(
            FamilyCorpus, FamilyCorpus.family_import_id == Family.import_id
        ).filter(FamilyCorpus.corpus_import_id.in_(allowed_corpora_ids))

    return query.group_by(Family.family_category).all()


def _convert_to_dto(counts: list[tuple[FamilyCategory, int]]) -> dict[str, int]:
    return {count[0].value: int(count[1]) for count in counts}
