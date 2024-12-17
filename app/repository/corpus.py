from typing import Any

from db_client.models.dfce.family import Corpus, Family, FamilyCategory, FamilyCorpus
from db_client.models.organisation import CorpusType, Organisation
from sqlalchemy import func
from sqlalchemy.orm import Session

from app import config
from app.models.metadata import CorpusConfig


def _get_family_stats_per_corpus(db: Session, corpus_import_id: str) -> dict[str, Any]:
    total = (
        db.query(Family)
        .join(FamilyCorpus, FamilyCorpus.family_import_id == Family.import_id)
        .filter(FamilyCorpus.corpus_import_id == corpus_import_id)
        .count()
    )

    counts = (
        db.query(Family.family_category, func.count())
        .join(FamilyCorpus, FamilyCorpus.family_import_id == Family.import_id)
        .filter(FamilyCorpus.corpus_import_id == corpus_import_id)
        .group_by(Family.family_category)
        .all()
    )
    found_categories = {c[0].value: c[1] for c in counts}
    count_by_category = {}

    # Supply zeros when there aren't any
    for category in [e.value for e in FamilyCategory]:
        if category in found_categories.keys():
            count_by_category[category] = found_categories[category]
        else:
            count_by_category[category] = 0

    return {"total": total, "count_by_category": count_by_category}


def _to_corpus_config(row, stats: dict[str, Any]) -> CorpusConfig:
    image_url = (
        f"https://{config.CDN_DOMAIN}/{row.image_url}"
        if row.image_url is not None and len(row.image_url) > 0
        else ""
    )
    corpus_text = row.text if row.text is not None else ""
    return CorpusConfig(
        corpus_import_id=row.corpus_import_id,
        title=row.title,
        description=row.description,
        corpus_type=row.corpus_type,
        corpus_type_description=row.corpus_type_description,
        taxonomy={**row.taxonomy},
        text=corpus_text,
        image_url=image_url,
        organisation_id=row.organisation_id,
        organisation_name=row.organisation_name,
        total=stats["total"],
        count_by_category=stats["count_by_category"],
    )


def get_config_for_allowed_corpora(
    db: Session, allowed_corpora: list[str]
) -> list[CorpusConfig]:
    query = (
        db.query(
            Corpus.import_id.label("corpus_import_id"),
            Corpus.title.label("title"),
            Corpus.description.label("description"),
            Corpus.corpus_image_url.label("image_url"),
            Corpus.corpus_text.label("text"),
            Corpus.corpus_type_name.label("corpus_type"),
            CorpusType.description.label("corpus_type_description"),
            CorpusType.valid_metadata.label("taxonomy"),
            Organisation.id.label("organisation_id"),
            Organisation.name.label("organisation_name"),
        )
        .join(
            CorpusType,
            Corpus.corpus_type_name == CorpusType.name,
        )
        .join(Organisation, Corpus.organisation_id == Organisation.id)
    )
    if allowed_corpora != []:
        query = query.filter(Corpus.import_id.in_(allowed_corpora))

    corpora = query.all()

    return [
        _to_corpus_config(
            row, _get_family_stats_per_corpus(db=db, corpus_import_id=row[0])
        )
        for row in corpora
    ]
