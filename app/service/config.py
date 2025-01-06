from typing import Any, Mapping

from db_client.models.dfce.family import FamilyCategory
from db_client.models.organisation import Corpus, CorpusType, Organisation
from sqlalchemy import cast
from sqlalchemy.orm import Session

from app import config
from app.models.config import CorpusConfig, CorpusTypeConfig
from app.repository import corpus_type as corpus_type_repo
from app.repository import organisation as org_repo
from app.repository.corpus import (
    get_allowed_corpora,
    get_family_count_by_category_per_corpus,
    get_total_families_per_corpus,
)


def _get_family_stats_per_corpus(db: Session, corpus_import_id: str) -> dict[str, Any]:
    total = get_total_families_per_corpus(db, corpus_import_id)

    counts = get_family_count_by_category_per_corpus(db, corpus_import_id)
    found_categories = {c[0].value: c[1] for c in counts}
    count_by_category = {}

    # Supply zeros when there aren't any
    for category in [e.value for e in FamilyCategory]:
        if category in found_categories.keys():
            count_by_category[category] = found_categories[category]
        else:
            count_by_category[category] = 0

    return {"total": total, "count_by_category": count_by_category}


def _to_corpus_type_config(
    corpus: Corpus,
    corpus_type: CorpusType,
    organisation: Organisation,
    stats: dict[str, Any],
) -> CorpusTypeConfig:
    image_url = (
        f"https://{config.CDN_DOMAIN}/{corpus.corpus_image_url}"
        if corpus.corpus_image_url is not None and len(str(corpus.corpus_image_url)) > 0
        else ""
    )
    corpus_text = corpus.corpus_text if corpus.corpus_text is not None else ""
    return CorpusTypeConfig(
        corpus_type_name=str(corpus_type.name),
        corpus_type_description=str(corpus_type.description),
        taxonomy={**cast(corpus_type.valid_metadata)},
        corpora=[
            CorpusConfig(
                title=str(corpus.title),
                description=str(corpus.description),
                corpus_import_id=str(corpus.import_id),
                text=str(corpus_text),
                image_url=image_url,
                organisation_id=int(str(organisation.id)),
                organisation_name=str(organisation.name),
                total=stats["total"],
                count_by_category=stats["count_by_category"],
            )
        ],
    )


def _get_config_for_corpus_type(
    db: Session, corpus: Corpus
) -> dict[str, CorpusTypeConfig]:
    stats = _get_family_stats_per_corpus(db, str(corpus.import_id))
    corpus_type = corpus_type_repo.get(db, str(corpus.corpus_type_name))
    organisation = org_repo.get(db, int(str(corpus.organisation_id)))
    return {
        str(corpus_type.name): _to_corpus_type_config(
            corpus, corpus_type, organisation, stats
        )
    }


def get_corpus_type_config_for_allowed_corpora(
    db: Session, allowed_corpora: list[str]
) -> Mapping[str, CorpusTypeConfig]:

    corpora = get_allowed_corpora(db, allowed_corpora)

    configs_for_each_allowed_corpus = (
        _get_config_for_corpus_type(db, corpus) for corpus in corpora
    )
    corpus_type_config_for_allowed_corpora = {
        k: v for config in configs_for_each_allowed_corpus for k, v in config.items()
    }

    return corpus_type_config_for_allowed_corpora
