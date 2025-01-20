from typing import Any, Mapping, cast

from db_client.models.dfce.family import FamilyCategory
from db_client.models.organisation import Corpus
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
    """
    Get family statistics per corpus.

    :param Session db: Database session
    :param str corpus_import_id: The import ID of the corpus
    :return dict[str, Any]: A dictionary containing total families and count by category
    """
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


def _to_corpus_type_config(db: Session, corpus: Corpus) -> CorpusTypeConfig:
    """
    Get configuration for a corpus type.

    :param Session db: Database session
    :param Corpus corpus: Corpus object
    :return CorpusTypeConfig: A dictionary containing CorpusTypeConfig for a single corpus type
    without the related corpora
    """
    corpus_type = corpus_type_repo.get(db, str(corpus.corpus_type_name))

    return CorpusTypeConfig(
        corpus_type_name=str(corpus_type.name),
        corpus_type_description=str(corpus_type.description),
        taxonomy={**cast(dict, corpus_type.valid_metadata)},
        corpora=[],
    )


def _to_corpus_config(db, corpus) -> CorpusConfig:
    """
    Convert corpus, organisation, and stats to CorpusConfig.

    :param Corpus corpus: A Corpus object
    :return CorpusConfig: An object containing config for a specific corpus
    """
    stats = _get_family_stats_per_corpus(db, str(corpus.import_id))
    organisation = org_repo.get(db, int(str(corpus.organisation_id)))
    image_url = (
        f"https://{config.CDN_DOMAIN}/{corpus.corpus_image_url}"
        if corpus.corpus_image_url is not None and len(str(corpus.corpus_image_url)) > 0
        else ""
    )
    corpus_text = corpus.corpus_text if corpus.corpus_text is not None else ""

    return CorpusConfig(
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


def get_corpus_type_config_for_allowed_corpora(
    db: Session, allowed_corpora: list[str]
) -> Mapping[str, CorpusTypeConfig]:
    """
    Get CorpusTypeConfig for allowed corpora.

    :param Session db: Database session
    :param list[str] allowed_corpora: A list of allowed corpora
    :return CorpusTypeConfig: A mapping of CorpusTypeConfig for allowed corpora
    """
    corpora = get_allowed_corpora(db, allowed_corpora)

    corpus_type_config = {}

    for corpus in corpora:
        if corpus.corpus_type_name not in corpus_type_config:
            corpus_type_config[corpus.corpus_type_name] = _to_corpus_type_config(
                db, corpus
            )
    for corpus in corpora:
        new_corpus = _to_corpus_config(db, corpus)
        corpus_type_config[corpus.corpus_type_name].corpora.append(new_corpus)

    return corpus_type_config
