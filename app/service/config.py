from typing import Any, cast

from db_client.models.dfce.family import FamilyCategory
from db_client.models.organisation import Corpus, CorpusType, Organisation
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

    :param db: Database session
    :param corpus_import_id: The import ID of the corpus
    :return: A dictionary containing total families and count by category
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


def _to_corpus_type_config(
    corpus: Corpus,
    corpus_type: CorpusType,
    organisation: Organisation,
    stats: dict[str, Any],
) -> CorpusTypeConfig:
    """
    Convert corpus, corpus type, organisation, and stats to CorpusTypeConfig.

    :param corpus: Corpus object
    :param corpus_type: CorpusType object
    :param organisation: Organisation object
    :param stats: A dictionary containing statistics
    :return: A CorpusTypeConfig object
    """
    image_url = (
        f"https://{config.CDN_DOMAIN}/{corpus.corpus_image_url}"
        if corpus.corpus_image_url is not None and len(str(corpus.corpus_image_url)) > 0
        else ""
    )
    corpus_text = corpus.corpus_text if corpus.corpus_text is not None else ""
    return CorpusTypeConfig(
        corpus_type_name=str(corpus_type.name),
        corpus_type_description=str(corpus_type.description),
        taxonomy={**cast(dict, corpus_type.valid_metadata)},
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
    """
    Get configuration for a corpus type.

    :param db: Database session
    :param corpus: Corpus object
    :return: A dictionary containing CorpusTypeConfig
    """
    stats = _get_family_stats_per_corpus(db, str(corpus.import_id))
    corpus_type = corpus_type_repo.get(db, str(corpus.corpus_type_name))
    organisation = org_repo.get(db, int(str(corpus.organisation_id)))
    return {
        str(corpus_type.name): _to_corpus_type_config(
            corpus, corpus_type, organisation, stats
        )
    }


# def get_corpus_type_config_for_allowed_corpora(
#     db: Session, allowed_corpora: list[str]
# ) -> Mapping[str, CorpusTypeConfig]:
#     """
#     Get CorpusTypeConfig for allowed corpora.

#     :param db: Database session
#     :param allowed_corpora: A list of allowed corpora
#     :return: A mapping of CorpusTypeConfig for allowed corpora
#     """
#     corpora = get_allowed_corpora(db, allowed_corpora)

#     configs_for_each_allowed_corpus = (
#         _get_config_for_corpus_type(db, corpus) for corpus in corpora
#     )
#     corpus_type_config_for_allowed_corpora = {
#         k: v for config in configs_for_each_allowed_corpus for k, v in config.items()
#     }

#     return corpus_type_config_for_allowed_corpora


def get_corpus_type_config_for_allowed_corpora(db: Session, allowed_corpora: list[str]):
    corpora = get_allowed_corpora(db, allowed_corpora)

    main_dictionaries = {}

    for corpus in corpora:
        if corpus.corpus_type_name not in main_dictionaries:
            main_dictionaries[corpus.corpus_type_name] = map_config_for_corpus_type(
                db, corpus
            )
    for corpus in corpora:
        new_corpus = transform_corpus(db, corpus)
        # note for readability you c
        main_dictionaries[corpus.corpus_type_name].corpora.append(new_corpus)

    return main_dictionaries


def map_config_for_corpus_type(db, corpus):
    """
    Get configuration for a corpus type.

    :param db: Database session
    :param corpus: Corpus object
    :return: A dictionary containing CorpusTypeConfig
    """
    corpus_type = corpus_type_repo.get(db, str(corpus.corpus_type_name))

    return CorpusTypeConfig(
        corpus_type_name=str(corpus_type.name),
        corpus_type_description=str(corpus_type.description),
        taxonomy={**cast(dict, corpus_type.valid_metadata)},
        corpora=[],
    )


def transform_corpus(db, corpus):
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


# def _to_corpus_type_config(
#     corpus: Corpus,
#     corpus_type: CorpusType,
#     organisation: Organisation,
#     stats: dict[str, Any],
# ) -> CorpusTypeConfig:
#     """
#     Convert corpus, corpus type, organisation, and stats to CorpusTypeConfig.

#     :param corpus: Corpus object
#     :param corpus_type: CorpusType object
#     :param organisation: Organisation object
#     :param stats: A dictionary containing statistics
#     :return: A CorpusTypeConfig object
#     """
#     image_url = (
#         f"https://{config.CDN_DOMAIN}/{corpus.corpus_image_url}"
#         if corpus.corpus_image_url is not None and len(str(corpus.corpus_image_url)) > 0
#         else ""
#     )
#     corpus_text = corpus.corpus_text if corpus.corpus_text is not None else ""
#     return CorpusTypeConfig(
#         corpus_type_name=str(corpus_type.name),
#         corpus_type_description=str(corpus_type.description),
#         taxonomy={**cast(dict, corpus_type.valid_metadata)},
#         corpora=[
#             CorpusConfig(
#                 title=str(corpus.title),
#                 description=str(corpus.description),
#                 corpus_import_id=str(corpus.import_id),
#                 text=str(corpus_text),
#                 image_url=image_url,
#                 organisation_id=int(str(organisation.id)),
#                 organisation_name=str(organisation.name),
#                 total=stats["total"],
#                 count_by_category=stats["count_by_category"],
#             )
#         ],
#     )


# Step 1 -

# Map the main_config_dictionary - "Laws & Policies " : { corpora: [], corpus_type: {}, corpus_type_description: {}, taxonomy: {} }
# Ensure that we only map once - i.e if there are three corpus of laws and policies corpus type then we only map the map object once
# Then when use the corpora information in the corpora array, and then just do a conditional / looping where its if corpus type in main_config_dictionary then we append corpus from corpora array to main_config_dictionary.corpora
