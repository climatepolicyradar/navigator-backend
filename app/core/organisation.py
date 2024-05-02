from dataclasses import asdict
from typing import Sequence, cast

from db_client.models.dfce.family import (
    Corpus,
    Family,
    FamilyCategory,
    FamilyCorpus,
    FamilyEventType,
)
from db_client.models.dfce.taxonomy_entry import Taxonomy, TaxonomyEntry
from db_client.models.organisation import CorpusType, Organisation
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.api.api_v1.schemas.metadata import CorpusData, OrganisationConfig
from app.core import config


def get_organisation_taxonomy(db: Session, org_id: int) -> Taxonomy:
    """
    Returns the taxonomy id and its dict representation for an organisation.

    :param Session db: connection to the database
    :param int org_id: organisation id
    :return tuple[int, Taxonomy]: the taxonomy id and the Taxonomy
    """
    taxonomy = (
        db.query(CorpusType.valid_metadata)
        .join(
            Corpus,
            CorpusType.name == Corpus.corpus_type_name,
        )
        .filter(Corpus.organisation_id == org_id)
        .one()
    )
    # The above line will throw if there is no taxonomy for the organisation

    return {k: TaxonomyEntry(**v) for k, v in taxonomy[0].items()}


def _to_corpus_data(row, event_types) -> CorpusData:
    image_url = (
        f"https://{config.CDN_DOMAIN}/{row.image_url}" if len(row.image_url) > 0 else ""
    )
    return CorpusData(
        corpus_import_id=row.corpus_import_id,
        title=row.title,
        description=row.description,
        corpus_type=row.corpus_type,
        corpus_type_description=row.corpus_type_description,
        image_url=image_url,
        text=row.text,
        taxonomy={
            **row.taxonomy,
            "event_types": asdict(event_types),
        },
    )


def get_corpora_for_org(db: Session, org_name: str) -> Sequence[CorpusData]:
    corpora = (
        db.query(
            Corpus.import_id.label("corpus_import_id"),
            Corpus.title.label("title"),
            Corpus.description.label("description"),
            Corpus.corpus_image_url.label("image_url"),
            Corpus.corpus_text.label("text"),
            Corpus.corpus_type_name.label("corpus_type"),
            CorpusType.description.label("corpus_type_description"),
            CorpusType.valid_metadata.label("taxonomy"),
        )
        .join(
            Corpus,
            Corpus.corpus_type_name == CorpusType.name,
        )
        .join(Organisation, Organisation.id == Corpus.organisation_id)
        .filter(Organisation.name == org_name)
        .all()
    )

    event_types = db.query(FamilyEventType).all()
    entry = TaxonomyEntry(
        allow_blanks=False,
        allowed_values=[r.name for r in event_types],
        allow_any=False,
    )
    return [_to_corpus_data(row, entry) for row in corpora]


def get_organisation_config(db: Session, org: Organisation) -> OrganisationConfig:
    total = (
        db.query(Family)
        .join(FamilyCorpus, FamilyCorpus.family_import_id == Family.import_id)
        .join(Corpus, Corpus.import_id == FamilyCorpus.corpus_import_id)
        .filter(Corpus.organisation_id == org.id)
        .count()
    )

    counts = (
        db.query(Family.family_category, func.count())
        .join(FamilyCorpus, FamilyCorpus.family_import_id == Family.import_id)
        .join(Corpus, Corpus.import_id == FamilyCorpus.corpus_import_id)
        .filter(Corpus.organisation_id == org.id)
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

    org_name = cast(str, org.name)
    return OrganisationConfig(
        total=total,
        count_by_category=count_by_category,
        corpora=get_corpora_for_org(db, org_name),
    )
