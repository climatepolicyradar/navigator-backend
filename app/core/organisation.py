from typing import Sequence, cast

from db_client.models.dfce.family import Corpus, Family, FamilyCategory, FamilyCorpus
from db_client.models.organisation import CorpusType, Organisation
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.api.api_v1.schemas.metadata import CorpusData, OrganisationConfig
from app.core import config


def _to_corpus_data(row) -> CorpusData:
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
        taxonomy={**row.taxonomy},
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

    return [_to_corpus_data(row) for row in corpora]


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


def get_all_organisations(db: Session) -> list[Organisation]:
    return db.query(Organisation).all()
