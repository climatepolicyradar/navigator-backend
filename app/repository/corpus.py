from db_client.models.dfce.family import Corpus
from db_client.models.organisation import CorpusType
from sqlalchemy.orm import Session

from app import config
from app.models.metadata import CorpusConfig


def _to_corpus_data(row) -> CorpusConfig:
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
    )


def get_allowed_corpora(db: Session, allowed_corpora: list[str]) -> list[CorpusConfig]:
    query = db.query(
        Corpus.import_id.label("corpus_import_id"),
        Corpus.title.label("title"),
        Corpus.description.label("description"),
        Corpus.corpus_image_url.label("image_url"),
        Corpus.corpus_text.label("text"),
        Corpus.corpus_type_name.label("corpus_type"),
        CorpusType.description.label("corpus_type_description"),
        CorpusType.valid_metadata.label("taxonomy"),
    ).join(
        Corpus,
        Corpus.corpus_type_name == CorpusType.name,
    )
    if allowed_corpora != []:
        query = query.filter(Corpus.import_id.in_(allowed_corpora))

    corpora = query.all()

    return [_to_corpus_data(row) for row in corpora]
