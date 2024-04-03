from datetime import datetime
import json
from pathlib import Path
import random
from typing import Iterable, Mapping, Optional, Sequence

from sqlalchemy.orm import Session

from db_client.data_migrations.taxonomy_cclw import get_cclw_taxonomy
from db_client.data_migrations.taxonomy_unf3c import get_unf3c_taxonomy

from db_client.models.organisation import Corpus
from db_client.models.dfce.family import (
    DocumentStatus,
    EventStatus,
    FamilyCategory,
    Family,
    FamilyDocument,
    FamilyDocumentType,
    FamilyEvent,
    Slug,
    Variant,
    FamilyCorpus,
)
from db_client.models.dfce.metadata import (
    FamilyMetadata,
)
from db_client.models.dfce import Geography
from db_client.models.document.physical_document import (
    LanguageSource,
    PhysicalDocument,
    PhysicalDocumentLanguage,
)


VESPA_FIXTURE_COUNT = 5
FIXTURE_DIR = Path(__file__).parents[1] / "search_fixtures"
VESPA_FAMILY_PATH = FIXTURE_DIR / "vespa_family_document.json"
VESPA_DOCUMENT_PATH = FIXTURE_DIR / "vespa_document_passage.json"

VespaFixture = Mapping[str, Mapping[str, str]]


def _parse_id(
    schema: VespaFixture,
    convert_to: Optional[str] = None,
) -> str:
    schema_id = schema["id"].split("::")[-1]  # type: ignore
    if convert_to is None:
        return schema_id
    else:
        id_parts = schema_id.split(".")
    return f"{id_parts[0]}.{convert_to}.{id_parts[2]}.{id_parts[3]}"


def _get_family_fixture(doc: VespaFixture) -> VespaFixture:
    with open(VESPA_FAMILY_PATH, "r") as vf:
        for family in json.load(vf):
            if family["id"] == doc["fields"]["family_document_ref"]:
                return family
    raise ValueError(f"No family found for fixture: \n{doc}")


def _fixture_docs() -> Iterable[tuple[VespaFixture, VespaFixture]]:
    with open(VESPA_DOCUMENT_PATH, "r") as vd:
        documents = json.load(vd)

    for doc in documents:
        family = _get_family_fixture(doc)
        yield doc, family


def _populate_db_families(db: Session, max_docs: int = VESPA_FIXTURE_COUNT) -> None:
    """
    Sets up the database using fixtures

    Lower `max_docs` to limit the number of fixtures added to the db.
    """

    seen_family_ids = []
    for count, (doc, family) in enumerate(_fixture_docs(), start=1):
        if doc["fields"]["family_document_ref"] not in seen_family_ids:
            _create_family(db, family)
            _create_family_event(db, family)
            _create_family_metadata(db, family)
            seen_family_ids.append(doc["fields"]["family_document_ref"])
        _create_document(db, doc, family)
        if count == max_docs:
            return


def _create_family(db: Session, family: VespaFixture):
    family_import_id = family["fields"]["family_import_id"]

    geo = family["fields"]["family_geography"]
    geography = db.query(Geography).filter(Geography.value == geo).one()

    family_object = Family(
        title=family["fields"]["family_name"],
        import_id=family_import_id,
        description=family["fields"]["family_description"],
        geography_id=geography.id,
        family_category=FamilyCategory(family["fields"]["family_category"]),
    )
    db.add(family_object)
    db.commit()

    family_slug = Slug(
        name=family["fields"]["family_slug"],
        family_import_id=family_import_id,
        family_document_import_id=None,
    )

    org = family["fields"]["family_source"]
    corpus = db.query(Corpus).filter(Corpus.import_id.like(f"{org}%")).one()
    db.add(
        FamilyCorpus(
            family_import_id=family_object.import_id, corpus_import_id=corpus.import_id
        )
    )
    db.add(family_slug)
    db.commit()
    db.refresh(family_object)


def _create_family_event(db: Session, family: VespaFixture):
    event_id = _parse_id(family, convert_to="event")
    family_id = _parse_id(family)

    family_import_id = family["fields"]["family_import_id"]
    family_event = FamilyEvent(
        import_id=event_id,
        title=f"{family_id} Event",
        date=datetime.fromisoformat(family["fields"]["family_publication_ts"]),
        event_type_name="Passed/Approved",
        family_import_id=family_import_id,
        family_document_import_id=None,
        status=EventStatus.OK,
    )
    db.add(family_event)
    db.commit()


def _generate_synthetic_metadata(
    taxonomy: Mapping[str, dict]
) -> Mapping[str, Sequence[str]]:
    meta_value = {}
    for k in taxonomy:
        allowed_values = taxonomy[k]["allowed_values"]
        element_count = random.randint(0, len(allowed_values))
        meta_value[k] = random.sample(allowed_values, element_count)
    return meta_value


def _create_family_metadata(db: Session, family: VespaFixture):
    if family["fields"]["family_source"] == "UNFCCC":
        taxonomy = get_unf3c_taxonomy()
    elif family["fields"]["family_source"] == "CCLW":
        taxonomy = get_cclw_taxonomy()
    else:
        raise ValueError(
            f"Could not get taxonomy for: {family['fields']['family_source']}"
        )
    metadata_value = _generate_synthetic_metadata(taxonomy)

    family_import_id = family["fields"]["family_import_id"]
    family_metadata = FamilyMetadata(
        family_import_id=family_import_id,
        taxonomy_id=1,
        value=metadata_value,
    )
    db.add(family_metadata)
    db.commit()


def _create_document(
    db: Session,
    doc: VespaFixture,
    family: VespaFixture,
):
    physical_document = PhysicalDocument(
        title="doc name",
        cdn_object="cdn_object",
        md5_sum="md5_sum",
        source_url="source_url",
        content_type="content_type",
    )

    db.add(physical_document)
    db.commit()
    db.refresh(physical_document)
    physical_document_language = PhysicalDocumentLanguage(
        language_id=1826,  # English
        document_id=physical_document.id,
        source=LanguageSource.USER,
        visible=True,
    )
    db.add(physical_document_language)
    db.commit()
    db.refresh(physical_document_language)
    db.refresh(physical_document)

    if len(family["fields"]["document_languages"]) > 0:
        variant = Variant(variant_name="Official Translation", description="")
    else:
        variant = Variant(variant_name="Original Language", description="")
    db.merge(variant)
    db.commit()

    doc_type = FamilyDocumentType(
        name=family["fields"]["family_category"], description=""
    )
    db.merge(doc_type)
    db.commit()

    family_import_id = family["fields"]["family_import_id"]
    doc_import_id = family["fields"]["document_import_id"]

    family_document = FamilyDocument(
        family_import_id=family_import_id,
        physical_document_id=physical_document.id,
        import_id=doc_import_id,
        variant_name=variant.variant_name,
        document_status=DocumentStatus.PUBLISHED,
        document_type=doc_type.name,
    )

    family_document_slug = Slug(
        name=family["fields"]["document_slug"],
        family_import_id=None,
        family_document_import_id=doc_import_id,
    )

    db.add(family_document)
    db.commit()
    db.add(family_document_slug)
    db.commit()
    db.refresh(family_document)
