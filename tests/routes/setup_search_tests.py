from datetime import datetime
import json
from pathlib import Path
import random
from typing import Iterable, Mapping, Optional, Sequence

from sqlalchemy.orm import Session

from app.data_migrations.taxonomy_cclw import get_cclw_taxonomy
from app.data_migrations.taxonomy_unf3c import get_unf3c_taxonomy

from app.db.models.app import Organisation
from app.db.models.law_policy.family import (
    DocumentStatus,
    EventStatus,
    FamilyCategory,
    Family,
    FamilyDocument,
    FamilyDocumentType,
    FamilyEvent,
    FamilyOrganisation,
    Slug,
    Variant,
)
from app.db.models.law_policy.metadata import (
    FamilyMetadata,
)
from app.db.models.document.physical_document import (
    LanguageSource,
    PhysicalDocument,
    PhysicalDocumentLanguage,
)
from app.initial_data import run_data_migrations

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


def _populate_db_families(db: Session) -> None:
    run_data_migrations(db)
    _create_organisation(db)

    seen_family_ids = []
    for doc, family in _fixture_docs():
        if doc["fields"]["family_document_ref"] not in seen_family_ids:
            _create_family(db, family)
            _create_family_event(db, family)
            _create_family_metadata(db, family)
            seen_family_ids.append(doc["fields"]["family_document_ref"])
        _create_document(db, doc, family)


def _create_organisation(db: Session):
    for org in [
        Organisation(
            id=0, name="CCLW", description="CCLW", organisation_type="CCLW Type"
        ),
        Organisation(
            id=1, name="UNFCCC", description="UNFCCC", organisation_type="UNFCCC Type"
        ),
    ]:
        db.merge(org)
        db.commit()


def _create_family(db: Session, family: VespaFixture):
    family_id = _parse_id(family)
    family_import_id = family["fields"]["family_import_id"]

    family_object = Family(
        title=family["fields"]["family_name"],
        import_id=family_import_id,
        description=family["fields"]["family_description"],
        geography_id=1,
        family_category=FamilyCategory(family["fields"]["family_category"]),
    )
    db.add(family_object)
    db.commit()

    family_slug = Slug(
        name=family_id,
        family_import_id=family_import_id,
        family_document_import_id=None,
    )

    if family["fields"]["family_source"] == "CCLW":
        orgid = 0
    elif family["fields"]["family_source"] == "UNFCCC":
        orgid = 1
    else:
        raise ValueError(f"Unexpected value in: {family['fields']['family_source']}")

    family_organisation = FamilyOrganisation(
        family_import_id=family_import_id,
        organisation_id=orgid,
    )

    db.add(family_slug)
    db.commit()
    db.add(family_organisation)
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
        name=f"fd_{_parse_id(doc)}",
        family_import_id=None,
        family_document_import_id=doc_import_id,
    )

    db.add(family_document)
    db.commit()
    db.add(family_document_slug)
    db.commit()
    db.refresh(family_document)