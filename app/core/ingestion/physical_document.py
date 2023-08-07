from logging import getLogger
from typing import Any

from sqlalchemy.orm import Session
from app.core.ingestion.params import IngestParameters
from app.core.ingestion.utils import to_dict

from app.db.models.document import PhysicalDocument
from app.db.models.document.physical_document import (
    Language,
    LanguageSource,
    PhysicalDocumentLanguage,
)

_LOGGER = getLogger(__name__)


def create_physical_document_from_params(
    db: Session,
    params: IngestParameters,
    result: dict[str, Any],
) -> PhysicalDocument:
    """
    Create the document part of the schema from the row.

    :param [Session] db: connection to the database.
    :param IngestParameters params: The parameters for the ingest.
    :param dict[str, Any] result: The result of the ingest
    :return [dict[str, Any]]: a dictionary to describe what was created.
    """
    physical_document = PhysicalDocument(
        title=params.document_title,
        source_url=params.source_url,
        md5_sum=None,
        content_type=None,
        cdn_object=None,
    )
    db.add(physical_document)
    db.flush()
    result["physical_document"] = to_dict(physical_document)

    update_physical_document_languages(db, params.language, result, physical_document)

    return physical_document


def update_physical_document_languages(
    db: Session,
    langs: list[str],
    result: dict[str, Any],
    physical_document: PhysicalDocument,
) -> None:
    """
    Updates the physical document with the languages in param.

    :param Session db: connection to the db.
    :param list[str] langs: List of languages.
    :param dict[str, Any] result: The result of the ingest
    :param PhysicalDocument physical_document: The physical document to update
    """
    existing_language_links: dict[int, PhysicalDocumentLanguage] = {
        pdl.language_id: pdl
        for pdl in db.query(PhysicalDocumentLanguage)
        .filter_by(document_id=physical_document.id)
        .all()
    }

    result_document_languages = []
    result_physical_document_languages = []
    for language in langs:
        lang = db.query(Language).filter(Language.name == language).one_or_none()
        if lang is None:
            _LOGGER.error(
                f"Ingest for physical_document '{physical_document.id}' attempted to "
                f"assign langauge '{language}' which does not exist in the database"
            )
            continue

        if lang.id in existing_language_links:
            physical_document_language = existing_language_links[lang.id]

            # If the Language source is already set to USER, do not toggle visibility
            should_update_language_link = (
                physical_document_language.source != LanguageSource.USER
                and not physical_document_language.visible
            )
            if should_update_language_link:
                physical_document_language.source = LanguageSource.USER  # type: ignore
                physical_document_language.visible = True  # type: ignore
                db.flush()
                result_physical_document_languages.append(
                    to_dict(physical_document_language)
                )
        else:
            physical_document_language = PhysicalDocumentLanguage(
                language_id=lang.id,
                document_id=physical_document.id,
                source=LanguageSource.USER,
                visible=True,
            )
            db.add(physical_document_language)
            db.flush()
            result_document_languages.append(to_dict(lang))
            result_physical_document_languages.append(
                to_dict(physical_document_language)
            )

    if result_document_languages:
        result["language"] = result_document_languages
    if result_physical_document_languages:
        result["physical_document_language"] = result_physical_document_languages
