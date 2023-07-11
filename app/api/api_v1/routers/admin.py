import logging

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Request,
    status,
)
from sqlalchemy import update

from app.api.api_v1.schemas.document import (
    DocumentUpdateRequest,
)
from app.core.auth import get_superuser_details
from app.core.validation import IMPORT_ID_MATCHER
from app.db.models.document.physical_document import (
    PhysicalDocument,
    Language,
    PhysicalDocumentLanguage,
)
from app.db.models.law_policy.family import FamilyDocument, Slug
from app.db.session import get_db

_LOGGER = logging.getLogger(__name__)

admin_document_router = r = APIRouter()


@r.put("/documents/{import_id_or_slug}", status_code=status.HTTP_200_OK)
async def update_document(
    request: Request,
    import_id_or_slug: str,
    meta_data: DocumentUpdateRequest,
    db=Depends(get_db),
    current_user=Depends(get_superuser_details),
):
    # TODO: As this grows move it out into the crud later.

    _LOGGER.info(
        f"Superuser '{current_user.email}' called update_document",
        extra={
            "props": {
                "superuser_email": current_user.email,
                "import_id_or_slug": import_id_or_slug,
                "meta_data": meta_data.as_json(),
            }
        },
    )

    # First query the FamilyDocument
    query = db.query(FamilyDocument)
    if IMPORT_ID_MATCHER.match(import_id_or_slug) is not None:
        family_document = query.filter(
            FamilyDocument.import_id == import_id_or_slug
        ).one_or_none()
        _LOGGER.info("update_document called with import_id")
    else:
        family_document = (
            query.join(Slug, Slug.family_document_import_id == FamilyDocument.import_id)
            .filter(Slug.name == import_id_or_slug)
            .one_or_none()
        )
        _LOGGER.info("update_document called with slug")

    # Check we have found one
    if family_document is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        )

    # Get the physical document to update
    physical_document = family_document.physical_document

    # Note this code relies on the fields being the same as the db column names
    num_changed = db.execute(
        update(PhysicalDocument)
        .values(meta_data.physical_doc_keys_json())
        .where(PhysicalDocument.id == physical_document.id)
    ).rowcount

    # Update the languages
    if meta_data.languages is not None:
        _LOGGER.info(
            "Adding meta_data object languages to the database.",
            extra={
                "props": {
                    "meta_data_languages": meta_data.languages,
                    "import_id_or_slug": import_id_or_slug,
                }
            },
        )

        physical_document_languages = (
            db.query(PhysicalDocumentLanguage, Language)
            .filter(PhysicalDocumentLanguage.document_id == physical_document.id)
            .join(Language, Language.id == PhysicalDocumentLanguage.language_id)
            .all()
        )
        existing_language_codes = {
            lang.language_code for _, lang in physical_document_languages
        }

        for language in meta_data.languages:
            if len(language) == 2:  # iso639-1 two letter language code
                lang = (
                    db.query(Language)
                    .filter(Language.part1_code == language)
                    .one_or_none()
                )
            elif len(language) == 3:  # iso639-2/3 three letter language code
                lang = (
                    db.query(Language)
                    .filter(Language.language_code == language)
                    .one_or_none()
                )
            else:
                _LOGGER.warning(
                    "Retrieved no language from database for meta_data object language.",
                    extra={
                        "props": {
                            "metadata_language": language,
                            "import_id_or_slug": import_id_or_slug,
                        }
                    },
                )
                lang = None

            _LOGGER.info(
                "Retrieved language from database for meta_data object language.",
                extra={
                    "props": {
                        "metadata_language": language,
                        "db_language": (None if lang is None else lang.language_code),
                        "import_id_or_slug": import_id_or_slug,
                    }
                },
            )
            if lang is not None and lang.language_code not in existing_language_codes:
                physical_document_language = PhysicalDocumentLanguage(
                    language_id=lang.id, document_id=physical_document.id
                )
                db.add(physical_document_language)
                db.flush()

    if num_changed == 0:
        _LOGGER.info("update_document complete - nothing changed")
        return physical_document  # Nothing to do - as should be idempotent

    if num_changed > 1:
        # This should never happen due to table uniqueness constraints
        # TODO Rollback
        raise HTTPException(
            detail=(
                f"There was more than one document identified by {import_id_or_slug}. "
                "This should not happen!!!"
            ),
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )

    db.commit()
    db.refresh(physical_document)
    _LOGGER.info(
        "Call to update_document complete",
        extra={
            "props": {
                "superuser_email": current_user.email,
                "num_changed": num_changed,
                "import_id": family_document.import_id,
                "md5_sum": physical_document.md5_sum,
                "content_type": physical_document.content_type,
                "cdn_object": physical_document.cdn_object,
                "languages": [doc.language_code for doc in physical_document.languages],
            }
        },
    )
    return physical_document
