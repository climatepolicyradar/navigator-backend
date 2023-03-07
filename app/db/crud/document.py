import logging
from sqlalchemy.orm import Session
from app.api.api_v1.schemas.document import DocumentWithFamilyResponse

_LOGGER = logging.getLogger(__file__)


def get_document_and_family(
    db: Session, import_id_or_slug: str
) -> DocumentWithFamilyResponse:
    """
    Get a document along with the family information.

    :param Session db: connection to db
    :param str import_id_or_slug: id of document
    :return DocumentWithFamilyResponse: response object
    """
    return DocumentWithFamilyResponse(requested_id=import_id_or_slug)
