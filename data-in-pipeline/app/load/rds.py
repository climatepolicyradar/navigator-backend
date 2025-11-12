from app.logging_config import ensure_logging_active, get_logger
from app.models import Document

_LOGGER = get_logger()
ensure_logging_active()


def load_rds(document: Document) -> Document:
    return document
