from app.logging_config import ensure_logging_active
from app.models import Document

ensure_logging_active()


def load_rds(document: Document) -> Document:
    return document
