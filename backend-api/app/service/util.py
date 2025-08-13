import logging
import os
import random
import string
from typing import Any, Optional

from db_client.models import AnyModel
from fastapi import HTTPException, status
from sqlalchemy.orm import Session

from app.clients.aws.client import get_s3_client
from app.config import (
    DOCUMENT_CACHE_BUCKET,
    INGEST_TRIGGER_ROOT,
    PIPELINE_BUCKET,
    PUBLIC_APP_URL,
)

_LOGGER = logging.getLogger(__name__)

CDN_DOMAIN: str = os.getenv("CDN_DOMAIN", "cdn.climatepolicyradar.org")
# TODO: remove & replace with proper content-type handling through pipeline
CONTENT_TYPE_MAP = {
    ".pdf": "application/pdf",
    ".html": "text/html",
    ".htm": "text/html",
    ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
}


def to_cdn_url(s3_object_key: Optional[str]) -> Optional[str]:
    """
    Convert an s3 object key for a PDF in our s3 bucket to a URL to a PDF in our CDN.

    :param [str] s3_url: object key for a PDF in our s3 bucket.
    :returns [str]: URL to the PDF via our CDN domain.
    """
    if s3_object_key is None:
        return None
    return f"https://{CDN_DOMAIN}/navigator/{s3_object_key}"


def random_string(length=12):
    return "".join(
        # trunk-ignore(bandit/B311)
        random.choice(string.ascii_lowercase)
        for i in range(length)
    )


def table_to_json(
    table: AnyModel,
    db: Session,
) -> list[dict]:
    json_out = []

    for row in db.query(table).all():
        row_object = {col.name: getattr(row, col.name) for col in row.__table__.columns}
        json_out.append(row_object)

    return json_out


def tree_table_to_json(
    table: AnyModel,
    db: Session,
) -> list[dict]:
    json_out = []
    child_list_map: dict[int, Any] = {}

    for row in db.query(table).order_by(table.id).all():
        row_object = {col.name: getattr(row, col.name) for col in row.__table__.columns}
        row_children: list[dict[str, Any]] = []
        child_list_map[row_object["id"]] = row_children

        # No parent indicates a top level element
        node_row_object = {"node": row_object, "children": row_children}
        node_id = row_object["parent_id"]
        if node_id is None:
            json_out.append(node_row_object)
        else:
            append_list = child_list_map.get(node_id)
            if append_list is None:
                raise RuntimeError(f"Could not locate parent node with id {node_id}")
            append_list.append(node_row_object)

    return json_out


def get_latest_ingest_start() -> str:
    if (
        PIPELINE_BUCKET is None
        or PUBLIC_APP_URL is None
        or DOCUMENT_CACHE_BUCKET is None
    ):
        if PIPELINE_BUCKET is None:
            _LOGGER.error("{PIPELINE_BUCKET} is not set")
        if PUBLIC_APP_URL is None:
            _LOGGER.error("{PUBLIC_APP_URL} is not set")
        if DOCUMENT_CACHE_BUCKET is None:
            _LOGGER.error("{DOCUMENT_CACHE_BUCKET} is not set")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Missing required environment variables",
        )

    s3_client = get_s3_client()
    latest_ingest_start = s3_client.get_latest_ingest_start(
        PIPELINE_BUCKET, INGEST_TRIGGER_ROOT
    )
    return latest_ingest_start
