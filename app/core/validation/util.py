import json
import logging
from datetime import datetime
from io import BytesIO
from typing import Any, Collection, Mapping, Optional, Sequence, Union
from app.core.aws import S3Document

from sqlalchemy.orm import Session

from app.api.api_v1.schemas.document import DocumentParserInput
from app.core.aws import S3Client
from app.core.lookups import get_metadata
from app.core.validation import PIPELINE_BUCKET

_LOGGER = logging.getLogger(__file__)

INGEST_TRIGGER_ROOT = "input"


def _flatten_maybe_tree(
    maybe_tree: Sequence[Mapping[str, Any]],
    values: Optional[list[str]] = None,
) -> Collection[str]:
    def is_tree_node(maybe_node: Mapping[str, Any]) -> bool:
        return set(maybe_node.keys()) == {"node", "children"}

    def get_value(data_node: Mapping[str, str]) -> str:
        value = data_node.get("name") or data_node.get("value")
        if value is None:
            raise Exception(f"No value found in '{data_node}'")
        return value

    values = values or []

    for maybe_node in maybe_tree:
        if is_tree_node(maybe_node):
            values.append(get_value(maybe_node["node"]))
            _flatten_maybe_tree(maybe_node["children"], values=values)
        else:
            values.append(get_value(maybe_node))

    return values


def get_valid_metadata(
    db: Session,
) -> Mapping[str, Mapping[str, Collection[str]]]:
    """
    Make a request to the backend to collect valid metadata values

    :param [requests.Session] session: The session used for making the request.
    :return [Mapping[str, Sequence[str]]]: _description_
    """
    _LOGGER.info("Retrieving valid metadata values from database")

    raw_metadata = get_metadata(db)["metadata"]

    return {
        source: {
            meta: _flatten_maybe_tree(raw_metadata[source][meta])
            for meta in raw_metadata[source]
        }
        for source in raw_metadata
    }


def write_documents_to_s3(
    s3_client: S3Client,
    s3_prefix: str,
    documents: Sequence[DocumentParserInput],
) -> Union[S3Document, bool]:
    """
    Write document specifications successfully created during a bulk import to S3

    :param [S3Client] s3_client: an S3 client to use to write data
    :param [str] s3_prefix: prefix into which to write the document updates in s3
    :param [Sequence[DocumentCreateRequest]] documents: a sequence of document
        specifications to write to S3
    """
    json_content = json.dumps([d.to_json() for d in documents], indent=2)
    bytes_content = BytesIO(json_content.encode("utf8"))
    documents_object_key = f"{s3_prefix}/documents.json"
    _LOGGER.info("Writing Documents file into S3")
    return _write_content_to_s3(
        s3_client=s3_client,
        s3_object_key=documents_object_key,
        bytes_content=bytes_content,
    )


def get_new_s3_prefix() -> str:
    """
    Get a name prefix to use for storing bulk import files in s3.

    :return [str]: the prefix to use for s3 objects triggered by a call to an endpoint
    """
    current_datetime = datetime.now().isoformat().replace(":", ".")
    return f"{INGEST_TRIGGER_ROOT}/{current_datetime}"


def write_csv_to_s3(
    s3_client: S3Client, s3_prefix: str, s3_content_label: str, file_contents: str
) -> Union[S3Document, bool]:
    """
    Write the csv into S3

    :param [S3Client] s3_client: a valid S3 client
    :param [str] file_contents: the contents of the file as a string
    :return [S3Document | False]: document object if successful, otherwise False
    """
    bytes_content = BytesIO(file_contents.encode("utf8"))
    csv_object_key = f"{s3_prefix}/bulk-import-{s3_content_label}.csv"
    _LOGGER.info("Writing CSV file into S3")
    return _write_content_to_s3(
        s3_client=s3_client,
        s3_object_key=csv_object_key,
        bytes_content=bytes_content,
    )


def _write_content_to_s3(
    s3_client: S3Client,
    s3_object_key: str,
    bytes_content: BytesIO,
) -> Union[S3Document, bool]:
    """
    Write document specifications successfully created during a bulk import to S3

    :param [S3Client] s3_client: an S3 client to use to write data
    :param [str] s3_object_key: path into which to write the document updates in s3
    :param [BytesIO] content: bytes to write into the given object path
    """
    _LOGGER.info(
        "Writing content into S3",
        extra={
            "props": {
                "bucket": PIPELINE_BUCKET,
                "file": s3_object_key,
            }
        },
    )

    return s3_client.upload_fileobj(
        bucket=PIPELINE_BUCKET,
        key=s3_object_key,
        content_type="application/json",
        fileobj=bytes_content,
    )
