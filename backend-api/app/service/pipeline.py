import enum
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from typing import Any, Collection, Mapping, Optional, Sequence, Union

from fastapi import Depends

from app.clients.aws.client import S3Client
from app.clients.aws.s3_document import S3Document
from app.clients.db.session import get_db
from app.config import INGEST_TRIGGER_ROOT, PIPELINE_BUCKET
from app.models.document import DocumentParserInput
from app.repository.pipeline import generate_pipeline_ingest_input

_LOGGER = logging.getLogger(__name__)

MetadataType = dict[str, list[str]]

_ID_ELEMENT = r"[a-zA-Z0-9]+([-_]?[a-zA-Z0-9]+)*"
IMPORT_ID_MATCHER = re.compile(
    rf"^{_ID_ELEMENT}\.{_ID_ELEMENT}\.{_ID_ELEMENT}\.{_ID_ELEMENT}$"
)


class ResultType(str, enum.Enum):
    """Result type used when processing metadata values."""

    OK = "Ok"
    RESOLVED = "Resolved"
    ERROR = "Error"


@dataclass
class Result:
    """Augmented result class for reporting extra details about processed metadata."""

    type: ResultType = ResultType.OK
    details: str = ""


def format_pipeline_ingest_input(
    documents: Sequence[DocumentParserInput],
) -> dict[str, Any]:
    """Format the DocumentParserInput objects for the db_state.json file.

    :param Sequence[DocumentParserInput] documents: A list of
        DocumentParserInput objects that can be used by the pipeline.
    :return dict[str, Any]: The contents of the db_state.json file in
        JSON form.
    """
    return {"documents": {d.import_id: d.to_json() for d in documents}}


def get_db_state_content(db=Depends(get_db)):
    """Get the db_state.json content in JSON form.

    :param Session db: The db session to query against.
    :return: A list of DocumentParserInput objects in the JSON format
        that will be written to the db_state.json file used by the
        pipeline.
    """
    pipeline_ingest_input = generate_pipeline_ingest_input(db)
    return format_pipeline_ingest_input(pipeline_ingest_input)


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


def write_documents_to_s3(
    s3_client: S3Client, s3_prefix: str, content: dict[str, Any]
) -> Union[S3Document, bool]:
    """
    Write current state of documents into S3 to trigger a pipeline run after bulk ingest

    :param [S3Client] s3_client: an S3 client to use to write data
    :param [str] s3_prefix: prefix into which to write the document updates in s3
    :param [dict[str, Any]] content: db state document content
    :param [Sequence[DocumentCreateRequest]] documents: a sequence of document
        specifications to write to S3
    """
    json_content = json.dumps(content, indent=2)
    bytes_content = BytesIO(json_content.encode("utf8"))
    documents_object_key = f"{s3_prefix}/db_state.json"
    _LOGGER.info("Writing Documents file into S3")
    return _write_content_to_s3(
        s3_client=s3_client,
        s3_object_key=documents_object_key,
        bytes_content=bytes_content,
    )


def write_ingest_results_to_s3(
    s3_client: S3Client,
    s3_prefix: str,
    results: Sequence[Result],
) -> Union[S3Document, bool]:
    """
    Write document specifications successfully created during a bulk import to S3

    :param [S3Client] s3_client: an S3 client to use to write data
    :param [str] s3_prefix: prefix into which to write the document updates in s3
    :param [Sequence[DocumentCreateRequest]] documents: a sequence of document
        specifications to write to S3
    """
    json_content = json.dumps(
        [
            {
                "type": r.type,
                "details": r.details,
            }
            for r in results
        ],
        indent=2,
    )
    bytes_content = BytesIO(json_content.encode("utf8"))
    documents_object_key = f"{s3_prefix}/ingest_results.txt"
    _LOGGER.info("Writing Ingest Results file into S3")
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
    if not PIPELINE_BUCKET:
        raise Exception("PIPELINE_BUCKET not set")

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
