import logging
from typing import Sequence

from sqlalchemy.orm import Session

from app.api.api_v1.schemas.document import DocumentParserInput
from app.repository.pipeline import generate_pipeline_ingest_input

_LOGGER = logging.getLogger(__name__)

MetadataType = dict[str, list[str]]


def format_pipeline_ingest_input(documents: Sequence[DocumentParserInput]):
    """Format the DocumentParserInput objects for the db_state.json file.

    :param Sequence[DocumentParserInput] documents: A list of
        DocumentParserInput objects that can be used by the pipeline.
    :return: The contents of the db_state.json file in JSON form.
    """
    return {"documents": {d.import_id: d.to_json() for d in documents}}


def get_db_state_content(db: Session):
    """Get the db_state.json content in JSON form.

    :param Session db: The db session to query against.
    :return: A list of DocumentParserInput objects in the JSON format
        that will be written to the db_state.json file used by the
        pipeline.
    """
    pipeline_ingest_input = generate_pipeline_ingest_input(db)
    return format_pipeline_ingest_input(pipeline_ingest_input)
