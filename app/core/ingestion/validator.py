from app.core.ingestion.ingest_row import DocumentIngestRow, EventIngestRow
from app.core.ingestion.metadata import build_metadata, Taxonomy
from app.core.ingestion.utils import IngestContext, Result, ResultType


def validate_document_row(
    context: IngestContext,
    row: DocumentIngestRow,
    taxonomy: Taxonomy,
) -> None:
    """
    Validate the constituent elements that represent this law & policy document row.

    :param [IngestContext] context: The ingest context.
    :param [DocumentIngestRow] row: DocumentIngestRow object from the current CSV row.
    :param [Taxonomy] taxonomy: the Taxonomy against which metadata should be validated.
    """
    result, _ = build_metadata(taxonomy, row)
    context.results.append(result)


def validate_event_row(context: IngestContext, row: EventIngestRow) -> None:
    """
    Validate the consituent elements that represent this event row.

    :param [IngestContext] context: The ingest context.
    :param [DocumentIngestRow] row: DocumentIngestRow object from the current CSV row.
    """
    result = Result(ResultType.OK, f"Event: {row.cpr_event_id}, org {context.org_id}")
    context.results.append(result)
