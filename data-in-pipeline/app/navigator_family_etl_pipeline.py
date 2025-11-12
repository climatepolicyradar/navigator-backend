from datetime import datetime

from prefect import flow, task
from prefect.runtime import flow_run, task_run
from returns.result import Failure, Result, Success

from app.extract.connector_config import NavigatorConnectorConfig
from app.extract.connectors import (
    FamilyFetchResult,
    NavigatorConnector,
    NavigatorFamily,
)
from app.extract.enums import CheckPointStorageType
from app.identify.navigator_family import identify_navigator_family
from app.load.aws_bucket import upload_to_s3
from app.logging_config import ensure_logging_active, get_logger
from app.models import Document, ExtractedEnvelope, Identified
from app.transform.models import NoMatchingTransformations
from app.transform.navigator_family import transform_navigator_family

ensure_logging_active()


# ---------------------------------------------------------------------
#  ETL TASKS
# ---------------------------------------------------------------------


@task(log_prints=True)
def extract() -> FamilyFetchResult:
    """Extract family data from the Navigator API.

    This task connects to the Navigator API and retrieves all family records
    using the paginated `/families` endpoint. Each page of data is validated
    and wrapped into an :class:`ExtractedEnvelope` object. The function
    returns both successful results and transient page-level failures
    (e.g., network timeouts), packaged into a :class:`FamilyFetchResult`.

    :return Result[FetchResult, Exception]:
        - **envelopes** – Successful page extractions.
        - **failure** – An error occurred that prevented
          completion of the extraction process.
    """

    task_run_id = (
        task_run.get_id() or f"task-run-extract-families-{datetime.now().isoformat()}"
    )
    flow_run_id = (
        flow_run.get_id()
        or f"flow-run-etl-pipeline-families-{datetime.now().isoformat()}"
    )

    connector_config = NavigatorConnectorConfig(
        source_id="navigator_family",
        checkpoint_storage=CheckPointStorageType.S3,
        checkpoint_key_prefix="navigator/families/",  # TODO : Implement convention for checkpoint keys APP-1409
        logger=get_logger(),
    )

    connector = NavigatorConnector(connector_config)
    result = connector.fetch_all_families(task_run_id, flow_run_id)
    connector.close()

    return result


@task(log_prints=True)
def load_to_s3(document: Document):
    """Upload transformed to S3 cache."""
    upload_to_s3(
        document.model_dump_json(),
        bucket="cpr-cache",
        key=f"pipelines/data-in-pipeline/navigator_family/{document.id}.json",
    )


@task(log_prints=True)
def identify(
    extracted: list[ExtractedEnvelope[list[NavigatorFamily]]],
) -> Identified[NavigatorFamily]:
    """Identify source document type."""
    return identify_navigator_family(extracted)


@task(log_prints=True)
def transform(
    identified: Identified[NavigatorFamily],
) -> Result[list[Document], NoMatchingTransformations]:
    """Transform document to target format."""
    return transform_navigator_family(identified)


# ---------------------------------------------------------------------
#  FLOW ORCHESTRATION
# ---------------------------------------------------------------------


@flow(log_prints=True)
def etl_pipeline() -> list[Document] | Exception:
    """Run the full Navigator ETL pipeline.

    Steps:
        1. Extract families from Navigator API.
        2. Identify their source type.
        3. Transform to target schema.
        4. Load transformed documents to S3 cache.

    Returns:
        Result[Document, Exception] | None
            The final transformation result for demonstration purposes.
            In real use, you may want to return all transformed Results
            or push them to a downstream Prefect block.
    """
    logger = get_logger()
    logger.info("ETL pipeline started")

    extracted_result = extract()

    if extracted_result.failure is not None:
        logger.error(f"Extraction failed: {extracted_result.failure}")
        return Exception(f"Extraction failed at page {extracted_result.failure.page}")

    envelopes = extracted_result.envelopes

    if not envelopes:
        logger.info("No families found to process")
        return []

    identified = identify(envelopes)
    transformed = transform(identified)

    match transformed:
        case Success(documents):
            load_to_s3.map(documents)
            return documents
        case Failure(error):
            # TODO: do not swallow errors
            logger.warning(f"Transformation failed: {error}")
            return error
        case _:
            return Exception("Unexpected transformed result state")
