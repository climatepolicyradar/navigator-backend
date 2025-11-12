import logging
from datetime import datetime

from prefect import flow, task
from prefect.runtime import flow_run, task_run
from returns.result import Failure, Result, Success

from app.extract.connector_config import NavigatorConnectorConfig
from app.extract.connectors import FetchResult, NavigatorConnector, NavigatorFamily
from app.extract.enums import CheckPointStorageType
from app.identify.navigator_family import identify_navigator_family
from app.load.aws_bucket import upload_to_s3
from app.models import Document, ExtractedEnvelope, Identified
from app.transform.models import NoMatchingTransformations
from app.transform.navigator_family import transform_navigator_family

_LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------
#  ETL TASKS
# ---------------------------------------------------------------------


@task(log_prints=True)
def extract() -> Result[FetchResult, Exception]:
    """Extract family data from the Navigator API.

    This task connects to the Navigator API and retrieves all family records
    using the paginated `/families` endpoint. Each page of data is validated
    and wrapped into an :class:`ExtractedEnvelope` object. The function
    returns both successful results and transient page-level failures
    (e.g., network timeouts), packaged into a :class:`FetchResult`.

    :return Result[FetchResult, Exception]:
        - **Success(FetchResult)** – Extraction succeeded (may include transient failures).
        - **Failure(Exception)** – A fatal error occurred that prevented
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
    extracted: ExtractedEnvelope[NavigatorFamily],
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
def etl_pipeline() -> Document | Exception:
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

    extracted_result = extract()

    envelopes: list[ExtractedEnvelope[NavigatorFamily]] = []

    match extracted_result:
        case Failure(error):
            _LOGGER.error(f"Extraction failed: {error}")
            return error
        case Success(fetch_result):
            if fetch_result.failures:
                _LOGGER.warning(
                    f"Some pages failed to extract: {fetch_result.failures}"
                )

            envelopes = fetch_result.envelopes

    identified = identify(envelopes)
    transformed = transform(identified).result()

    match transformed:
        case Success(documents):
            load_to_s3.map(documents)
            return documents
        case Failure(error):
            # TODO: do not swallow errors
            _LOGGER.warning(f"Transformation failed: {error}")
            return error
        case _:
            return Exception("Unexpected transformed result state")
