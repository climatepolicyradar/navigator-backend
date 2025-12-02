from datetime import datetime
from typing import Literal

from prefect import flow, task
from prefect.runtime import flow_run, task_run
from returns.result import Failure, Result, Success

from app.bootstrap_telemetry import get_logger, pipeline_metrics
from app.extract.connector_config import NavigatorConnectorConfig
from app.extract.connectors import (
    FamilyFetchResult,
    NavigatorConnector,
    NavigatorFamily,
)
from app.extract.enums import CheckPointStorageType
from app.identify.navigator_family import identify_navigator_family
from app.load.aws_bucket import upload_to_s3
from app.models import Document, ExtractedEnvelope, Identified
from app.pipeline_metrics import Operation, PipelineType, Status
from app.transform.models import NoMatchingTransformations
from app.transform.navigator_family import transform_navigator_family


def generate_s3_cache_key(step: Literal["extract", "identify", "transform"]) -> str:
    flow_run_id = flow_run.get_id() or "flow-run-etl-pipeline-families"
    return f"pipelines/data-in-pipeline/navigator_family/{step}/{flow_run_id}/result_{datetime.now().isoformat()}.json"


# ---------------------------------------------------------------------
#  ETL TASKS
# ---------------------------------------------------------------------


@task(log_prints=True)
@pipeline_metrics.track(operation=Operation.EXTRACT)
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
    )

    connector = NavigatorConnector(connector_config)
    result = connector.fetch_all_families(task_run_id, flow_run_id)
    connector.close()

    return result


@task(log_prints=True)
@pipeline_metrics.track(operation=Operation.LOAD)
def load_to_s3(document: Document):
    """Upload transformed to S3 cache."""
    upload_to_s3(
        document.model_dump_json(),
        bucket="cpr-cache",
        key=f"pipelines/data-in-pipeline/navigator_family/{document.id}.json",
    )


@task(log_prints=True)
def cache_extraction_result(result: FamilyFetchResult):
    """Cache extraction result to S3 for debugging purposes."""
    upload_to_s3(
        result.model_dump_json(),
        bucket="cpr-cache",
        key=generate_s3_cache_key("extract"),
    )


@task(log_prints=True)
@pipeline_metrics.track(operation=Operation.IDENTIFY)
def identify(
    extracted: list[ExtractedEnvelope[list[NavigatorFamily]]],
) -> Identified[NavigatorFamily]:
    """Identify source document type."""
    return identify_navigator_family(extracted)


@task(log_prints=True)
@pipeline_metrics.track(operation=Operation.TRANSFORM)
def transform(
    identified: Identified[NavigatorFamily],
) -> Result[list[Document], NoMatchingTransformations]:
    """Transform document to target format."""
    return transform_navigator_family(identified)


# ---------------------------------------------------------------------
#  FLOW ORCHESTRATION
# ---------------------------------------------------------------------


@flow(log_prints=True)
@pipeline_metrics.track(
    pipeline_type=PipelineType.FAMILY, scope="batch", flush_on_exit=True
)
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
    _LOGGER = get_logger()
    _LOGGER.info("ETL pipeline started")

    extracted_result = extract()
    cache_extraction_result(extracted_result)

    if extracted_result.failure is not None:
        _LOGGER.error(f"Extraction failed: {extracted_result.failure}")
        pipeline_metrics.record_processed(PipelineType.FAMILY, Status.FAILURE)
        return Exception(f"Extraction failed at page {extracted_result.failure.page}")

    envelopes = extracted_result.envelopes

    family_count = sum(len(env.data) for env in envelopes)
    run_id = flow_run.get_name() or "unknown"
    pipeline_metrics.log_run_info(PipelineType.FAMILY, family_count, run_id)

    if not envelopes:
        _LOGGER.info("No families found to process")
        return []

    identified = identify(envelopes)
    transformed = transform(identified)

    match transformed:
        case Success(documents):
            load_to_s3.map(documents)
            pipeline_metrics.record_processed(PipelineType.FAMILY, Status.SUCCESS)
            return documents
        case Failure(error):
            # TODO: do not swallow errors
            _LOGGER.warning(f"Transformation failed: {error}")
            pipeline_metrics.record_processed(PipelineType.FAMILY, Status.FAILURE)
            return error
        case _:
            pipeline_metrics.record_processed(PipelineType.FAMILY, Status.FAILURE)
            return Exception("Unexpected transformed result state")
