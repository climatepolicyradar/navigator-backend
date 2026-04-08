import asyncio
import io
import itertools
import json
from datetime import datetime
from typing import Any, Literal, cast

import pyarrow as pa
import pyarrow.parquet as pq
from data_in_models.models import Document
from prefect import flow, task
from prefect.futures import PrefectFuture
from prefect.runtime import flow_run, task_run
from prefect.task_runners import TaskRunner, ThreadPoolTaskRunner
from returns.result import Failure, Success

from app.bootstrap_telemetry import get_logger, pipeline_metrics
from app.extract.connector_config import NavigatorConnectorConfig
from app.extract.connectors import (
    FamilyFetchResult,
    NavigatorConnector,
    NavigatorFamily,
)
from app.extract.enums import CheckPointStorageType
from app.identify.navigator_family import identify_navigator_families
from app.load.aws_bucket import upload_to_s3
from app.load.load import load_to_db
from app.models import ExtractedEnvelope, Identified, PipelineResult
from app.pipeline_metrics import Operation, PipelineType, Status
from app.run_db_migrations.run_db_migrations import run_db_migrations
from app.transform.navigator_family import (
    transform_navigator_family,
)
from app.util import SlackNotify, get_s3_client


def generate_s3_cache_key(step: Literal["extract", "identify", "transform"]) -> str:
    flow_run_id = flow_run.get_id() or "flow-run-etl-pipeline-families"
    return f"pipelines/data-in-pipeline/navigator_family/{step}/{flow_run_id}/result_{datetime.now().isoformat()}.json"


# ---------------------------------------------------------------------
#  ETL TASKS
# ---------------------------------------------------------------------


@task(log_prints=True)
def run_db_migrations_task():
    """Run migrations against the load-api database."""
    _LOGGER = get_logger()
    _LOGGER.info("Running migrations against the load-api database")
    run_db_migrations()


@task(log_prints=True)
@pipeline_metrics.track(operation=Operation.EXTRACT)
def extract(ids: list[str] | None = None) -> FamilyFetchResult:
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

    if ids is None:
        result = connector.fetch_all_families(task_run_id, flow_run_id)
    else:
        result = connector.fetch_families(ids, task_run_id, flow_run_id)
    connector.close()

    return result


@task(log_prints=True)
@pipeline_metrics.track(operation=Operation.LOAD)
def cache_jsonl_to_s3(documents: list[Document], run_id: str | None = None):
    """Upload documents as jsonl to S3 cache."""
    client = get_s3_client()

    buffer = io.BytesIO()
    for chunk in itertools.batched(documents, 10_000):
        buffer.write(
            "\n".join(json.dumps(doc.model_dump(mode="json")) for doc in chunk).encode()
        )
        buffer.write(b"\n")

    value = buffer.getvalue()
    client.put_object(
        Bucket="cpr-cache",
        Key=f"pipelines/data-in-pipeline/navigator_family/{run_id}-transformed-result.jsonl",
        Body=value,
        ContentType="application/x-ndjson",
    )
    client.put_object(
        Bucket="cpr-cache",
        Key="pipelines/data-in-pipeline/navigator_family/documents-latest.jsonl",
        Body=value,
        ContentType="application/x-ndjson",
    )


@task(log_prints=True)
@pipeline_metrics.track(operation=Operation.LOAD)
def cache_parquet_to_s3(documents: list[Document], run_id: str | None = None):
    """Upload documents as parquet to S3 cache."""
    client = get_s3_client()

    buffer = io.BytesIO()
    writer = None
    for chunk in itertools.batched(documents, 10_000):
        rows = []
        for doc in chunk:
            # we serialise the attributes to a json string and transform an empty dict => None
            # as parquet breaks when auto-deriving a field with `{}` as its value.
            doc_data = doc.model_dump(mode="json")
            if "attributes" in doc_data and doc_data["attributes"]:
                doc_data["attributes"] = json.dumps(doc_data["attributes"])
            else:
                doc_data["attributes"] = None
            rows.append(doc_data)
        table = pa.Table.from_pylist(rows)
        if writer is None:
            writer = pq.ParquetWriter(buffer, table.schema)
        writer.write_table(table)
    if writer:
        writer.close()

    value = buffer.getvalue()
    client.put_object(
        Bucket="cpr-cache",
        Key=f"pipelines/data-in-pipeline/navigator_family/{run_id}-transformed-result.parquet",
        Body=value,
        ContentType="application/octet-stream",
    )
    client.put_object(
        Bucket="cpr-cache",
        Key="pipelines/data-in-pipeline/navigator_family/documents-latest.parquet",
        Body=value,
        ContentType="application/octet-stream",
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
def cache_transform_errors(errors: list[Exception], run_id: str):
    """Cache transformation errors to S3."""
    error_data = [str(e) for e in errors]
    upload_to_s3(
        json.dumps(error_data),
        bucket="cpr-cache",
        key=f"pipelines/data-in-pipeline/transformation_errors/{run_id}-transform-errors.json",
    )


@task(log_prints=True)
@pipeline_metrics.track(operation=Operation.IDENTIFY)
def identify(
    extracted: list[ExtractedEnvelope[list[NavigatorFamily]]],
) -> list[Identified[NavigatorFamily]]:
    """Identify source document type."""
    return identify_navigator_families(extracted)


@task(log_prints=True)
@pipeline_metrics.track(operation=Operation.TRANSFORM)
def transform(
    identified_families: list[Identified[NavigatorFamily]],
) -> tuple[list[Document], list[Exception]]:
    """Transform all families to target format, collecting successes and failures."""
    _LOGGER = get_logger()

    all_documents = []
    errors = []
    for family in identified_families:
        transformed = transform_navigator_family(family)

        match transformed:
            case Success(documents):
                all_documents.extend(documents)
            case Failure(error):
                _LOGGER.warning(f"Transformation failed: {error}")
                errors.append(error)
            case _:
                errors.append(Exception("Unexpected transformed result state"))

    _LOGGER.info(
        f"Transformation complete: {len(all_documents)} documents from "
        f"{len(identified_families)} families ({len(errors)} failures)"
    )

    return all_documents, errors


@task(
    log_prints=True,
    retries=2,
    retry_delay_seconds=[5, 10],
    tags=["load-api-write-concurrency-limit"],
)
@pipeline_metrics.track(operation=Operation.LOAD)
def load_batch(
    transformed: list[Document],
) -> str | Exception:
    """Load a batch of documents to the database.

    This task includes automatic retries for transient failures.

    :param documents: Batch of documents to load
    :return: List of document IDs or Exception if the batch fails
    """
    return load_to_db(transformed)


@task(log_prints=True)
def create_batches(
    documents: list[Document], batch_size: int = 500
) -> list[list[Document]]:
    """Split documents into batches for parallel loading.

    :param documents: All documents to be batched
    :param batch_size: Number of documents per batch
    :return: List of document batches
    """
    _LOGGER = get_logger()
    batches = [
        documents[i : i + batch_size] for i in range(0, len(documents), batch_size)
    ]
    _LOGGER.info(
        f"Created {len(batches)} batches of size {batch_size} from {len(documents)} documents"
    )
    return batches


@task(log_prints=True)
def upload_report(
    document_batches: list[list[Document]],
    load_results: list[str | Exception],
    run_id: str,
) -> None:
    """Upload report of what was processed in this run."""
    _LOGGER = get_logger()

    report = {
        "run_id": run_id,
        "total_batches": len(document_batches),
        "total_documents": sum(len(batch) for batch in document_batches),
        "successful_batches": sum(
            1 for r in load_results if not isinstance(r, Exception)
        ),
        "failed_batches": sum(1 for r in load_results if isinstance(r, Exception)),
    }

    upload_to_s3(
        json.dumps(report),
        bucket="cpr-cache",
        key=f"pipelines/data-in-pipeline/navigator_family/{run_id}-load-report.json",
    )

    _LOGGER.info(f"Uploaded load report for {report['total_documents']} documents")


@task(log_prints=True)
def count_batch_successes_and_failures(
    batched_results: list[str | Exception],
) -> tuple[int, int]:
    """Count the number of successful and failed batches.

    :param batched_results: Results from batch load operations
    :return: Tuple of (number of batches that succeeded, number of
        batches that had failures)
    """
    _LOGGER = get_logger()

    errors = [result for result in batched_results if isinstance(result, Exception)]

    if errors:
        _LOGGER.error(
            f"Load failed for {len(errors)} out of {len(batched_results)} batches"
        )
        for error in errors:
            _LOGGER.error(f"Batch error: {error}")

    _LOGGER.info(f"{len(batched_results) - len(errors)} batches loaded successfully")
    return len(batched_results) - len(errors), len(errors)


# ---------------------------------------------------------------------
#  FLOW ORCHESTRATION
# ---------------------------------------------------------------------


# NOTE: Pyright flags ThreadPoolTaskRunner here due to invariant generic
# mismatch in Prefect's type hints, even though it is runtime-compatible.
# We cast explicitly to document intent and avoid a broad type ignore.
task_runner = cast(
    TaskRunner[PrefectFuture[Any]],
    ThreadPoolTaskRunner(max_workers=1),
)


@flow(log_prints=True, task_runner=task_runner, on_failure=[SlackNotify.message])
@pipeline_metrics.track(
    pipeline_type=PipelineType.FAMILY, scope="batch", flush_on_exit=True
)
def data_in_pipeline(
    ids: list[str] | None = None,
    batch_size: int = 500,
    max_concurrent_batches: int = 3,
) -> PipelineResult | Exception:
    """Run the full Navigator ETL pipeline.

    If IDs are provided, processes only those specific families.
    If no IDs are provided, processes all families from the API.

    Steps:
        - Extract families from Navigator API (all or by ID).
        - Identify their source type.
        - Transform to target schema.
        - Load transformed documents to S3 cache.
        - Save transformed documents to the load DB in batches.
        - Upload report with results.
    """
    _LOGGER = get_logger()
    _LOGGER.info("ETL pipeline started")

    # Set flow_run_name early so all metrics (including extract) have it
    run_id = flow_run.get_name() or "unknown"
    pipeline_metrics.set_flow_run_name(run_id)

    run_db_migrations_task()

    # If IDs provided, process only those families
    if ids is not None:
        _LOGGER.info(f"Processing {len(ids)} specific families")
    else:
        # Otherwise, process all families (existing batch logic)
        _LOGGER.info("Processing all families")

    # -------------------------
    # EXTRACT
    # -------------------------
    extracted_result = extract(ids)
    envelopes = extracted_result.envelopes
    cache_extraction_result(extracted_result)

    family_count = sum(len(env.data) for env in envelopes)

    # This provides a searchable log string and key data, it should be before any return statements
    pipeline_metrics.log_run_info(PipelineType.FAMILY, family_count, run_id)

    if not envelopes:
        _LOGGER.info("No families found to process")
        return Exception("No families found to process")

    if extracted_result.failure:
        _LOGGER.error(f"Extraction failed: {extracted_result.failure}")
        pipeline_metrics.record_processed(
            PipelineType.FAMILY,
            Operation.EXTRACT,
            Status.FAILURE,
            len(envelopes),
        )
        return Exception(f"Extraction failed at page {extracted_result.failure.page}")

    pipeline_metrics.record_processed(
        PipelineType.FAMILY,
        Operation.EXTRACT,
        Status.SUCCESS,
        len(envelopes),
    )

    # -------------------------
    # IDENTIFY
    # -------------------------

    identified_families = identify(envelopes)

    # -------------------------
    # TRANSFORM
    # -------------------------
    transformed_documents, errors = transform(identified_families)

    if errors:
        cache_transform_errors(errors, run_id)
        asyncio.run(
            SlackNotify.send_custom_message(
                message=(
                    f"⚠️ *Partial transform failure* in `{run_id}`\n"
                    f"*Failed:* {len(errors)} / {family_count} families\n"
                    f"*Succeeded:* {len(transformed_documents)}\n"
                    f"Failed IDs cached to S3 → `s3://cpr-cache/pipelines/data-in-pipeline/transformation_errors/{run_id}-transform-errors.json`"
                )
            )
        )
        pipeline_metrics.record_processed(
            PipelineType.FAMILY,
            Operation.TRANSFORM,
            Status.FAILURE,
            len(errors),
        )
        _LOGGER.exception(f"Transformation errors: {len(errors)}")

    if len(transformed_documents) == 0:
        _LOGGER.error("No documents were transformed successfully; aborting load")
        return Exception("No documents transformed successfully")

    pipeline_metrics.record_processed(
        PipelineType.FAMILY,  # refactoring: this part of the abstraction is wrong, and we will remove so metrics always count documents only
        Operation.TRANSFORM,  # We will move this to structured logging for stage-by-stage observability, and count document
        Status.SUCCESS,
        # FIXME
        len(transformed_families) - len(errors),
    )

    # -------------------------
    # CACHE TO S3
    # -------------------------
    jsonl_future = cache_jsonl_to_s3.submit(transformed_documents, run_id)
    # parquet_future = cache_parquet_to_s3.submit(transformed_documents, run_id)

    jsonl_future.result()
    # parquet_future.result()

    # -------------------------
    # BATCH AND LOAD TO DB
    # -------------------------
    _LOGGER.info(
        f"Starting batched load: {len(transformed_documents)} documents, "
        f"batch_size={batch_size}, max_concurrent={max_concurrent_batches}"
    )

    document_batches = create_batches(transformed_documents, batch_size)
    load_results = load_batch.map(document_batches)

    # Prefect resolves mapped task results before invoking tasks.
    # Pyright sees PrefectFutureList here, but runtime value is list[str | Exception].
    successful_batches, failed_batches = count_batch_successes_and_failures(load_results)  # type: ignore[reportArgumentType]

    pipeline_metrics.record_processed(
        PipelineType.FAMILY,
        Operation.LOAD,
        Status.SUCCESS,
        successful_batches * batch_size,
    )

    if failed_batches > 0:
        pipeline_metrics.record_processed(
            PipelineType.FAMILY,
            Operation.LOAD,
            Status.FAILURE,
            failed_batches * batch_size,
        )
        return Exception(f"{failed_batches} batches failed to load")

    if successful_batches == len(transformed_documents) / batch_size:
        _LOGGER.info("All batches loaded successfully")

    _LOGGER.info("ETL pipeline completed successfully")

    # Prefect resolves mapped task results before invoking tasks.
    # Pyright sees PrefectFutureList here, but runtime value is list[str | Exception].
    upload_report(document_batches, load_results, run_id)  # type: ignore[reportArgumentType]

    return PipelineResult(
        documents_processed=len(transformed_documents),
        batches_loaded=len(document_batches),
        status="success",
    )
