import logging
from datetime import datetime

from prefect import flow, task
from prefect.runtime import flow_run, task_run
from returns.pipeline import is_successful
from returns.result import Failure, Result

from app.extract.connector_config import NavigatorConnectorConfig
from app.extract.connectors import NavigatorConnector, NavigatorDocument
from app.extract.enums import CheckPointStorageType
from app.identify.navigator_document import identify_navigator_document
from app.load.aws_bucket import upload_to_s3
from app.logging_config import ensure_logging_active
from app.models import Document, ExtractedEnvelope, Identified
from app.transform.navigator_document import transform_navigator_document

_LOGGER = logging.getLogger(__name__)

ensure_logging_active()


@task(log_prints=True)
def extract(
    document_id: str,
) -> Result[ExtractedEnvelope[NavigatorDocument], Exception]:
    """Extract"""
    connector_config = NavigatorConnectorConfig(
        source_id="navigator_document",
        checkpoint_storage=CheckPointStorageType.S3,
        checkpoint_key_prefix="navigator/documents/",  # TODO : Implement convention for checkpoint keys APP-1409
    )

    task_run_id = (
        task_run.get_id()
        or f"task-run-extract-{document_id}-{datetime.now().isoformat()}"
    )
    flow_run_id = (
        flow_run.get_id()
        or f"flow-run-etl-pipeline-{document_id}-{datetime.now().isoformat()}"
    )

    connector = NavigatorConnector(connector_config)
    envelope = connector.fetch_document(document_id, task_run_id, flow_run_id)
    connector.close()
    return envelope


@task(log_prints=True)
def load_to_s3(document: Document):
    """Upload to S3 cache"""
    upload_to_s3(
        document.model_dump_json(),
        bucket="cpr-cache",
        key=f"pipelines/data-in-pipeline/navigator_document/{document.id}.json",
    )


@task(log_prints=True)
def identify(extracted: ExtractedEnvelope[NavigatorDocument]):
    """Identify"""
    return identify_navigator_document(extracted)


@task(log_prints=True)
def transform(identified: Identified[NavigatorDocument]):
    """Transform"""
    return transform_navigator_document(identified)


@task(log_prints=True)
def etl_pipeline(
    id: str,
) -> Result[Document, Exception]:
    """ETL pipeline"""
    extracted_result = extract(id)
    if not is_successful(extracted_result):
        _LOGGER.exception(f"Extraction failed for {id}: {extracted_result.failure()}")
        return Failure(extracted_result.failure())
    extracted = extracted_result.unwrap()
    identified = identify(extracted)
    document = transform(identified)
    load_to_s3(document.unwrap())
    return document


@flow
def process_updates(ids: list[str] = []):
    results = etl_pipeline.map(ids)
    documents = []
    for r in results:
        result = r.result()
        if is_successful(result):
            documents.append(result.unwrap())

    return documents
