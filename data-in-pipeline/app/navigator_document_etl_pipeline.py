from prefect import flow, task

from app.connector_config import NavigatorConnectorConfig
from app.connectors import NavigatorConnector
from app.enums import CheckPointStorageType
from app.extract.navigator_document import NavigatorDocument
from app.identify.navigator_document import identify_navigator_document
from app.load.aws_bucket import upload_to_s3
from app.models import Document, ExtractedEnvelope, Identified
from app.transform.navigator_document import transform_navigator_document


@task(log_prints=True)
def extract(document_id: str) -> ExtractedEnvelope[NavigatorDocument]:
    """Extract"""

    connector_config = NavigatorConnectorConfig(
        source_id="navigator_document",
        checkpoint_storage=CheckPointStorageType.S3,
        checkpoint_key_prefix="navigator/documents/",  # TODO : Implement convention for checkpoint keys APP-1409
    )

    connector = NavigatorConnector(connector_config)
    envelope = connector.fetch_document(document_id)
    connector.close()
    return envelope


@task(log_prints=True)
def load_to_s3(document: Document):
    """Upload to S3 cache"""
    upload_to_s3(
        document.model_dump_json(),
        bucket="cpr-production-document-cache",
        key=f"navigator/{document.id}.json",
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
def etl_pipeline(id: str):
    """ETL pipeline"""
    extracted = extract(id)
    identified = identify(extracted)
    document = transform(identified)
    load_to_s3(document)
    return document


@flow
def process_updates(ids: list[str] = []):
    result = etl_pipeline.map(ids)
    return [result.result().id for result in result]
