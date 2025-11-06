from prefect import flow, task

from app.connectors import NavigatorConnectorConfig
from app.enums import CheckPointStorageType
from app.extract.navigator_family import NavigatorFamily, extract_navigator_family
from app.identify.navigator_family import identify_navigator_family
from app.load.aws_bucket import upload_to_s3
from app.models import Document, ExtractedEnvelope, Identified
from app.transform.navigator_family import transform_navigator_family


@task(log_prints=True)
def extract(family_id: str):
    """Extract"""

    connector = NavigatorConnectorConfig(
        source_id="navigator_family",
        checkpoint_storage=CheckPointStorageType.S3,
        checkpoint_key_prefix="navigator/families/",
    )
    return extract_navigator_family(family_id, connector)


@task(log_prints=True)
def load_to_s3(document: Document):
    """Upload transformed to S3 cache."""
    upload_to_s3(
        document.model_dump_json(),
        bucket="cpr-production-document-cache",
        key=f"navigator/{document.id}.json",
    )


@task(log_prints=True)
def identify(
    extracted: ExtractedEnvelope[NavigatorFamily],
) -> Identified[NavigatorFamily]:
    """Identify source document type."""
    return identify_navigator_family(extracted)


@task(log_prints=True)
def transform(identified: Identified[NavigatorFamily]) -> Document:
    """Transform document to target format."""
    return transform_navigator_family(identified)


@task(log_prints=True)
def etl_pipeline(id: str):
    """Process a single document through the pipeline."""
    extracted = extract(id)
    identified = identify(extracted)
    document = transform(identified)
    load_to_s3(document)
    return document


@flow
def process_updates(ids: list[str] = []):
    result = etl_pipeline.map(ids)
    return [result.result().id for result in result]
