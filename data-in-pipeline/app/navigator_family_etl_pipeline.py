from prefect import flow, task
from returns.result import Failure, Result, Success

from app.extract.connector_config import NavigatorConnectorConfig
from app.extract.connectors import NavigatorConnector, NavigatorFamily
from app.extract.enums import CheckPointStorageType
from app.identify.navigator_family import identify_navigator_family
from app.load.aws_bucket import upload_to_s3
from app.models import Document, ExtractedEnvelope, Identified
from app.transform.models import NoMatchingTransformations
from app.transform.navigator_family import transform_navigator_family


@task(log_prints=True)
def extract(family_id: str) -> ExtractedEnvelope[NavigatorFamily]:
    """Extract"""

    connector_config = NavigatorConnectorConfig(
        source_id="navigator_family",
        checkpoint_storage=CheckPointStorageType.S3,
        checkpoint_key_prefix="navigator/families/",  # TODO : Implement convention for checkpoint keys APP-1409
    )

    connector = NavigatorConnector(connector_config)
    envelope = connector.fetch_family(family_id)
    connector.close()

    return envelope


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
) -> Result[Document, NoMatchingTransformations]:
    """Transform document to target format."""
    return transform_navigator_family(identified)


@task(log_prints=True)
def etl_pipeline(id: str):
    """Process a single document through the pipeline."""
    extracted = extract(id)
    identified = identify(extracted)
    transformed = transform(identified)

    match transformed:
        case Success(document):
            load_to_s3(document)
        case Failure(error):
            # TODO: do not swallow errors
            print(error)

    return transformed


@flow
def process_updates(ids: list[str] = []):
    result = etl_pipeline.map(ids)
    return result
