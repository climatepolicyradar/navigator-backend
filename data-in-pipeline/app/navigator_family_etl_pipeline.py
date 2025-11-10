import logging

from prefect import flow, task
from returns.pipeline import is_successful
from returns.result import Failure, Result, Success

from app.extract.connector_config import NavigatorConnectorConfig
from app.extract.connectors import NavigatorConnector, NavigatorFamily
from app.extract.enums import CheckPointStorageType
from app.identify.navigator_family import identify_navigator_family
from app.load.aws_bucket import upload_to_s3
from app.models import Document, ExtractedEnvelope, Identified
from app.transform.models import NoMatchingTransformations
from app.transform.navigator_family import transform_navigator_family

logger = logging.getLogger(__name__)


@task(log_prints=True)
def extract(family_id: str) -> Result[ExtractedEnvelope[NavigatorFamily], Exception]:
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
def transform(
    identified: Identified[NavigatorFamily],
) -> Result[Document, NoMatchingTransformations]:
    """Transform document to target format."""
    return transform_navigator_family(identified)


@task(log_prints=True)
def etl_pipeline(
    id: str,
) -> Result[Document, Exception]:
    """Process a single document through the pipeline."""

    extracted_result = extract(id)
    if not is_successful(extracted_result):
        logger.exception(f"Extraction failed for {id}: {extracted_result.failure()}")
        return Failure(extracted_result.failure())
    extracted = extracted_result.unwrap()
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
