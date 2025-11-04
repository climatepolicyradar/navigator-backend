from prefect import flow, task

from app.extract.navigator_document import extract_navigator_document
from app.identify.main import identify_source_document
from app.load.aws_bucket import upload_to_s3
from app.load.rds import load_rds
from app.transform.main import transform


@task(log_prints=True)
def extract_document(document_id: str):
    """Extract document from Navigator."""
    return extract_navigator_document(document_id)


@task(log_prints=True)
def load_document_to_s3(navigator_document):
    """Upload transformed document to S3 cache."""
    upload_to_s3(
        navigator_document.model_dump_json(),
        bucket="cpr-production-document-cache",
        key=f"navigator/{navigator_document.id}.json",
    )


@task(log_prints=True)
def identify_document(navigator_document):
    """Identify source document type."""
    return identify_source_document(navigator_document)


@task(log_prints=True)
def transform_document(identified_source_document):
    """Transform document to target format."""
    return transform(identified_source_document)


@task(log_prints=True)
def load_document_to_rds(document):
    """Load document to RDS."""
    load_rds(document)
    return document


@task(log_prints=True)
def document_pipeline(id: str):
    """Process a single document through the pipeline."""
    navigator_document = extract_document(id)
    identified_source_document = identify_document(navigator_document)
    document = transform_document(identified_source_document)
    load_document_to_s3(document)
    return document


@flow(name="process_document_updates")
def process_document_updates(ids: list[str] = []):
    result = document_pipeline.map(ids)
    return [result.result().id for result in result]
