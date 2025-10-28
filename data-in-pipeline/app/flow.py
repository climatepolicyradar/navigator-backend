from prefect import flow, task

from app.extract.navigator import extract_navigator_document
from app.identify.main import identify_source_document
from app.load.rds import load_rds
from app.models import SourceDocument
from app.transform.main import transform


@flow()
def process_document_updates(ids: list[str] = []):
    result = document_pipeline.map(ids)
    return [result.result().id for result in result]


@task(log_prints=True)
def document_pipeline(id: str):
    navigator_document = extract_navigator_document(id)
    source_document = SourceDocument(source=navigator_document)
    identified_source_document = identify_source_document(source_document)
    document = transform(identified_source_document)
    load_rds(document)
    return document
