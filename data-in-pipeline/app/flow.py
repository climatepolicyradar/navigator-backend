from prefect import flow, task

from .extract.navigator import extract_navigator_document
from .identify.main import identify_source_document
from .load.rds import load_rds
from .models import SourceDocument
from .transform.main import transform


@flow()
def process_document_updates(ids: list[str] = []):
    document_pipline.map(ids)


@task(log_prints=True)
def document_pipline(id: str):
    navigator_document = extract_navigator_document(id)
    source_document = SourceDocument(source=navigator_document)
    identified_source_document = identify_source_document(source_document)
    document = transform(identified_source_document)
    load_rds(document)
