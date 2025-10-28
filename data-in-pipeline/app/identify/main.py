from app.models import IdentifiedSourceDocument, SourceDocument


def identify_source_document(
    source_document: SourceDocument,
) -> IdentifiedSourceDocument:
    return IdentifiedSourceDocument(
        source=source_document.source_data, id=source_document.source_data.import_id
    )
