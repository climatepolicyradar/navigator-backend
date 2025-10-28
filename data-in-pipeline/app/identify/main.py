from ..models import IdentifiedSourceDocument, SourceDocument


def identify_source_document(
    source_document: SourceDocument,
) -> IdentifiedSourceDocument:
    return IdentifiedSourceDocument(source=source_document.source, id="1")
