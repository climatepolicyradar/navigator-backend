from app.extract.navigator import NavigatorDocument
from app.models import IdentifiedSourceDocument, SourceDocument


def identify_source_document(
    source_document: SourceDocument[NavigatorDocument],
) -> IdentifiedSourceDocument:
    return IdentifiedSourceDocument(
        source=source_document.data, id=source_document.data.import_id
    )
