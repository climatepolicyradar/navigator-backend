from returns.result import Success

from app.extract.connectors import NavigatorDocument
from app.models import Document, Identified


def transform_navigator_document(
    input: Identified[NavigatorDocument],
) -> Success[Document]:
    document = Document(id=input.id, title=input.data.title)
    return Success(document)
