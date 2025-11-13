from returns.result import Success

from app.extract.connectors import NavigatorDocument
from app.logging_config import get_logger
from app.models import Document, Identified

_LOGGER = get_logger()


def transform_navigator_document(
    input: Identified[NavigatorDocument],
) -> Success[Document]:
    document = Document(id=input.id, title=input.data.title)
    return Success(document)
