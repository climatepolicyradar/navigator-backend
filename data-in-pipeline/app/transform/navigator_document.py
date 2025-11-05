from app.extract.navigator_document import NavigatorDocument
from app.models import Document, Identified


def transform_navigator_document(input: Identified[NavigatorDocument]) -> Document:
    return Document(id=input.id)
