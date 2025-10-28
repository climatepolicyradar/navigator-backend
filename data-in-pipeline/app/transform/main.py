from app.extract.navigator import NavigatorDocument
from app.models import Document, Identified


def transform(input: Identified[NavigatorDocument]) -> Document:
    return Document(id=input.id)
