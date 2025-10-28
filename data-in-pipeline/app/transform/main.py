from ..models import Document, IdentifiedSourceDocument


def transform(input: IdentifiedSourceDocument) -> Document:
    return Document(id=input.id)
