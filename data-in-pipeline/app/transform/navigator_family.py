from app.extract.navigator_family import NavigatorFamily
from app.models import Document, Identified


def transform_navigator_family(input: Identified[NavigatorFamily]) -> Document:
    return Document(id=input.id)
