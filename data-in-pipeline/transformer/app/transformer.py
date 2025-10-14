from pydantic import BaseModel


class NavigatorFamily(BaseModel):
    title: str
    summary: str
    geographies: list[str]


class NavigatorDocument(BaseModel):
    id: int
    title: str
    family: NavigatorFamily


class Label(BaseModel):
    type: str
    title: str


class DocumentLabelRelationship(BaseModel):
    label: Label
    relationship: str


class Document(BaseModel):
    id: int
    title: str
    labels: list[DocumentLabelRelationship]


def transform(input: NavigatorDocument) -> Document:
    geography_document_label_relationships = [
        DocumentLabelRelationship(
            label=Label(type="geography", title=geography), relationship="part_of"
        )
        for geography in input.family.geographies
    ]
    return Document(
        id=input.id,
        title=input.title,
        labels=[
            DocumentLabelRelationship(
                label=Label(type="family", title=input.family.title),
                relationship="part_of",
            ),
        ]
        + geography_document_label_relationships,
    )
