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


class Document(BaseModel):
    id: int
    title: str
    labels: list[Label]


def transform(input: NavigatorDocument) -> Document:

    return Document(
        id=input.id,
        title=input.title,
        labels=[
            Label(type="family", title=input.family.title),
            Label(type="geography", title=input.family.geographies[0]),
        ],
    )
