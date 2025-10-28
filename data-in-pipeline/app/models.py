from typing import Generic, TypeVar

from pydantic import BaseModel

InputData = TypeVar("InputData")


class SourceDocument(BaseModel, Generic[InputData]):
    """Generic type for a source document.

    We can inherit this later down the line and augment it with the more
    specific type to build a more specific source document.
    """

    source_data: InputData
    source: str


class IdentifiedSourceDocument(BaseModel, Generic[InputData]):
    source: InputData
    id: str


class Document(BaseModel):
    id: str
