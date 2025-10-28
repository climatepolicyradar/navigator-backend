from typing import Generic, TypeVar

from pydantic import BaseModel

ExtractedData = TypeVar("ExtractedData")


class Extracted(BaseModel, Generic[ExtractedData]):
    """Generic type for a source document.

    We can inherit this later down the line and augment it with the more
    specific type to build a more specific source document.
    """

    data: ExtractedData
    source: str


class Identified(BaseModel, Generic[ExtractedData]):
    data: Extracted[ExtractedData]
    id: str


class Document(BaseModel):
    id: str
