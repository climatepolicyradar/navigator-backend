from typing import Generic, TypeVar

from pydantic import BaseModel

InputData = TypeVar("InputData")


class SourceDocument(BaseModel, Generic[InputData]):
    source: InputData


class IdentifiedSourceDocument(BaseModel, Generic[InputData]):
    source: InputData
    id: str


class Document(BaseModel):
    id: str
