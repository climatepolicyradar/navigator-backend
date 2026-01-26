from pydantic import BaseModel


class Document(BaseModel):
    id: str
    title: str
    description: str | None = None
