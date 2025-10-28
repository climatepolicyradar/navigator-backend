from pydantic import BaseModel


class NavigatorDocument(BaseModel):
    id: str


def extract_navigator_document(id: str) -> NavigatorDocument:
    return NavigatorDocument(id=id)
