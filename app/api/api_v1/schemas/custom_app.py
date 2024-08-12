from datetime import datetime

from pydantic import BaseModel, HttpUrl


class CustomAppConfigDTO(BaseModel):
    """A JSON representation of custom app configurable options."""

    allowed_corpora_ids: list[str]
    subject: str
    audience: HttpUrl
    issuer: str
    expiry: datetime
    issued_at: int
