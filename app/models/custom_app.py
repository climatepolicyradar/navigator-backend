from datetime import datetime

from pydantic import BaseModel


class CustomAppConfigDTO(BaseModel):
    """A JSON representation of custom app configurable options."""

    allowed_corpora_ids: list[str]
    subject: str
    audience: str
    issuer: str
    expiry: datetime
    issued_at: int
