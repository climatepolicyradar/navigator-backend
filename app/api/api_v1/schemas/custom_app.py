from typing import Optional

from pydantic import BaseModel


class CustomAppConfigDTO(BaseModel):
    """A JSON representation of custom app configurable options."""

    allowed_corpora_ids: list[str]
    years: Optional[int]
