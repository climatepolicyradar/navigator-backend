from typing import Mapping

from pydantic import BaseModel


class Geography(BaseModel):
    """DTO for Geography object used by world map."""

    display_name: str
    iso_code: str
    slug: str
    family_counts: Mapping[str, int]
