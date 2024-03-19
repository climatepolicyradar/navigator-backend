from typing import Any, Mapping

from db_client.models.law_policy import FamilyCategory
from pydantic import BaseModel

Json = dict[str, Any]


class Geography(BaseModel):
    """DTO for Geography object used by world map."""

    display_name: str
    iso_code: str
    slug: str
    family_counts: Mapping[FamilyCategory, int]
