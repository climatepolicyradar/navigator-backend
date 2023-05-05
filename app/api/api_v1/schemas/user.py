from typing import Any, Mapping, Optional

from pydantic import BaseModel


class JWTUser(BaseModel):
    """Used by get_current_user dependency injection"""

    email: str
    is_superuser: bool = False
    authorization: Optional[Mapping[str, Any]] = None
