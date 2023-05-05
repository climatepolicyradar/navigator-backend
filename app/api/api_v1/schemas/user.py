from typing import Any, Mapping, Optional

from pydantic import BaseModel


class _User(BaseModel):  # noqa: D101
    email: str


class UserFlags(BaseModel):  # noqa: D101
    is_active: bool = False
    is_superuser: bool = False
    authorization: Optional[Mapping[str, Any]] = None


class JWTUser(_User, UserFlags):
    """Used by get_current_user dependency injection"""


class UserPreferences(BaseModel):  # noqa: D101
    names: Optional[str] = None
    job_role: Optional[str] = None
    location: Optional[str] = None
    affiliation_organisation: Optional[str] = None
    affiliation_type: Optional[list[str]] = None
    policy_type_of_interest: Optional[list[str]] = None
    geographies_of_interest: Optional[list[str]] = None
    data_focus_of_interest: Optional[list[str]] = None


class UserBaseWithoutFlags(_User, UserPreferences):  # noqa: D101
    pass


class UserBase(_User, UserFlags, UserPreferences):  # noqa: D101
    pass


class UserCreate(UserBaseWithoutFlags):  # noqa: D101
    """Used by regular users"""


class UserCreateAdmin(UserBase):  # noqa: D101
    """Used by admin"""


class UserOut(UserBase):  # noqa: D101
    pass


class ResetPassword(BaseModel):
    """Resets a password with a token (latter usu. sent to requester's inbox)."""

    token: str
    password: str


class User(UserBase):  # noqa: D101
    id: int

    class Config:  # noqa: D106
        orm_mode = True


class Token(BaseModel):  # noqa: D101
    access_token: str
    token_type: str


class TokenData(BaseModel):  # noqa: D101
    email: str
    permissions: str = "user"
