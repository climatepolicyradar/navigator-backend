from typing import Any, Optional, cast

import jwt
from db_client.models.organisation import AppUser
from fastapi import Depends, HTTPException, status
from jwt import PyJWTError

from app.api.api_v1.schemas.user import JWTUser
from app.core import security
from app.repository.user import get_app_user_by_email

CREDENTIALS_EXCEPTION = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Could not validate credentials",
    headers={"WWW-Authenticate": "Bearer"},
)


def _decode_jwt(token: str = Depends(security.oauth2_scheme)) -> JWTUser:
    """Light-weight user-retrieval that only decodes the JWT."""
    try:
        payload = jwt.decode(
            token, security.SECRET_KEY, algorithms=[security.ALGORITHM]
        )

        email: Optional[str] = payload.get("email")
        if email is None:
            raise CREDENTIALS_EXCEPTION

        authorisation: Optional[dict[str, Any]] = payload.get("authorisation")

        jwt_user = JWTUser(
            email=email,
            is_superuser=payload.get("is_superuser", False),
            authorisation=authorisation,
        )
        return jwt_user
    except PyJWTError:
        raise CREDENTIALS_EXCEPTION


def authenticate_user(db, email: str, password: str) -> Optional[AppUser]:
    try:
        app_user = get_app_user_by_email(db, email)
    except HTTPException:
        return None
    if not security.verify_password(password, cast(str, app_user.hashed_password)):
        return None
    return app_user


def get_user_details(
    current_user: JWTUser = Depends(_decode_jwt),
) -> JWTUser:
    return current_user


def get_superuser_details(
    current_user: JWTUser = Depends(get_user_details),
) -> JWTUser:
    if not current_user.is_superuser:
        raise HTTPException(status_code=404, detail="Not Found")
    return current_user
