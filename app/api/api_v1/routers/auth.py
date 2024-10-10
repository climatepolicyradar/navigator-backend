import logging
from typing import cast

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm

from app.clients.db.session import get_db
from app.core.auth import authenticate_user
from app.core.security import create_access_token
from app.repository.user import get_app_user_authorisation

auth_router = r = APIRouter()

_LOGGER = logging.getLogger(__file__)


@r.post("/tokens")
async def login(db=Depends(get_db), form_data: OAuth2PasswordRequestForm = Depends()):
    _LOGGER.info(
        "Auth token requested",
        extra={"props": {"user_id": form_data.username}},
    )

    app_user = authenticate_user(db, form_data.username, form_data.password)
    if app_user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    app_user_links = get_app_user_authorisation(db, app_user)
    authorisation = {
        cast(str, org.name): {"is_admin": cast(bool, org_user.is_admin)}
        for org_user, org in app_user_links
    }
    token_data = {
        "sub": cast(str, app_user.email),
        "email": cast(str, app_user.email),
        "is_superuser": cast(bool, app_user.is_superuser),
        "authorisation": authorisation,
    }

    access_token = create_access_token(data=token_data)

    _LOGGER.info(
        "Auth token generated",
        extra={"props": {"user_id": form_data.username}},
    )
    return {"access_token": access_token, "token_type": "bearer"}
