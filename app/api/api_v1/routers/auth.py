import logging

from app.core.security import create_access_token
from app.core.auth import authenticate_user
from app.db.session import get_db
from app.db.crud.user import get_app_user_authorisation
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm

auth_router = r = APIRouter()

_LOGGER = logging.getLogger(__file__)


@r.post("/tokens")
async def login(db=Depends(get_db), form_data: OAuth2PasswordRequestForm = Depends()):
    _LOGGER.info(
        "Auth token requested",
        extra={"props": {"user_id": form_data.username}},
    )

    authenticated = authenticate_user(db, form_data.username, form_data.password)
    if authenticated is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    user, app_user = authenticated
    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User is de-activated",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if user.is_superuser:
        permissions = "admin"
    else:
        permissions = "user"

    token_data = {
        "sub": user.email,  # TODO: switch to user id
        "email": user.email,
        "permissions": permissions,  # TODO: remove after transition
        "is_active": user.is_active,  # TODO: remove after transition
    }

    if app_user is not None:
        app_user_links = get_app_user_authorisation(db, app_user)
        token_data.update(
            {
                "authorization": {
                    org.name: {"is_admin": org_user.is_admin}
                    for org_user, org in app_user_links
                }
            }
        )

    access_token = create_access_token(data=token_data)

    _LOGGER.info(
        "Auth token generated",
        extra={"props": {"user_id": form_data.username}},
    )
    return {"access_token": access_token, "token_type": "bearer"}
