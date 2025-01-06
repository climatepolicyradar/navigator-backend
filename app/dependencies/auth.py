from typing import Optional

from fastapi import Depends, HTTPException, Request, status
from sqlalchemy.orm import Session

from app.clients.db.session import get_db
from app.service.custom_app import AppTokenFactory


def get_validated_token(
    request: Request, db: Session = Depends(get_db)
) -> Optional[AppTokenFactory]:
    """Get and validate the app token from request state."""
    raw_token = getattr(request.state, "raw_token", None)
    if not raw_token:
        return None

    # Create token instance and validate it
    token = AppTokenFactory()
    try:
        token.decode_and_validate(db, request, raw_token)
        return token
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(e),
        )
