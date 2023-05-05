from typing import Optional, Tuple

from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.db.models.app import AppUser, Organisation, OrganisationUser
from app.db.models.deprecated import User


def get_user_by_email(db: Session, email: str) -> User:
    user = db.query(User).filter(User.email == email).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user


def get_app_user_by_email(db: Session, email: str) -> Optional[AppUser]:
    user = db.query(User).filter(User.email == email).first()
    # TODO: add after transition to AppUser
    # if not user:
    #     raise HTTPException(status_code=404, detail="User not found")
    return user


def get_app_user_authorisation(
    db: Session, app_user: AppUser
) -> list[Tuple[OrganisationUser, Organisation]]:
    return (
        db.query(OrganisationUser, Organisation)
        .filter(OrganisationUser.appuser_email == app_user.email)
        .join(Organisation, Organisation.id == OrganisationUser.organisation_id)
        .all()
    )
