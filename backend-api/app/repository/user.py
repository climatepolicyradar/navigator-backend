from db_client.models.organisation import AppUser, Organisation, OrganisationUser
from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session


def get_app_user_by_email(db: Session, email: str) -> AppUser:
    stmt = select(AppUser).where(AppUser.email == email)
    user = db.execute(stmt).scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user


def get_app_user_authorisation(
    db: Session, app_user: AppUser
) -> list[tuple[OrganisationUser, Organisation]]:
    stmt = (
        select(OrganisationUser, Organisation)
        .where(OrganisationUser.appuser_email == app_user.email)
        .join(Organisation, Organisation.id == OrganisationUser.organisation_id)
    )
    return [tuple(row) for row in db.execute(stmt).all()]
