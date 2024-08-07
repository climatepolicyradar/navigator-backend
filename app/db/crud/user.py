from db_client.models.organisation import AppUser, Organisation, OrganisationUser
from fastapi import HTTPException
from sqlalchemy.orm import Session


def get_app_user_by_email(db: Session, email: str) -> AppUser:
    user = db.query(AppUser).filter(AppUser.email == email).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user


def get_app_user_authorisation(
    db: Session, app_user: AppUser
) -> list[tuple[OrganisationUser, Organisation]]:
    return (
        db.query(OrganisationUser, Organisation)
        .filter(OrganisationUser.appuser_email == app_user.email)
        .join(Organisation, Organisation.id == OrganisationUser.organisation_id)
        .all()
    )
