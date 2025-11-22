from db_client.models.organisation import Organisation
from sqlalchemy import select
from sqlalchemy.orm import Session


def get(db: Session, org_id: int) -> Organisation:
    stmt = select(Organisation).where(Organisation.id == org_id)
    return db.execute(stmt).scalar_one()
