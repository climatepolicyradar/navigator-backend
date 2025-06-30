from db_client.models.organisation import Organisation
from sqlalchemy.orm import Session


def get(db: Session, org_id: int) -> Organisation:
    return db.query(Organisation).filter(Organisation.id == org_id).one()
