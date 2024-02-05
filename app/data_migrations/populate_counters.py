from sqlalchemy.orm import Session

from db_client.models.app.counters import (
    ORGANISATION_CCLW,
    ORGANISATION_UNFCCC,
    EntityCounter,
)


def populate_counters(db: Session):
    n_rows = db.query(EntityCounter).count()
    if n_rows == 0:
        db.add(
            EntityCounter(
                prefix=ORGANISATION_CCLW, description="Counter for CCLW entities"
            )
        )
        db.add(
            EntityCounter(
                prefix=ORGANISATION_UNFCCC, description="Counter for UNFCCC entities"
            )
        )
        db.commit()
