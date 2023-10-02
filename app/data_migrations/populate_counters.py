from sqlalchemy.orm import Session

from app.db.models.app.counters import (
    ORGANISATION_CCLW,
    ORGANISATION_UNFCCC,
    EntityCounters,
)


def populate_counters(db: Session):
    n_rows = db.query(EntityCounters).count()
    if n_rows == 0:
        db.add(
            EntityCounters(
                prefix=ORGANISATION_CCLW, description="Counter for CCLW entities"
            )
        )
        db.add(
            EntityCounters(
                prefix=ORGANISATION_UNFCCC, description="Counter for UNFCCC entities"
            )
        )
        db.commit()
