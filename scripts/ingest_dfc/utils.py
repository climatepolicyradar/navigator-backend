from datetime import datetime
import enum
from sqlalchemy.orm import Session

class PublicationDateAccuracy(enum.Enum):
    SECOND_ACCURACY = 100000,
    MINUTE_ACCURACY = 200000,
    HOUR_ACCURACY = 300000,
    DAY_ACCURACY = 400000,
    MONTH_ACCURACY = 500000,
    YEAR_ACCURACY = 600000,
    NOT_DEFINED = 999999,


DEFAULT_POLICY_DATE = datetime(1900, 1, 1, 0, 0, 0, 999999)



def get_or_create(db: Session, model, **kwargs):
    # Remove any extra kwargs before we do the search
    extra = {}
    if "extra" in kwargs.keys():
        extra = kwargs["extra"]
        del kwargs["extra"]
    instance = db.query(model).filter_by(**kwargs).first()

    if instance:
        return instance
    else:
        # Add the extra args in for creation
        for k, v in extra.items():
            kwargs[k] = v
        instance = model(**kwargs)
        db.add(instance)
        db.commit()
        return instance

def to_dict(base_object: object):
    extra = ["__class__"]
    return dict((col, str(getattr(base_object, col))) for col in base_object.__table__.columns.keys() + extra)

