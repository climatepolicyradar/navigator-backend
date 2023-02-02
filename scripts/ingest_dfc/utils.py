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
    after_create = None
    if "extra" in kwargs.keys():
        extra = kwargs["extra"]
        del kwargs["extra"]
    if "after_create" in kwargs.keys():
        after_create = kwargs["after_create"]
        del kwargs["after_create"]

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
        if after_create:
            after_create(instance)
        return instance

def sanitize(value):
    s = str(value)
    if s.startswith("<class"):
        # Magic parsing of class name
        return s[8:-2].split(".")[-1]
    if len(s) > 80:
        return s[:80] + "..."
    return s


def to_dict(base_object: object):
    extra = ["__class__"]
    return dict((col, sanitize(getattr(base_object, col))) for col in base_object.__table__.columns.keys() + extra)



def mypprint(d, indent=0):
    def print_item(k, v, indent):
        indent += 4
        if type(v) == dict:
            print(" "*indent + f"{k}: ")
            mypprint(v,indent)
        else:
            print(" "*indent + f"{k}: {v}")
              
    print(" "*indent + "{")
    sorted_d = dict(sorted(d.items()))
    for k, v in sorted_d.items():
        print_item(k, v, indent) 
    print(" "*indent + "}")