from datetime import datetime
import enum
from sqlalchemy.orm import Session

from app.db.session import Base

class PublicationDateAccuracy(enum.Enum):
    """Uses the microsecond part of a datetime to record its accuracy.
    
    This assumes that the application will never need microsecond.
    """
    SECOND_ACCURACY = 100000,
    MINUTE_ACCURACY = 200000,
    HOUR_ACCURACY = 300000,
    DAY_ACCURACY = 400000,
    MONTH_ACCURACY = 500000,
    YEAR_ACCURACY = 600000,
    NOT_DEFINED = 999999,


"""An undefined datetime"""
UNDEFINED_DATA_TIME = datetime(1900, 1, 1, 0, 0, 0, 999999)


def get_or_create(db: Session, model, **kwargs) -> Base:
    """Gets or Creates a row represented by model, and described by kwargs.

    Args:
        db (Session): connection to the database.  
        model (_type_): the model (table) you are querying.
        kwargs: a list of attributes to describe the row you are interested in.
        NOTE:
            - if kwargs contains an `extra` key then this will be used during 
            creation.
            - if kwargs contains an `after_create` key then the value should 
            be a callback function that is called after an object is created.

    Returns:
        Base : The object that was either created or retrieved, or None
    """
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


def _sanitize(value: str) -> str:
    """Sanitizes a string by parsing out the class name and truncating.

    Used by `to_dict()`
    Args:
        value (str): the string to be sanitized.

    Returns:
        str: the sanitized string.
    """
    s = str(value)
    if s.startswith("<class"):
        # Magic parsing of class name
        return s[8:-2].split(".")[-1]
    if len(s) > 80:
        return s[:80] + "..."   
    return s


def to_dict(base_object: Base) -> dict:
    """Returns a dict of the attributes of the db Base object.
    
    This also adds the class name too.
    """
    extra = ["__class__"]
    return dict((col, _sanitize(getattr(base_object, col))) for col in base_object.__table__.columns.keys() + extra)


def mypprint(dict_to_print:dict, indent:int =0) -> None:
    """Prints a prettier for of a dict than pprint can."""
    def print_item(k, v, indent):
        indent += 4
        if type(v) == dict:
            print(" "*indent + f"{k}: ")
            mypprint(v, indent)
        else:
            print(" "*indent + f"{k}: {v}")
              
    print(" "*indent + "{")
    sorted_d = dict(sorted(dict_to_print.items()))
    for k, v in sorted_d.items():
        print_item(k, v, indent) 
    print(" "*indent + "}")