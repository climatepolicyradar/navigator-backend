from db_client.models import Base
from sqlalchemy.orm import class_mapper

_AnyModel = Base


def json_serialize(model: _AnyModel):
    """Transforms a model into a dictionary which can be dumped to JSON."""
    # first we get the names of all the columns on your model
    columns = [c.key for c in class_mapper(model.__class__).columns]
    # then we return their values in a dict
    return dict((c, getattr(model, c)) for c in columns)
