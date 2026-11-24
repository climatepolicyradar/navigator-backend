import pytest
from db_client.models import Base
from sqlalchemy.orm import class_mapper

_AnyModel = Base


def json_serialize(model: _AnyModel):
    """Transforms a model into a dictionary which can be dumped to JSON."""
    # first we get the names of all the columns on your model
    columns = [c.key for c in class_mapper(model.__class__).columns]
    # then we return their values in a dict
    return dict((c, getattr(model, c)) for c in columns)


# Vespa has been retired and the endpoints that queried it are gated with a 410
# (see ENRI-1959). Tests for those endpoints are skipped rather than deleted so
# they can be restored alongside the endpoints.
VESPA_RETIRED_SKIP = pytest.mark.skip(
    reason="Vespa retired: endpoint gated with 410 Gone (ENRI-1959)"
)
