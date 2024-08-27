import pytest
from db_client.models.dfce import Geography
from db_client.models.organisation import Organisation


@pytest.fixture
def summary_geography_family_data(test_db):
    geos = [
        Geography(
            display_value="A place on the land", slug="a-place-on-the-land", value="XXX"
        ),
        Geography(
            display_value="A place in the sea", slug="a-place-in-the-sea", value="YYY"
        ),
    ]
    organisations = [Organisation(name="test org")]

    test_db.add_all(geos)
    test_db.add_all(organisations)
    test_db.flush()

    # Now setup the Documents/Families

    ## WORKING HERE
    documents = []
    families = []

    test_db.add_all(documents)
    test_db.add_all(families)
    test_db.flush()

    # Now some events
    events = []

    test_db.add_all(events)

    test_db.commit()
    yield {
        "db": test_db,
        "docs": documents,
        "families": documents,
        "geos": geos,
        "organisations": organisations,
        "events": events,
    }
