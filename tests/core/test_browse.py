from db_client.models.dfce import Geography

from app.api.api_v1.schemas.search import SearchResponse
from app.core.browse import BrowseArgs, browse_rds_families
from tests.non_search.setup_helpers import setup_with_two_docs


def test_browse_rds_families(data_db):
    setup_with_two_docs(data_db)
    geo = data_db.query(Geography).get(1)
    expected = 2

    args = BrowseArgs(
        country_codes=[geo.value],
        categories=["Executive"],
        offset=0,
        limit=10,
    )
    result: SearchResponse = browse_rds_families(data_db, args)
    assert result.hits == expected
