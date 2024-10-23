from db_client.models.dfce import Geography

from app.core.browse import BrowseArgs, browse_rds_families
from app.models.search import SearchResponse
from tests.non_search.setup_helpers import setup_with_two_docs


def test_browse_rds_families(data_db):
    setup_with_two_docs(data_db)
    geo = data_db.query(Geography).get(1)
    expected = 1

    args = BrowseArgs(
        country_codes=[geo.value],
        categories=["Executive"],
        offset=0,
        limit=10,
    )
    result: SearchResponse = browse_rds_families(data_db, args)
    assert result.hits == expected

    family = result.families[0]
    assert family.family_slug == "FamSlug1"
    assert family.family_name == "Fam1"
    assert family.family_description == "Summary1"
    assert family.family_category == "Executive"
    assert family.family_date == "2019-12-25T00:00:00+00:00"
    assert family.family_last_updated_date == "2019-12-25T00:00:00+00:00"
    assert family.family_source == "CCLW"
    assert family.corpus_import_id == "CCLW.corpus.i00000001.n0000"
    assert family.corpus_type_name == "Laws and Policies"
    assert family.family_geographies == ["South Asia"]
    assert family.family_metadata == {}
    assert family.total_passage_hits == 0
    assert family.family_documents == []
