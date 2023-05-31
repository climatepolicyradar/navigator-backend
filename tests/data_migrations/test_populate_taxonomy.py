from app.core.organisation import get_organisation_taxonomy_by_name
from app.data_migrations import populate_event_type, populate_taxonomy


def test_populate_taxonomy_cclw_no_transport(
    test_db,
):
    populate_taxonomy(db=test_db)
    taxonomy = get_organisation_taxonomy_by_name(test_db, "CCLW")

    assert "sector" in taxonomy
    assert "Transportation" not in taxonomy["sector"]["allowed_values"]


def test_populate_taxonomy_cclw_correct_counts(test_db):
    populate_event_type(test_db)
    populate_taxonomy(test_db)
    taxonomy = get_organisation_taxonomy_by_name(test_db, "CCLW")

    assert 7 == len(taxonomy)

    assert "event_types" in taxonomy
    assert 17 == len(taxonomy["event_types"]["allowed_values"])

    assert "topic" in taxonomy
    assert 4 == len(taxonomy["topic"]["allowed_values"])

    assert "sector" in taxonomy
    assert 23 == len(taxonomy["sector"]["allowed_values"])

    assert "keyword" in taxonomy
    assert 219 == len(taxonomy["keyword"]["allowed_values"])

    assert "instrument" in taxonomy
    assert 25 == len(taxonomy["instrument"]["allowed_values"])

    assert "hazard" in taxonomy
    assert 81 == len(taxonomy["hazard"]["allowed_values"])

    assert "framework" in taxonomy
    assert 3 == len(taxonomy["framework"]["allowed_values"])


def test_populate_taxonomy_unf3c_correct_counts(test_db):
    populate_event_type(test_db)
    populate_taxonomy(test_db)
    taxonomy = get_organisation_taxonomy_by_name(test_db, "UNFCCC")

    assert 3 == len(taxonomy)

    assert "event_types" in taxonomy
    assert 17 == len(taxonomy["event_types"]["allowed_values"])

    assert "submission_type" not in taxonomy

    assert "author_type" in taxonomy
    assert 2 == len(taxonomy["author_type"]["allowed_values"])
    assert not taxonomy["author_type"]["allow_any"]

    assert "author" in taxonomy
    assert 0 == len(taxonomy["author"]["allowed_values"])
    assert taxonomy["author"]["allow_any"]
