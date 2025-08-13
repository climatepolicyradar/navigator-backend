from db_client.functions.dfce_helpers import add_families
from db_client.models.dfce import Family

from app.service import custom_app
from app.service.custom_app import AppTokenFactory
from tests.non_search.setup_helpers import (
    generate_documents,
    setup_new_corpus,
    setup_with_six_families,
)


def test_endpoint_does_not_unpublished_families(data_client, data_db, valid_token):
    family_data = {
        "import_id": "CCLW.family.1001.0",
        "corpus_import_id": "CCLW.corpus.i00000001.n0000",
        "title": "Fam1",
        "slug": "FamSlug1",
        "description": "Summary1",
        "geography_id": 1,
        "category": "Executive",
        "documents": [],
        "metadata": {},
    }

    add_families(data_db, families=[family_data])
    response = data_client.get(
        "/api/v1/latest_published", headers={"app-token": valid_token}
    )

    assert response.status_code == 200
    assert isinstance(response.json(), list)
    assert len(response.json()) == 0


def test_latest_updates_returns_five_families(data_client, data_db, valid_token):
    setup_with_six_families(data_db)

    all_families = data_db.query(Family).all()

    # Make sure we have more than 5 families in the DB
    assert len(all_families) > 5

    response = data_client.get(
        "/api/v1/latest_published", headers={"app-token": valid_token}
    )

    assert response.status_code == 200
    # Check that the response is a list of only 5 families
    assert len(response.json()) == 5

    expected_fields = [
        "import_id",
        "title",
        "description",
        "family_category",
        "published_date",
        "last_modified",
        "metadata",
        "geographies",
        "slugs",
    ]

    # Check that the response contains the right data for families
    for i, family in enumerate(response.json()):
        missing_fields = [field for field in expected_fields if field not in family]
        family_id = family.get("id", f"index {i}")
        assert (
            not missing_fields
        ), f"Missing fields: {missing_fields} for family {family_id}: {family}"


def create_token(monkeypatch):
    """Generate valid config token using TOKEN_SECRET_KEY.

    Need to generate the config token using the token secret key from
    your local env file. For tests in CI, this will be the secret key in
    the .env.example file, but for local development this secret key
    might be different (e.g., the one for staging). This fixture works
    around this.
    """

    def mock_return(_, __, ___):
        return True

    corpora_ids = "CCLW.corpus.i00000001.n0000"
    subject = "CCLW"
    audience = "localhost"
    input_str = f"{corpora_ids};{subject};{audience}"

    af = AppTokenFactory()
    monkeypatch.setattr(custom_app.AppTokenFactory, "validate", mock_return)
    return af.create_configuration_token(input_str)


def test_returns_families_within_token_corpora(data_client, data_db, monkeypatch):
    """Test that the latest_published endpoint returns families within the token corpora."""

    corpus = setup_new_corpus(
        data_db,
        "New Corpus Title",
        "New Corpus Description",
        "",
        "New Corpus Image URL",
        "New.corpus.i00000001.n0000",
    )

    document_one, document_two = generate_documents(2)
    family_one = {
        "import_id": "CCLW.family.1001.0",
        "corpus_import_id": "CCLW.corpus.i00000001.n0000",
        "title": "Fam1",
        "slug": "FamSlug1",
        "description": "Summary1",
        "geography_id": 1,
        "category": "Executive",
        "documents": [document_one],
        "metadata": {},
    }
    family_two = {
        "import_id": "UNFCCC.family.1002.0",
        "corpus_import_id": corpus.import_id,
        "title": "Fam2",
        "slug": "FamSlug2",
        "description": "Summary2",
        "geography_id": 2,
        "category": "Legislative",
        "documents": [document_two],
        "metadata": {},
    }

    add_families(data_db, families=[family_one, family_two])

    # Create a token that includes only one corpus
    token = create_token(monkeypatch)

    response = data_client.get("/api/v1/latest_published", headers={"app-token": token})

    assert response.status_code == 200
    families = response.json()
    assert len(families) == 1

    for family in families:
        assert family["import_id"].startswith("CCLW.family") or family[
            "import_id"
        ].startswith("UNFCCC.family")
