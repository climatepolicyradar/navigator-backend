from db_client.functions.dfce_helpers import add_families
from db_client.models.dfce import Family, Slug

from app.api.api_v1.routers.families import get_latest_slug
from app.service import custom_app
from app.service.custom_app import AppTokenFactory
from tests.non_search.setup_helpers import (
    generate_documents,
    setup_new_corpus,
    setup_with_six_families,
    setup_with_two_docs_one_family,
)


def test_endpoint_returns_five_families_as_default(data_client, data_db, valid_token):
    setup_with_six_families(data_db)

    all_families = data_db.query(Family).all()

    # Make sure we have more than 5 families in the DB
    assert len(all_families) > 5

    response = data_client.get("/api/v1/latest", headers={"app-token": valid_token})

    assert response.status_code == 200

    assert len(response.json()) == 5

    expected_fields = [
        "import_id",
        "title",
        "created",
        "slug",
    ]

    for i, family in enumerate(response.json()):
        missing_fields = [field for field in expected_fields if field not in family]
        family_id = family.get("id", f"index {i}")
        assert (
            not missing_fields
        ), f"Missing fields: {missing_fields} for family {family_id}: {family}"


def test_endpoint_returns_families_with_limit_passed(data_client, data_db, valid_token):
    setup_with_six_families(data_db)

    all_families = data_db.query(Family).all()

    # Make sure we have more than 5 families in the DB
    assert len(all_families) > 5

    response = data_client.get(
        "/api/v1/latest?limit=3", headers={"app-token": valid_token}
    )

    assert response.status_code == 200

    assert len(response.json()) == 3

    expected_fields = [
        "import_id",
        "title",
        "created",
        "slug",
    ]

    for i, family in enumerate(response.json()):
        missing_fields = [field for field in family if field not in expected_fields]
        family_id = family.get("id", f"index {i}")
        assert (
            not missing_fields
        ), f"Missing fields: {missing_fields} for family {family_id}: {family}"


def create_token(monkeypatch):
    """Generate valid config token using TOKEN_SECRET_KEY."""

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
    """Test that the latest endpoint returns families within the token corpora."""

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

    response = data_client.get("/api/v1/latest", headers={"app-token": token})

    assert response.status_code == 200
    families = response.json()
    assert len(families) == 1

    for family in families:
        assert family["import_id"].startswith("CCLW.family")


def test_returns_one_slug_from_slugs(data_db):
    setup_with_two_docs_one_family(data_db)

    additional_slug = Slug(
        family_import_id="CCLW.family.1001.0",
        family_document_import_id=None,
        name="AdditionalSlug",
    )

    new_slug = Slug(
        family_import_id="CCLW.family.1001.0",
        family_document_import_id=None,
        name="NewSlug",
    )

    data_db.add_all([additional_slug, new_slug])
    data_db.commit()

    data_db.flush()

    family = (
        data_db.query(Family).filter(Family.import_id == "CCLW.family.1001.0").one()
    )

    # We can't reliably test that the slug is the latest unless we control the created timestamp
    # This is not possible in a test environment as all modifications and additions to the test db are ran as one transaction
    # So we just test that we get a string back from the function after passing the 3 slugs on the family (two we just added and one from setup)
    assert len(family.slugs) == 3
    assert isinstance(get_latest_slug(family.slugs), str)
