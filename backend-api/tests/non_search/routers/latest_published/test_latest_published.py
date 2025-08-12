from datetime import datetime, timedelta

from db_client.functions.dfce_helpers import add_event, add_families
from db_client.models.dfce import Family

from tests.non_search.setup_helpers import generate_documents, setup_with_six_families


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


def test_latest_updates_returns_5_most_recently_published_families(
    data_client, data_db, valid_token
):
    setup_with_six_families(data_db)

    all_families = data_db.query(Family).all()
    all_families.sort(key=lambda x: x.published_date, reverse=True)

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

    expected_families = all_families[:5]
    expected_family_import_ids = [f.import_id for f in expected_families]
    # Check that the response contains most recently published families
    # assert [f["import_id"] for f in response.json()] == expected_family_import_ids

    assert response.json() == [
        {
            "title": "Family1",
            "slug": "FamSlug1",
            "import_id": "CCLW.executive.1.0",
            "corpus_import_id": "CCLW.corpus.i00000001.n0000",
            "published_date": datetime.now(),
            "last_modified": datetime.now(),
            "description": "Summary1",
            "geographies": [1],
            "category": "Executive",
            "documents": [],
            "metadata": {},
        },
        {
            "title": "Family2",
            "slug": "FamSlug2",
            "import_id": "CCLW.executive.2.0",
            "corpus_import_id": "CCLW.corpus.i00000001.n0000",
            "published_date": datetime.now() - timedelta(days=1),
            "last_modified": datetime.now() - timedelta(days=1),
            "description": "Summary2",
            "geographies": [1],
            "category": "Executive",
            "documents": [],
            "metadata": {},
        },
        {
            "title": "Family3",
            "slug": "FamSlug3",
            "import_id": "CCLW.executive.3.0",
            "corpus_import_id": "CCLW.corpus.i00000001.n0000",
            "published_date": datetime.now() - timedelta(days=2),
            "last_modified": datetime.now() - timedelta(days=2),
            "description": "Summary3",
            "geographies": [1],
            "category": "Executive",
            "documents": [],
            "metadata": {},
        },
        {
            "title": "Family4",
            "slug": "FamSlug4",
            "import_id": "CCLW.executive.4.0",
            "corpus_import_id": "CCLW.corpus.i00000001.n0000",
            "published_date": datetime.now() - timedelta(days=3),
            "last_modified": datetime.now() - timedelta(days=3),
            "description": "Summary4",
            "geographies": [1],
            "category": "Executive",
            "documents": [],
            "metadata": {},
        },
        {
            "title": "Family5",
            "slug": "FamSlug5",
            "import_id": "CCLW.executive.5.0",
            "corpus_import_id": "CCLW.corpus.i00000001.n0000",
            "published_date": datetime.now() - timedelta(days=4),
            "last_modified": datetime.now() - timedelta(days=4),
            "description": "Summary5",
            "geographies": [1],
            "category": "Executive",
            "documents": [],
            "metadata": {},
        },
        {
            "title": "Family6",
            "slug": "FamSlug6",
            "import_id": "CCLW.executive.6.0",
            "corpus_import_id": "CCLW.corpus.i00000001.n0000",
            "published_date": datetime.now() - timedelta(days=5),
            "last_modified": datetime.now() - timedelta(days=5),
            "description": "Summary6",
            "geographies": [1],
            "category": "Executive",
            "documents": [],
            "metadata": {},
        },
    ]
