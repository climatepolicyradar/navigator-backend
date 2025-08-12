from tests.non_search.setup_helpers import setup_with_six_families


def test_latest_updates_returns_5_families(data_client, data_db, valid_token):
    setup_with_six_families(data_db)

    response = data_client.get(
        "/api/v1/latest_published", headers={"app-token": valid_token}
    )

    assert response.status_code == 200
    assert len(response.json()) == 5

    expected_fields = [
        "import_id",
        "title",
        "description",
        "category",
        "published_date",
        "last_modified",
        "metadata",
        "geographies",
        "slug",
    ]

    for i, family in enumerate(response.json()):
        missing_fields = [field for field in expected_fields if field not in family]
        family_id = family.get("id", f"index {i}")
        assert (
            not missing_fields
        ), f"Missing fields: {missing_fields} for family {family_id}: {family}"
