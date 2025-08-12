from tests.non_search.setup_helpers import setup_with_six_families


def test_latest_updates_returns_5_most_recently_updated_families(
    data_client, data_db, valid_token
):
    setup_with_six_families(data_db)

    response = data_client.get(
        "/api/v1/latest_published", headers={"app-token": valid_token}
    )

    assert response.status_code == 200
    assert len(response.json()) == 5
