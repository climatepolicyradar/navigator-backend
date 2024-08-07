from fastapi import status


def test_create_config_token_fails_when_invalid_data(test_client):
    response = test_client.post(
        "/api/custom_app_tokens",
        json={"username": "not_a_year", "password": "not_allowed_corpora_ids"},
    )
    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


def test_create_config_token_fails_when_all_data_present(test_client):
    response = test_client.post(
        "/api/custom_app_tokens",
        json={"years": 1, "allowed_corpora_ids": ["a_corpus_id"]},
    )
    assert response.status_code == status.HTTP_200_OK


def test_create_config_token_fails_when_years_missing(test_client):
    response = test_client.post(
        "/api/custom_app_tokens",
        json={
            "years": None,
            "allowed_corpora_ids": ["a_corpus_id", "another_corpus_id"],
        },
    )
    assert response.status_code == status.HTTP_200_OK
