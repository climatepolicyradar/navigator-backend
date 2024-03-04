import os


def test_read_main(test_client):
    response = test_client.get("/api/v1")
    assert response.status_code == 200
    assert response.json() == {"message": "CPR API v1"}


def test_read_docs(test_client):
    docs_enabled_env = os.getenv("ENABLE_API_DOCS", "")
    docs_response = test_client.get("/api/docs")
    openapi_response = test_client.get("/api")

    if docs_enabled_env.lower() == "true":
        assert docs_response.status_code == 200
        assert openapi_response.status_code == 200
    else:
        assert docs_response.status_code == 404
        assert openapi_response.status_code == 404
