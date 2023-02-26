from http.client import OK


def _url_under_test(slug: str, group_documents: bool = False) -> str:
    url_under_test = f"/api/v1/summaries/country/{slug}"
    if group_documents:
        url_under_test = f"{url_under_test}?group_documents=true"
    return url_under_test


def test_endpoint_returns_documents_ok(client):
    """Test the endpoint returns an empty sets of data"""
    response = client.get(
        _url_under_test("moldova"),
    )
    assert response.status_code == OK
    resp = response.json()

    assert resp["document_counts"]["Law"] == 0
    assert resp["document_counts"]["Policy"] == 0
    assert resp["document_counts"]["Case"] == 0

    assert len(resp["top_documents"]["Law"]) == 0
    assert len(resp["top_documents"]["Policy"]) == 0
    assert len(resp["top_documents"]["Case"]) == 0

    assert len(resp["document_counts"]) == 3
    assert len(resp["top_documents"]) == 3

    assert len(resp["targets"]) == 0


def test_endpoint_returns_families_ok(client):
    """Test the endpoint returns an empty sets of data"""
    response = client.get(
        _url_under_test("moldova", group_documents=True),
    )
    assert response.status_code == OK
    resp = response.json()

    assert resp["family_counts"]["EXECUTIVE"] == 0
    assert resp["family_counts"]["LEGISLATIVE"] == 0

    assert len(resp["top_families"]["EXECUTIVE"]) == 0
    assert len(resp["top_families"]["LEGISLATIVE"]) == 0

    assert len(resp["family_counts"]) == 2
    assert len(resp["top_families"]) == 2

    assert len(resp["targets"]) == 0


def test_geography_with_documents(client, summary_geography_document_data):
    """Test that all the data is returned filtered on category"""
    geography_slug = summary_geography_document_data["geos"][0].slug
    response = client.get(
        _url_under_test(geography_slug),
    )
    assert response.status_code == OK
    resp = response.json()

    assert resp["document_counts"]["Law"] == 3
    assert resp["document_counts"]["Policy"] == 2
    assert resp["document_counts"]["Case"] == 0

    assert len(resp["top_documents"]["Law"]) == 3
    assert len(resp["top_documents"]["Policy"]) == 2
    assert len(resp["top_documents"]["Case"]) == 0

    assert len(resp["targets"]) == 0


def test_geography_with_documents_ordered(client, summary_geography_document_data):
    """Test that all the data is returned ordered by published date"""
    geography_slug = summary_geography_document_data["geos"][0].slug
    response = client.get(
        _url_under_test(geography_slug),
    )
    assert response.status_code == OK
    resp = response.json()

    assert len(resp["top_documents"]["Law"]) == 3

    assert resp["top_documents"]["Law"][0]["document_name"] == "doc3"
    assert resp["top_documents"]["Law"][1]["document_name"] == "doc2"
    assert resp["top_documents"]["Law"][2]["document_name"] == "doc1"


def test_geography_with_families_ordered(client, summary_geography_family_data):
    """Test that all the data is returned ordered by published date"""
    geography_slug = summary_geography_family_data["geos"][0].slug
    response = client.get(
        _url_under_test(geography_slug, group_documents=True),
    )
    assert response.status_code == OK
    resp = response.json()
    assert resp

    # FIXME: working here
