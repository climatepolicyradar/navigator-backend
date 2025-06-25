from .conftest import EXPECTED_REGIONS


def test_regions_endpoint_returns_all_regions(test_client):
    response = test_client.get("/geographies/regions")

    assert response.status_code == 200
    assert response.json() == EXPECTED_REGIONS


def test_regions_endpoint_returns_data_for_a_specific_region_when_requested(
    test_client,
):
    for region in EXPECTED_REGIONS:
        response = test_client.get(f"/geographies/regions/{region['slug']}")

        assert response.status_code == 200
        assert response.json() == region


def test_regions_endpoint_returns_404_when_no_regions_exist_for_slug(test_client):
    invalid_slug = "invalid"
    response = test_client.get(f"/geographies/regions/{invalid_slug}")

    assert response.status_code == 404
    assert (
        response.json().get("detail")
        == f"Could not find a region for slug: {invalid_slug}"
    )


# def test_regions_endpoint_returns_all_countries_for_requested_region(
#     test_client, mock_get_countries_data
# ):
#     response = test_client.get("/geographies/regions/north-america/countries")

#     assert response.status_code == 200
#     assert response.json() == [
#         {
#             "alpha_2": "CA",
#             "alpha_3": "CAN",
#             "name": "Canada",
#             "official_name": "Canada",
#             "numeric": "124",
#             "flag": "🇨🇦",
#         },
#         {
#             "alpha_2": "US",
#             "alpha_3": "USA",
#             "name": "United States",
#             "official_name": "United States of America",
#             "numeric": "840",
#             "flag": "🇺🇸",
#         },
#     ]
