def test_regions_endpoint_returns_all_regions(test_client):
    response = test_client.get("/geographies/regions")

    assert response.status_code == 200
    assert response.json() == [
        {
            "name": "North America",
            "type": "World Bank Region",
            "slug": "north-america",
        },
        {
            "name": "East Asia & Pacific",
            "type": "World Bank Region",
            "slug": "east-asia-pacific",
        },
        {
            "name": "Latin America & Caribbean",
            "type": "World Bank Region",
            "slug": "latin-america-caribbean",
        },
        {
            "name": "Sub-Saharan Africa",
            "type": "World Bank Region",
            "slug": "sub-saharan-africa",
        },
        {
            "name": "Middle East & North Africa",
            "type": "World Bank Region",
            "slug": "middle-east-north-africa",
        },
        {
            "name": "Europe & Central Asia",
            "type": "World Bank Region",
            "slug": "europe-central-asia",
        },
        {
            "name": "South Asia",
            "type": "World Bank Region",
            "slug": "south-asia",
        },
    ]
