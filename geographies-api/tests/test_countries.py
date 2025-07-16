def test_countries_endpoint_returns_country(test_client):
    response = test_client.get("geographies/countries/AGO")
    assert response.status_code == 200
    assert response.json() == {
        "alpha_2": "AO",
        "alpha_3": "AGO",
        "flag": "🇦🇴",
        "name": "Angola",
        "numeric": "024",
        "official_name": "Republic of Angola",
    }


def test_countries_endpoint_returns_404_for_nonexistent_country(test_client):
    response = test_client.get("geographies/countries/XYZ")

    assert response.status_code == 404
    assert response.json() == {"detail": "Country with alpha-3 code 'XYZ' not found"}


def test_countries_endpoint_returns_CPR_related_geography(test_client):
    response = test_client.get("geographies/countries/XAB")
    assert response.status_code == 200
    assert response.json() == {
        "alpha_2": "XAB",
        "alpha_3": "XAB",
        "name": "International",
        "official_name": "International",
        "numeric": "",
        "flag": "",
    }


def test_countries_endpoint_returns_subdivisions(test_client):
    response = test_client.get("geographies/subdivisions/SGP")
    assert response.status_code == 200
    assert response.json() == [
        {
            "code": "SG-01",
            "name": "Central Singapore",
            "type": "District",
            "country_alpha_2": "SG",
            "country_alpha_3": "SGP",
        },
        {
            "code": "SG-02",
            "name": "North East",
            "type": "District",
            "country_alpha_2": "SG",
            "country_alpha_3": "SGP",
        },
        {
            "code": "SG-03",
            "name": "North West",
            "type": "District",
            "country_alpha_2": "SG",
            "country_alpha_3": "SGP",
        },
        {
            "code": "SG-04",
            "name": "South East",
            "type": "District",
            "country_alpha_2": "SG",
            "country_alpha_3": "SGP",
        },
        {
            "code": "SG-05",
            "name": "South West",
            "type": "District",
            "country_alpha_2": "SG",
            "country_alpha_3": "SGP",
        },
    ]


def test_countries_endpoint_returns_all_subdivisions(test_client):
    response = test_client.get("geographies/subdivisions")
    assert response.status_code == 200
    data = response.json()

    assert isinstance(data, list), "Response should be a list"

    assert len(data) > 1, "Response list should contain more than one subdivision"

    expected_keys = {"code", "name", "type", "country_alpha_2", "country_alpha_3"}
    assert expected_keys.issubset(
        data[0].keys()
    ), "Missing expected keys in subdivision item"
