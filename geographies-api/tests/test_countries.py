def test_countries_endpoint_returns_country(test_client, mock_get_countries_data):
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


def test_countries_endpoint_returns_404_for_nonexistent_country(
    test_client, mock_get_countries_data
):
    response = test_client.get("geographies/countries/XYZ")

    assert response.status_code == 404
    assert response.json() == {"detail": "Country with alpha-3 code 'XYZ' not found"}


def test_countries_endpoint_returns_CPR_related_geography(
    test_client, mock_get_countries_data
):
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


def test_countries_endpoint_returns_subdivisions(test_client, mock_get_countries_data):
    response = test_client.get("geographies/subdivisions/ABW")
    assert response.status_code == 200
    assert response.json() == [
        {
            "code": "AW-01",
            "country_alpha_2": "AW",
            "country_alpha_3": "ABW",
            "name": "Oranjestad",
            "type": "Region",
        },
        {
            "code": "AW-02",
            "country_alpha_2": "AW",
            "country_alpha_3": "ABW",
            "name": "San Nicolas",
            "type": "Region",
        },
    ]
