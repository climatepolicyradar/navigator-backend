import pytest


@pytest.mark.cors
@pytest.mark.parametrize(
    "origin,should_be_allowed",
    [
        ("http://localhost:3000", True),  # local testing
        ("https://app.climatepolicyradar.org", True),  # main app URL
        ("https://app.dev.climatepolicyradar.org", True),  # main staging URL
        ("https://random.dev.climatepolicyradar.org", True),  # main staging URL
        ("https://some.sandbox.climatepolicyradar.org", True),  # sandbox test
        ("https://some-other.sandbox.climatepolicyradar.org", True),  # sandbox test
        ("https://climate-laws.org", True),  # base climate laws URL
        ("https://preview.climate-laws.org", True),  # climate laws subdomain URL
        ("http://app.climatepolicyradar.org", False),  # bad scheme
        ("https://.climatepolicyradar.org", False),  # empty subdomain
        ("https://app.climatepolicyradar.com", False),  # wrong domain
        ("https://app.devclimatepolicyradar.com", False),  # prefixed wrong domain
        ("https://app-climatepolicyradar.com", False),  # prefixed wrong domain
        ("https://prefix-climate-laws.org", False),  # climate laws prefixed domain
        ("https://.climateprojectexplorer.org", False),  # empty subdomain
        ("https://prefix-climateprojectexplorer.org", False),  # MCF prefixed domain
        ("https://climateprojectexplorer.org", True),  # base MCF URL
        (
            "https://preview.climateprojectexplorer.org",
            True,
        ),  # MCF subdomain URL
    ],
)
def test_cors_regex(test_client, origin, should_be_allowed):
    response = test_client.options(
        "/api/v1/config",
        headers={"Origin": origin, "Access-Control-Request-Method": "GET"},
    )
    if should_be_allowed:
        assert response.status_code == 200
    else:
        assert response.status_code == 400
