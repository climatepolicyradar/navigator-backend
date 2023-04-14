import requests
import sys
from tqdm import tqdm
import json

HOST = "http://localhost:8888"
URL = f"{HOST}/api/v1/admin/validate/climate-laws-urls"
CDN_DOMAIN = "https://cdn.dev.climatepolicyradar.org"


def call_api(cdn: str) -> int:
    """Retrieve the headers of the document and return the status code."""
    return requests.get(
        f"{CDN_DOMAIN}/{cdn}", headers={"Content-Type": "text"}
    ).status_code


def validate_cdn_urls(token: str):
    payload = {}
    headers = {
        "Authorization": "Bearer " + token,
    }

    response = requests.request("HEAD", URL, headers=headers, data=payload)

    results = {
        id_: {"status_code": call_api(cdn), "cdn": cdn}
        for id_, cdn in tqdm(response.json()["climate_laws"])
    }

    with open("cdn_results.json", "w") as f:
        f.write(json.dumps(results, indent=4))


if __name__ == "__main__":
    bearer_token = sys.argv[1]
    validate_cdn_urls(token=bearer_token)
