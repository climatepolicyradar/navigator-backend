import json
import os

import requests


def cache_all_navigator_families():
    page = 1
    family_import_ids = []
    families = []
    while True:
        print(f"Fetching page {page}")
        response = requests.get(
            f"https://api.climatepolicyradar.org/families/?page={page}",
            timeout=5,  # seconds
        )
        response.raise_for_status()
        data = response.json()
        if len(data["data"]) == 0:
            break
        page += 1
        family_import_ids.extend([item["import_id"] for item in data["data"]])
        families.extend(data["data"])

    os.makedirs(".data_cache", exist_ok=True)
    with open(".data_cache/navigator_family_ids.json", "w") as f:
        json.dump(family_import_ids, f, indent=4)
    with open(".data_cache/navigator_families.json", "w") as f:
        json.dump(families, f, indent=4)
    return family_import_ids


if __name__ == "__main__":
    cache_all_navigator_families()
