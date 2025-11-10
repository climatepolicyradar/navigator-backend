import json
import os

import requests


def cache_all_navigator_families():
    page = 1
    ids = []
    families = []
    while True:
        response = requests.get(
            f"https://api.climatepolicyradar.org/families/?page={page}",
            timeout=5,
        )
        response.raise_for_status()
        data = response.json()
        if len(data["data"]) == 0:
            break
        page += 1
        ids.extend([item["import_id"] for item in data["data"]])
        families.extend(data["data"])

    os.makedirs(".data_cache", exist_ok=True)
    with open(".data_cache/navigator_family_ids.json", "w") as f:
        json.dump(ids, f, indent=4)
    with open(".data_cache/navigator_families.json", "w") as f:
        json.dump(families, f, indent=4)
    return ids
