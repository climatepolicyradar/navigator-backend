import requests
import json

from requests import Response

payload = json.dumps(
    {
        "query_string": "",
        "exact_match": False,
        "max_passages_per_doc": 10,
        "keyword_filters": {},
        "year_range": [1947, 2023],
        "sort_field": None,
        "sort_order": "desc",
        "limit": 10000,
        "offset": 0,
    }
)
headers = {"Content-Type": "application/json"}


def make_post_request(url: str) -> Response:
    return requests.request("POST", url, headers=headers, data=payload)


if __name__ == "__main__":
    document_response = make_post_request(
        "https://api.dev.climatepolicyradar.org/api/v1/searches"
    )
    document_import_ids = [
        document["document_id"]
        for document in json.loads(document_response.text)["documents"]
    ]

    # TODO not getting any family documents for the family in the response?
    family_document_response = make_post_request(
        "https://api.dev.climatepolicyradar.org/api/v1/searches?group_documents=True"
    )
    family_document_import_ids = [
        family_document["import_id"]
        for family_document in json.loads(family_document_response.text)["families"]
    ]
