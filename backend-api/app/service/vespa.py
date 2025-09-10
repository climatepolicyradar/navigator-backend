from cpr_sdk.search_adaptors import VespaSearchAdapter
from fastapi import Request

from app.config import VESPA_CLOUD_SECRET_TOKEN, VESPA_INSTANCE_URL


def make_vespa_search_adapter() -> VespaSearchAdapter:
    return VespaSearchAdapter(
        vespa_cloud_secret_token=VESPA_CLOUD_SECRET_TOKEN,
        instance_url=VESPA_INSTANCE_URL,
    )


def get_vespa_search_adapter(request: Request) -> VespaSearchAdapter:
    return request.app.state.vespa_search_adapter
