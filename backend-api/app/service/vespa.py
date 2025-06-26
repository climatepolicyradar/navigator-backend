from app.config import VESPA_SECRETS_LOCATION, VESPA_URL
from cpr_sdk.search_adaptors import VespaSearchAdapter
from fastapi import Request


def make_vespa_search_adapter() -> VespaSearchAdapter:
    return VespaSearchAdapter(
        instance_url=VESPA_URL,
        cert_directory=VESPA_SECRETS_LOCATION,
    )


def get_vespa_search_adapter(request: Request) -> VespaSearchAdapter:
    return request.app.state.vespa_search_adapter
