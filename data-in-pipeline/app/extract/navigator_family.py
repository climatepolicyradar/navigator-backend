import datetime
import logging
from http import HTTPStatus

import requests
from pydantic import BaseModel
from requests.adapters import HTTPAdapter, Retry

from app.connectors import NavigatorConnectorConfig
from app.models import ExtractedEnvelope
from app.util import generate_envelope_uuid

logger = logging.getLogger(__name__)


class NavigatorFamily(BaseModel):
    import_id: str


def extract_navigator_family(
    import_id: str, config: NavigatorConnectorConfig
) -> ExtractedEnvelope:
    """
    Extract a single family from Navigator API.

    :param import_id: Family identifier
    :param config: Typed connector configuration
    :return ExtractedEnvelope with family data and metadata

    Raises:
        requests.HTTPError: On non-retryable HTTP errors
        requests.Timeout: If request exceeds timeout
    """

    logger.info(f"Extracting family {import_id} from Navigator")

    try:
        family = _fetch_with_retry(import_id, config)

        envelope = ExtractedEnvelope(
            data=family,
            envelope_id=generate_envelope_uuid(),
            source_name="navigator-families",
            source="navigator",
            source_record_id=import_id,
            raw_payload=family.model_dump_json(),
            content_type="application/json",
            connector_version="1.0.0",
            extracted_at=datetime.datetime.now(datetime.timezone.utc),
            metadata={
                "endpoint": f"/families/{import_id}",
                "http_status": HTTPStatus.OK,
            },
        )

        logger.info(f"Successfully extracted family {import_id}")
        return envelope

    except requests.HTTPError as e:
        logger.error(f"HTTP error fetching {import_id}: {e}")
        raise e
    except requests.Timeout as e:
        logger.error(f"Timeout fetching {import_id}: {e}")
        raise e
    except Exception as e:
        logger.error(f"Unexpected error fetching {import_id}: {e}")
        raise e


def _fetch_with_retry(
    import_id: str, config: NavigatorConnectorConfig
) -> NavigatorFamily:
    """
    Fetch family with automatic retries.

    Args:
    :param import_id: Document identifier
    :param config: Connector configuration with retry settings

    :return    Validated NavigatorDocument
    """
    session = requests.Session()

    retry_strategy = Retry(
        total=config.max_retries,
        backoff_factor=config.retry_backoff_seconds,
        allowed_methods=["GET"],
        status_forcelist=[
            HTTPStatus.TOO_MANY_REQUESTS,
            HTTPStatus.INTERNAL_SERVER_ERROR,
            HTTPStatus.BAD_GATEWAY,
            HTTPStatus.SERVICE_UNAVAILABLE,
            HTTPStatus.GATEWAY_TIMEOUT,
        ],
    )

    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=config.connection_pool_size,
        pool_maxsize=config.connection_pool_size,
    )

    session.mount("https://", adapter)
    session.mount("http://", adapter)

    # Construct URL
    url = f"{config.base_url}/families/{import_id}"

    logger.debug(f"Fetching from: {url}")

    try:
        response = session.get(
            url, timeout=config.timeout_seconds, headers={"Accept": "application/json"}
        )

        response.raise_for_status()

        response_json = response.json()
        response_data = response_json.get("data")

        if not response_data:
            raise ValueError(f"No data in response for {import_id}")

        # Validate and return
        family = NavigatorFamily.model_validate(response_data)
        return family

    finally:
        session.close()
