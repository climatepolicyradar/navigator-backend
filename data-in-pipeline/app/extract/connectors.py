import datetime
import logging
from http import HTTPStatus

import requests
from pydantic import BaseModel
from requests.adapters import HTTPAdapter, Retry
from returns.result import Failure, Result, Success

from app.extract.connector_config import NavigatorConnectorConfig
from app.models import ExtractedEnvelope, ExtractedMetadata
from app.util import generate_envelope_uuid

_LOGGER = logging.getLogger(__name__)


class NavigatorDocument(BaseModel):
    import_id: str
    title: str


class NavigatorCorpus(BaseModel):
    import_id: str


class NavigatorFamily(BaseModel):
    import_id: str
    title: str
    documents: list[NavigatorDocument]
    corpus: NavigatorCorpus


class HTTPConnector:
    """Base class for HTTP-based connectors."""

    def __init__(self, config):
        self.config = config
        self.session = self._init_session()

    def _init_session(self) -> requests.Session:
        """Initialize a requests session with retry and pooling configuration."""
        session = requests.Session()

        retry_strategy = Retry(
            total=self.config.max_retries,
            backoff_factor=self.config.retry_backoff_seconds,
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
            pool_connections=self.config.connection_pool_size,
            pool_maxsize=self.config.connection_pool_size,
        )

        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def get(self, path: str, **kwargs):
        """Perform a GET request and handle retries and errors."""
        url = f"{self.config.base_url}/{path.lstrip('/')}"
        _LOGGER.debug(f"Fetching from {url}")

        response = self.session.get(url, timeout=self.config.timeout_seconds, **kwargs)
        response.raise_for_status()
        return response.json()

    def close(self):
        """Close the session cleanly."""
        self.session.close()


class NavigatorConnector(HTTPConnector):
    """Connector for fetching documents and families from the Navigator API."""

    def __init__(self, config: NavigatorConnectorConfig):
        super().__init__(config)

    def fetch_document(self, import_id: str) -> Result[ExtractedEnvelope, Exception]:
        """Fetch a single document from Navigator API."""
        try:
            response_json = self.get(f"families/documents/{import_id}")
            document_data = response_json.get("data")
            _LOGGER.info(f"Successfully fetched document data for '{import_id}'")

            if not document_data:
                raise ValueError(f"No document data in response for {import_id}")

            document = NavigatorDocument.model_validate(document_data)

            return Success(
                ExtractedEnvelope(
                    data=document,
                    id=generate_envelope_uuid(),
                    source_name="navigator_document",
                    source_record_id=import_id,
                    raw_payload=document.model_dump_json(),
                    content_type="application/json",
                    connector_version="1.0.0",
                    extracted_at=datetime.datetime.now(datetime.timezone.utc),
                    metadata=ExtractedMetadata(
                        endpoint=f"{self.config.base_url}/families/documents/{import_id}",
                        http_status=HTTPStatus.OK,
                    ),
                )
            )
        except requests.RequestException as e:
            _LOGGER.exception(f"Request failed fetching document {import_id}")
            return Failure(e)
        except Exception as e:
            _LOGGER.exception(f"Unexpected error fetching document {import_id}")
            return Failure(e)

    def fetch_family(self, import_id: str) -> Result[ExtractedEnvelope, Exception]:
        """Fetch a single family from Navigator API."""
        try:
            response_json = self.get(f"families/{import_id}")
            family_data = response_json.get("data")

            if not family_data:
                raise ValueError(f"No family data in response for {import_id}")

            family = NavigatorFamily.model_validate(family_data)

            return Success(
                ExtractedEnvelope(
                    data=family,
                    id=generate_envelope_uuid(),
                    source_name="navigator_family",
                    source_record_id=import_id,
                    raw_payload=family.model_dump_json(),
                    content_type="application/json",
                    connector_version="1.0.0",
                    extracted_at=datetime.datetime.now(datetime.timezone.utc),
                    metadata=ExtractedMetadata(
                        endpoint=f"{self.config.base_url}/families/documents/{import_id}",
                        http_status=HTTPStatus.OK,
                    ),
                )
            )
        except requests.RequestException as e:
            _LOGGER.exception(f"Request failed fetching family {import_id}")
            return Failure(e)
        except Exception as e:
            _LOGGER.exception(f"Unexpected error fetching family {import_id}")
            return Failure(e)
