import datetime
from http import HTTPStatus

import requests
from pydantic import BaseModel
from requests.adapters import HTTPAdapter, Retry
from returns.result import Failure, Result, Success

from app.bootstrap_telemetry import get_logger, pipeline_metrics
from app.extract.connector_config import NavigatorConnectorConfig
from app.models import ExtractedEnvelope, ExtractedMetadata
from app.pipeline_metrics import ErrorType, Operation
from app.util import generate_envelope_uuid


class NavigatorEvent(BaseModel):
    import_id: str
    event_type: str
    date: datetime.datetime


class NavigatorDocument(BaseModel):
    import_id: str
    title: str
    events: list[NavigatorEvent]
    valid_metadata: dict[str, list[str]] = {}


class NavigatorCorpusType(BaseModel):
    name: str


class NavigatorCorpus(BaseModel):
    import_id: str


class NavigatorFamily(BaseModel):
    import_id: str
    title: str
    documents: list[NavigatorDocument]
    corpus: NavigatorCorpus
    events: list[NavigatorEvent]


class PageFetchFailure(BaseModel):
    page: int
    error: str
    task_run_id: str | None


class FamilyFetchResult(BaseModel):
    envelopes: list[ExtractedEnvelope]
    failure: PageFetchFailure | None = None


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
        logger = get_logger()

        url = f"{self.config.base_url}/{path.lstrip('/')}"
        logger.debug(f"Fetching from {url}")

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

    def fetch_document(
        self, import_id: str, task_run_id: str, flow_run_id: str
    ) -> Result[ExtractedEnvelope, Exception]:
        """Fetch a single document from Navigator API."""
        logger = get_logger()

        try:
            response_json = self.get(f"families/documents/{import_id}")
            document_data = response_json.get("data")
            logger.info(f"Successfully fetched document data for '{import_id}'")

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
                    task_run_id=task_run_id,
                    flow_run_id=flow_run_id,
                )
            )
        except requests.RequestException as e:
            logger.error(f"Request failed fetching document {import_id}")
            pipeline_metrics.record_error(Operation.EXTRACT, ErrorType.NETWORK)
            return Failure(e)
        except Exception as e:
            logger.error(f"Unexpected error fetching document {import_id}")
            pipeline_metrics.record_error(Operation.EXTRACT, ErrorType.UNKNOWN)
            return Failure(e)

    def fetch_family(
        self, import_id: str, task_run_id: str, flow_run_id: str
    ) -> Result[ExtractedEnvelope, Exception]:
        """Fetch a single family from Navigator API."""
        logger = get_logger()
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
                    task_run_id=task_run_id,
                    flow_run_id=flow_run_id,
                )
            )
        except requests.RequestException as e:
            logger.error(f"Request failed fetching family {import_id}")
            return Failure(e)
        except Exception as e:
            logger.error(f"Unexpected error fetching family {import_id}")
            return Failure(e)

    def fetch_all_families(
        self, task_run_id: str, flow_run_id: str
    ) -> FamilyFetchResult:
        """Fetch all family records from the Navigator API with pagination.

        This method iterates through all available pages of the Navigator API's
        `/families` endpoint. Each page of results is fetched and transformed into
        an :class:`ExtractedEnvelope` object.

        Errors, such as temporary network issues, are
        recorded as :class:`PageFailure` objects and returned alongside the
        successfully fetched results.
        :param str task_run_id: The unique Prefect task run identifier associated
            with this extraction.
        :param str flow_run_id: The unique Prefect flow run identifier for the
            current pipeline run.
        :return FetchResult:
            - **Success((envelopes, failures))** if all (or some) pages are fetched successfully.
            - **Failure(exception)** if a fatal error prevents completion of the operation.

        """
        logger = get_logger()

        page = 1
        successful_envelopes: list[ExtractedEnvelope] = []
        while True:
            try:
                logger.info(f"Fetching families page {page}")
                response_json = self.get(f"families/?page={page}")
                families_data = response_json.get("data", [])

                # Break the loop if no more families are returned from the endpoint
                if not families_data:
                    logger.info(
                        f"No more families found at page {page}. Total pages fetched: {len(successful_envelopes)}"
                    )
                    break

                validated_families = [
                    NavigatorFamily.model_validate(family) for family in families_data
                ]

                envelope = ExtractedEnvelope(
                    data=validated_families,
                    id=generate_envelope_uuid(),
                    source_name="navigator_family",
                    source_record_id=f"{task_run_id}-families-endpoint-page-{page}",
                    raw_payload=families_data,
                    content_type="application/json",
                    connector_version="1.0.0",
                    extracted_at=datetime.datetime.now(datetime.timezone.utc),
                    task_run_id=task_run_id,
                    flow_run_id=flow_run_id,
                    metadata=ExtractedMetadata(
                        endpoint=f"{self.config.base_url}/families/?page={page}",
                        http_status=HTTPStatus.OK,
                    ),
                )

                successful_envelopes.append(envelope)
                page += 1

            except requests.RequestException as e:
                logger.error(
                    f"Request failed while fetching all families at page {page}"
                )
                return FamilyFetchResult(
                    envelopes=successful_envelopes,
                    failure=PageFetchFailure(
                        page=page, error=str(e), task_run_id=task_run_id
                    ),
                )

            except Exception as e:
                logger.error(
                    f"Unexpected error {e} while fetching page {page} of families"
                )
                return FamilyFetchResult(
                    envelopes=successful_envelopes,
                    failure=PageFetchFailure(
                        page=page, error=str(e), task_run_id=task_run_id
                    ),
                )

        logger.info(
            f"Fetch families completed: {len(successful_envelopes)} pages succeeded"
        )
        return FamilyFetchResult(envelopes=successful_envelopes)
