"""Adaptors for searching CPR data"""

import logging
import time
from abc import ABC
from pathlib import Path
from typing import Optional

from cpr_sdk.exceptions import DocumentNotFoundError, FetchError, QueryError
from cpr_sdk.models.search import Hit, SearchParameters, SearchResponse
from cpr_sdk.vespa import (
    VespaErrorDetails,
    build_vespa_request_body,
    find_vespa_cert_paths,
    parse_vespa_response,
    split_document_id,
)
from requests.exceptions import HTTPError
from vespa.application import Vespa, VespaAsync
from vespa.exceptions import VespaError

LOGGER = logging.getLogger(__name__)


class SearchAdapter(ABC):
    """Base class for all search adapters."""

    def search(self, parameters: SearchParameters) -> SearchResponse:
        """
        Search a dataset

        :param SearchParameters parameters: a search request object
        :return SearchResponse: a list of parent families, each containing relevant
            child documents and passages
        """
        raise NotImplementedError

    def get_by_id(self, document_id: str) -> SearchResponse:
        """
        Get a single document by its id

        :param str document_id: document id
        :return Hit: a single document or passage
        """
        raise NotImplementedError


class VespaAsyncSearchAdapter(SearchAdapter):
    """Search within a Vespa instance."""

    instance_url: str
    client: VespaAsync

    def __init__(
        self,
        instance_url: str,
        cert_directory: Optional[str] = None,
    ):
        """
        Initialize the Vespa search adapter.

        :param instance_url: URL of the Vespa instance to connect to
        :param cert_directory: Optional directory containing cert.pem and key.pem files.
            If None, will attempt to find certs automatically.
        """
        self.instance_url = instance_url
        if cert_directory is None:
            cert_path, key_path = find_vespa_cert_paths()
        else:
            cert_path = (Path(cert_directory) / "cert.pem").__str__()
            key_path = (Path(cert_directory) / "key.pem").__str__()

        self.client = VespaAsync(
            app=Vespa(url=instance_url, cert=cert_path, key=key_path)
        )

    def search(self, parameters: SearchParameters) -> SearchResponse:
        """
        Search a vespa instance

        :param SearchParameters parameters: a search request object
        :return SearchResponse: a list of families, with response metadata
        """
        total_time_start = time.time()
        vespa_request_body = build_vespa_request_body(parameters)
        query_time_start = time.time()
        try:
            vespa_response = self.client.query(body=vespa_request_body)  # type: ignore
        except VespaError as e:
            err_details = VespaErrorDetails(e)
            if err_details.is_invalid_query_parameter:
                LOGGER.error(err_details.message)
                raise QueryError(err_details.summary)
            else:
                raise e
        query_time_end = time.time()

        response = parse_vespa_response(vespa_response=vespa_response)  # type: ignore

        response.query_time_ms = int((query_time_end - query_time_start) * 1000)
        response.total_time_ms = int((time.time() - total_time_start) * 1000)

        return response

    def get_by_id(self, document_id: str) -> Hit:  # type: ignore
        """
        Get a single document by its id

        :param str document_id: IDs should look something like
            "id:doc_search:family_document::CCLW.family.11171.0"
            or
            "id:doc_search:document_passage::UNFCCC.party.1060.0.3743"
        :return Hit: a single document or passage
        """
        namespace, schema, data_id = split_document_id(document_id)
        try:
            vespa_response = self.client.get_data(  # type: ignore
                namespace=namespace, schema=schema, data_id=data_id
            )
        except HTTPError as e:
            if e.response is not None:
                status_code = e.response.status_code
            else:
                status_code = "Unknown"
            if status_code == 404:
                raise DocumentNotFoundError(document_id) from e
            else:
                raise FetchError(
                    f"Received status code {status_code} when fetching "
                    f"document {document_id}",
                    status_code=status_code,
                ) from e

        return Hit.from_vespa_response(vespa_response.json)  # type: ignore
