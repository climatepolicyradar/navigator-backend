from http import HTTPStatus
from unittest.mock import MagicMock, patch

import pytest

from app.extract.connector_config import NavigatorConnectorConfig
from app.extract.connectors import (
    FamilyFetchResult,
    PageFetchFailure,
)
from app.extract.enums import CheckPointStorageType
from app.models import ExtractedEnvelope, ExtractedMetadata
from app.navigator_family_etl_pipeline import (
    extract,
)
from tests.factories import (
    NavigatorCorpusFactory,
    NavigatorCorpusTypeFactory,
    NavigatorFamilyFactory,
    NavigatorOrganisationFactory,
    PageFetchFailureFactory,
)


@pytest.fixture
def base_config():
    return NavigatorConnectorConfig(
        base_url="test-url",
        source_id="navigator_family",
        checkpoint_storage=CheckPointStorageType.S3,
        checkpoint_key_prefix="navigator/families/",
    )


def test_extract_families_handles_valid_ids_success():
    """Test extract task successfully processes valid family IDs."""
    valid_ids = ["FAM-001", "FAM-002"]

    with patch(
        "app.navigator_family_etl_pipeline.NavigatorConnector"
    ) as mock_connector_class:
        mock_connector_instance = MagicMock()
        mock_connector_class.return_value = mock_connector_instance
        mock_connector_instance.close.return_value = None

        families = [
            NavigatorFamilyFactory.build(
                import_id="FAM-001",
                title="Family 1",
                summary="Summary 1",
                documents=[],
                corpus=NavigatorCorpusFactory.build(
                    import_id="COR-001",
                    corpus_type=NavigatorCorpusTypeFactory.build(name="corpus_type"),
                    organisation=NavigatorOrganisationFactory.build(
                        id=1, name="UNFCCC"
                    ),
                ),
                events=[],
                collections=[],
                geographies=[],
                category="REPORTS",
            ),
            NavigatorFamilyFactory.build(
                import_id="FAM-002",
                title="Family 2",
                summary="Summary 2",
                documents=[],
                corpus=NavigatorCorpusFactory.build(
                    import_id="COR-002",
                    corpus_type=NavigatorCorpusTypeFactory.build(name="corpus_type"),
                    organisation=NavigatorOrganisationFactory.build(
                        id=1, name="UNFCCC"
                    ),
                ),
                events=[],
                collections=[],
                geographies=[],
                category="REPORTS",
            ),
        ]
        mock_envelope = ExtractedEnvelope(
            data=families,
            raw_payload=families,
            id="test-uuid",
            source_name="navigator_family",
            source_record_id="task-001-families-by-ids",
            content_type="application/json",
            connector_version="1.0.0",
            metadata=ExtractedMetadata(
                endpoint="test-url/families/",
                http_status=HTTPStatus.OK,
            ),
            task_run_id="task-001",
            flow_run_id="flow-001",
        )

        mock_connector_instance.fetch_families.return_value = FamilyFetchResult(
            envelopes=[mock_envelope], failure=None
        )

        result = extract(valid_ids)

    assert result.failure is None
    assert len(result.envelopes) == 1
    assert result.envelopes[0].source_name == "navigator_family"
    expected_family_count = 2
    assert len(result.envelopes[0].data) == expected_family_count
    mock_connector_instance.fetch_families.assert_called_once()
    call_args = mock_connector_instance.fetch_families.call_args
    assert call_args[0][0] == valid_ids


def test_extract_families_propagates_connector_failure():
    """Test extract propagates failure when connector returns failure."""
    invalid_ids = ["INVALID_ID"]

    with patch(
        "app.navigator_family_etl_pipeline.NavigatorConnector"
    ) as mock_connector_class:
        mock_connector_instance = MagicMock()
        mock_connector_class.return_value = mock_connector_instance
        mock_connector_instance.close.return_value = None

        expected_error = "Failed to fetch families: API timeout"
        mock_connector_instance.fetch_families.return_value = FamilyFetchResult(
            envelopes=[],
            failure=PageFetchFailureFactory.build(
                page=0, error=expected_error, task_run_id="task-001"
            ),
        )

        result = extract(invalid_ids)

    assert result.failure is not None
    assert isinstance(result.failure, PageFetchFailure)
    assert "API timeout" in result.failure.error
    assert len(result.envelopes) == 0
    mock_connector_instance.fetch_families.assert_called_once()


def test_extract_families_handles_http_error():
    """Test extract propagates HTTPError failures from connector."""
    invalid_ids = ["NOT_FOUND_ID"]

    with patch(
        "app.navigator_family_etl_pipeline.NavigatorConnector"
    ) as mock_connector_class:
        mock_connector_instance = MagicMock()
        mock_connector_class.return_value = mock_connector_instance
        mock_connector_instance.close.return_value = None

        expected_error = "404 Client Error: Not Found"
        mock_connector_instance.fetch_families.return_value = FamilyFetchResult(
            envelopes=[],
            failure=PageFetchFailureFactory.build(
                page=0, error=expected_error, task_run_id="task-001"
            ),
        )

        result = extract(invalid_ids)

    assert result.failure is not None
    assert isinstance(result.failure, PageFetchFailure)
    assert "404" in result.failure.error
    assert len(result.envelopes) == 0
