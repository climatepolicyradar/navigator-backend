from unittest.mock import patch

from pydantic import ValidationError

from app.extract.connectors import (
    NavigatorConnector,
    NavigatorConnectorConfig,
    NavigatorFamily,
    RecordValidationFailure,
)
from app.extract.enums import CheckPointStorageType


def test_fetch_all_families_collects_validation_failures_and_continues():
    invalid_family = {"import_id": "Invalid.family.0.0", "other": "fields"}
    valid_family = {
        "import_id": "Valid.family.0.0",
        "title": "Test title",
        "summary": "Test summary",
        "documents": [],
        "corpus": {
            "import_id": "Test.corpus.0.0",
            "corpus_type": {"name": "Test"},
            "organisation": {"id": 0, "name": "Test"},
            "corpus_text": "Test",
        },
        "events": [],
        "collections": [],
        "geographies": [],
        "category": "Test",
        "slug": "valid-test-family",
    }

    responses = [
        {"data": [invalid_family]},  # page 1 → ValidationError
        {"data": [valid_family]},  # page 2 → succeeds
        {"data": []},  # page 3 → terminates
    ]

    valid_family_instance = NavigatorFamily.model_construct()
    validation_error = ValidationError.from_exception_data(
        "NavigatorFamily",
        [{"type": "missing", "loc": ("slug",), "input": {}}],
    )

    connector = NavigatorConnector(
        NavigatorConnectorConfig(
            source_id="navigator_family",
            checkpoint_storage=CheckPointStorageType.S3,
            checkpoint_key_prefix="navigator/families/",
        )
    )

    with (
        patch.object(connector, "get", side_effect=responses),
        patch.object(
            NavigatorFamily,
            "model_validate",
            side_effect=[validation_error, valid_family_instance],
        ),
    ):
        result = connector.fetch_all_families(
            task_run_id="task-123",
            flow_run_id="flow-456",
        )

    assert len(result.envelopes) == 1
    assert result.envelopes[0].source_record_id == "task-123-families-endpoint-page-2"

    assert isinstance(result.failures, list)
    assert len(result.failures) == 1

    failure = result.failures[0]
    assert isinstance(failure, RecordValidationFailure)
    assert failure.import_ids[0] == "Invalid.family.0.0"
    assert failure.page == 1
    assert (
        failure.error
        == 'Error validating family: [{"type":"missing","loc":["slug"],"msg":"Field required","input":{},"url":"https://errors.pydantic.dev/2.12/v/missing"}]'
    )
