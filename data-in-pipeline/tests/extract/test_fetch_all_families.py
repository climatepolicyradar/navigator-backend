from unittest.mock import call, patch

import requests
from pydantic import ValidationError

from app.extract.connectors import (
    DuplicateRecordFailure,
    NavigatorConnector,
    NavigatorConnectorConfig,
    NavigatorFamily,
    PageFetchFailure,
    RecordValidationFailure,
)
from app.extract.enums import CheckPointStorageType
from tests.factories import NavigatorCorpusFactory, NavigatorFamilyFactory


def _connector() -> NavigatorConnector:
    return NavigatorConnector(
        NavigatorConnectorConfig(
            source_id="navigator_family",
            checkpoint_storage=CheckPointStorageType.S3,
            checkpoint_key_prefix="navigator/families/",
        )
    )


def _family_payload(import_id: str) -> dict:
    return NavigatorFamilyFactory.build(
        import_id=import_id,
        corpus=NavigatorCorpusFactory.build(import_id="CCLW.corpus.i00000001.n0000"),
    ).model_dump(mode="json")


def test_fetch_all_families_keeps_validation_failures_when_a_page_fetch_fails():
    responses = [
        {"data": [{"import_id": "Invalid.family.0.0"}]},  # page 1 → validation failure
        requests.ConnectionError("connection reset"),  # page 2 → fetch failure
    ]

    connector = _connector()
    with patch.object(connector, "get", side_effect=responses):
        result = connector.fetch_all_families(task_run_id="t", flow_run_id="f")

    assert [type(f) for f in result.failures] == [
        RecordValidationFailure,
        PageFetchFailure,
    ]
    assert result.failures[1] == PageFetchFailure(
        page=2, error="connection reset", task_run_id="t"
    )


def test_fetch_all_families_reports_records_seen_on_more_than_one_page():
    """A family returned on two pages means the pages no longer line up, so others may be missing."""
    responses = [
        {"data": [_family_payload("family.a"), _family_payload("family.b")]},
        {"data": [_family_payload("family.b"), _family_payload("family.c")]},
        {"data": []},
    ]

    connector = _connector()
    with patch.object(connector, "get", side_effect=responses):
        result = connector.fetch_all_families(task_run_id="t", flow_run_id="f")

    assert result.failures == [
        DuplicateRecordFailure(import_ids=["family.b"], page=2),
    ]


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


def _page_request(after_import_id: str):
    return call(
        "families/", params={"page_size": 100, "after_import_id": after_import_id}
    )


def test_fetch_all_families_requests_pages_by_import_id_cursor():
    responses = [
        {"data": [_family_payload("family.a"), _family_payload("family.b")]},
        {"data": [_family_payload("family.c")]},
        {"data": []},
    ]

    connector = _connector()
    with patch.object(connector, "get", side_effect=responses) as get:
        connector.fetch_all_families(task_run_id="t", flow_run_id="f")

    assert get.call_args_list == [
        _page_request(""),
        _page_request("family.b"),
        _page_request("family.c"),
    ]


def test_fetch_all_families_advances_cursor_past_an_invalid_last_family():
    responses = [
        {"data": [_family_payload("family.a"), {"import_id": "z.invalid"}]},
        {"data": []},
    ]

    connector = _connector()
    with patch.object(connector, "get", side_effect=responses) as get:
        result = connector.fetch_all_families(task_run_id="t", flow_run_id="f")

    assert get.call_args_list[1] == _page_request("z.invalid")
    assert [type(f) for f in result.failures] == [RecordValidationFailure]


def _same_page_repeatedly(page: dict) -> list:
    # Capped so a crawl that never stops fails the test instead of hanging it.
    return [page] * 5 + [AssertionError("crawl did not stop")]


def test_fetch_all_families_stops_when_a_page_has_nothing_new():
    """An API that ignores the cursor returns the same page forever."""
    page = {"data": [_family_payload("family.a"), _family_payload("family.b")]}

    connector = _connector()
    with patch.object(connector, "get", side_effect=_same_page_repeatedly(page)) as get:
        result = connector.fetch_all_families(task_run_id="t", flow_run_id="f")

    assert get.call_args_list == [_page_request(""), _page_request("family.b")]
    assert result.failures == [
        DuplicateRecordFailure(import_ids=["family.a", "family.b"], page=2),
    ]


def test_fetch_all_families_stops_when_a_repeated_page_contains_an_invalid_family():
    page = {"data": [_family_payload("family.a"), {"import_id": "z.invalid"}]}

    connector = _connector()
    with patch.object(connector, "get", side_effect=_same_page_repeatedly(page)) as get:
        result = connector.fetch_all_families(task_run_id="t", flow_run_id="f")

    assert get.call_args_list == [_page_request(""), _page_request("z.invalid")]
    assert result.failures[-1] == DuplicateRecordFailure(
        import_ids=["family.a", "z.invalid"], page=2
    )
