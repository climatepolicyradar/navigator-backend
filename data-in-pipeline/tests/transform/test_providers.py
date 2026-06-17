from unittest.mock import MagicMock, patch

from data_in_models.models import Label

from app.transform.models import NoMatchingTransformations
from app.transform.providers import create_provider_labels


@patch(
    "app.transform.providers.corpus_to_provider_map",
    new={"test.corpus.0.0": "Test corpus name"},
)
@patch("app.transform.providers.NavigatorConnector")
def test_create_provider_labels_successfully_maps_corpus_to_provider(
    mock_connector_cls,
):
    mock_connector_cls.return_value.fetch_all_corpora.return_value.envelopes = [
        MagicMock(
            data=[
                MagicMock(
                    import_id="test.corpus.0.0",
                    attribution_url="https://climate-laws.com",
                    corpus_text="Test corpus text",
                    corpus_image_url="corpora/test.corpus.0.0/logo.png",
                ),
            ]
        ),
    ]
    labels, warnings = create_provider_labels("1", "2")

    assert not warnings

    assert labels == [
        Label(
            attributes={
                "attribution_url": "https://climate-laws.com",
                "corpus_text": "Test corpus text",
                "corpus_image_url": "https://cdn.climatepolicyradar.org/corpora/test.corpus.0.0/logo.png",
            },
            id="agent::Test corpus name",
            type="agent",
            value="Test corpus name",
            labels=[],
            documents=[],
        )
    ]


@patch(
    "app.transform.providers.corpus_to_provider_map",
    new={},
)
@patch("app.transform.providers.NavigatorConnector")
def test_create_provider_labels_returns_warning_if_no_provider_matching_corpus(
    mock_connector_cls,
):
    mock_connector_cls.return_value.fetch_all_corpora.return_value.envelopes = [
        MagicMock(
            data=[
                MagicMock(
                    import_id="test.corpus.0.0",
                    attribution_url="https://climate-laws.com",
                    corpus_text="Test corpus text",
                    corpus_image_url="corpora/test.corpus.0.0/logo.png",
                )
            ]
        )
    ]

    labels, warnings = create_provider_labels("1", "2")

    assert not labels
    assert len(warnings) == 1

    assert isinstance(warnings[0], NoMatchingTransformations)
    error_message = warnings[0].args[0]
    assert error_message == "Missing transformation for corpus: {'test.corpus.0.0'}"
