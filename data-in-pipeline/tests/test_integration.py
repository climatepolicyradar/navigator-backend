from unittest.mock import patch

from app.models import Document, DocumentLabelRelationship, Label
from app.navigator_document_etl_pipeline import (
    process_updates as process_document_updates,
)
from app.navigator_family_etl_pipeline import process_updates as process_family_updates
from app.transform.navigator_family import TransformerLabel


@patch("app.navigator_document_etl_pipeline.upload_to_s3")
def test_process_document_updates_flow(mock_upload):
    mock_upload.return_value = None
    assert process_document_updates(["CCLW.legislative.10695.6311"]) == [
        Document(id="CCLW.legislative.10695.6311", title="Climate Change Act 2022")
    ]


@patch("app.navigator_document_etl_pipeline.upload_to_s3")
def test_process_document_updates_flow_with_invalid_id(mock_upload):
    mock_upload.return_value = None
    assert process_document_updates(["CCLW.INVALID_ID"]) == []


@patch("app.navigator_family_etl_pipeline.upload_to_s3")
def test_process_family_updates_flow(mock_upload):
    mock_upload.return_value = None
    assert process_family_updates(["UNFCCC.family.i00000314.n0000"]) == [
        [
            Document(
                id="UNFCCC.document.i00000315.n0000",
                title="Belgium UNCBD National Targets",
                labels=[
                    DocumentLabelRelationship(
                        type="family",
                        label=Label(
                            id="UNFCCC.family.i00000314.n0000",
                            title="Belgium UNCBD National Targets",
                            type="family",
                        ),
                    ),
                    DocumentLabelRelationship(
                        type="transformer",
                        label=TransformerLabel(
                            id="transform_navigator_family_with_single_matching_document",
                            title="transform_navigator_family_with_single_matching_document",
                            type="transformer",
                        ),
                    ),
                ],
            )
        ]
    ]


@patch("app.navigator_family_etl_pipeline.upload_to_s3")
def test_process_family_updates_flow_with_invalid_id(mock_upload):
    mock_upload.return_value = None
    assert process_family_updates(["UNFCCC.INVALID_ID"]) == []
